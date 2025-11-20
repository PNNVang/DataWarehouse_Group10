import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class ETLMain {

    private final String pathFile;
    private final int sourceId;
    private final String dateStr;

    public ETLMain(String pathFile, int sourceId, String dateStr) {
        this.pathFile = pathFile;
        this.sourceId = sourceId;
        this.dateStr = (dateStr == null || dateStr.isBlank())
                ? LocalDate.now().toString()
                : dateStr;
    }

    public void run() {
        long logId = 0;

        try {
            // 1. Load file control input: control.xml, scource_id, date (default = today)
            DatabaseConnector db = loadControlFile();

            // 2. Kết nối với database control
            Connection connControl = connectControlDB(db);

            // 3. Kiểm tra tiến trình P3?
            checkProcessP3(connControl);

            // 4. Đọc cấu hình ETL từ table config_database. Lấy ra: db_staging, transform_table, db_warehouse, db_host, db_port, db_username, db_password
            Map<String, String> cfg = loadEtlConfig(connControl);

            DbConfig rootCfg = buildRootConfig(cfg);
            DbConfig warehouseCfg = buildWarehouseConfig(cfg);

            // 5. Kiểm tra database warehouse có tồn tại chưa?
            createWarehouse(rootCfg, warehouseCfg);

            // 6. Kết nối database staging và warehouse
            try (
                    Connection connStaging = connectStaging(cfg);
                    Connection connWarehouse = connectWarehouse(cfg)
            ) {

                // 7. Ghi log status RUNNING vào process_log trước khi load dữ liệu.
                logId = insertRunningLog(connControl);

                // 8. Load dữ liệu từ table stg_lottery_data_transform qua table warehouse
                int rows = loadToWarehouse(
                        connStaging, connWarehouse,
                        cfg.get("staging_schema"),
                        cfg.get("transform_table")
                );

                // 9. Ghi log status = SUCCESS, lưu message kết quả số dòng được thêm  và hiển thị ra console
                updateProcessLog(connControl, logId, "SUCCESS",
                        "ETL thành công. Tổng số dòng: " + rows);

                System.out.println("[SUCCESS] ETL hoàn tất, thêm " + rows + " dòng");

            } catch (Exception ex) {
                updateProcessLog(connControl, logId, "FAILED",
                        "Lỗi khi load dữ liệu: " + ex.getMessage());
                throw ex;
            }

        } catch (Exception e) {
            System.err.println("Lỗi ETL tổng: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public DatabaseConnector loadControlFile() throws Exception {
        DatabaseConnector db = new DatabaseConnector(pathFile);
        if (db == null) throw new Exception("Không thể parse file control.xml");
        System.out.println("Đã load file control.xml thành công");
        return db;
    }

    public void checkProcessP3(Connection conn) throws Exception {
        if (!isProcessSuccess(conn)) {
            String msg = "Tiến trình P3 chưa SUCCESS. Dừng ETL!";
            insertProcessLog(conn, sourceId, "P" + sourceId, "FAILED", msg);
            throw new Exception(msg);
        }
        System.out.println("P3 đã SUCCESS → tiếp tục ETL");
    }

    public boolean isProcessSuccess(Connection conn) throws SQLException {
        String sql = """
            SELECT 1 
            FROM process_log 
            WHERE process_code='P3' AND status='SUCCESS'
            ORDER BY ended_at DESC LIMIT 1
        """;

        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            return rs.next();
        }
    }

    public Connection connectControlDB(DatabaseConnector db) throws Exception {
        Connection conn = db.getConnection();
        if (conn == null) throw new SQLException("Không thể kết nối database control");
        System.out.println("Kết nối database control thành công");
        return conn;
    }

    public Map<String, String> loadEtlConfig(Connection conn) throws Exception {
        Map<String, String> cfg = Util.getConfigDatabase(conn);
        System.out.println("Đã đọc cấu hình ETL");
        return cfg;
    }

    public DbConfig buildRootConfig(Map<String, String> cfg) {
        DbConfig root = new DbConfig();
        root.setHost(cfg.get("db_host"));
        root.setPort(cfg.get("db_port"));
        root.setDatabase("");
        root.setUsername(cfg.get("db_username"));
        root.setPassword(cfg.get("db_password"));
        return root;
    }

    public DbConfig buildWarehouseConfig(Map<String, String> cfg) {
        DbConfig w = new DbConfig();
        w.setHost(cfg.get("db_host"));
        w.setPort(cfg.get("db_port"));
        w.setDatabase(cfg.get("warehouse_schema"));
        w.setUsername(cfg.get("db_username"));
        w.setPassword(cfg.get("db_password"));
        return w;
    }

    public void createWarehouse(DbConfig rootCfg, DbConfig warehouseCfg) throws Exception {
        // 5.1.Gọi procedure sp_create_warehouse_table để tạo bảng
        Util.createWarehouse(rootCfg, warehouseCfg);
        System.out.println("Đã kiểm tra/tạo warehouse qua stored procedure.");
    }

    public Connection connectStaging(Map<String, String> cfg) throws Exception {
        return DriverManager.getConnection(
                buildJdbcUrl(cfg.get("staging_schema"), cfg),
                cfg.get("db_username"), cfg.get("db_password")
        );
    }

    public Connection connectWarehouse(Map<String, String> cfg) throws Exception {
        return DriverManager.getConnection(
                buildJdbcUrl(cfg.get("warehouse_schema"), cfg),
                cfg.get("db_username"), cfg.get("db_password")
        );
    }

    public String buildJdbcUrl(String dbName, Map<String, String> cfg) {
        return String.format(
                "jdbc:mysql://%s:%s/%s?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
                cfg.get("db_host"),
                cfg.get("db_port"),
                dbName
        );
    }

    public long insertRunningLog(Connection conn) throws Exception {
        return insertProcessLog(conn, sourceId, "P" + sourceId, "RUNNING",
                "Bắt đầu ETL warehouse");
    }

    public long insertProcessLog(Connection conn, int sourceId, String processCode,
                                 String status, String message) throws SQLException {

        String sql = """
            INSERT INTO process_log 
            (source_id, status, started_at, process_code, message)
            VALUES (?, ?, NOW(), ?, ?)
        """;

        try (PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            ps.setInt(1, sourceId);
            ps.setString(2, status);
            ps.setString(3, processCode);
            ps.setString(4, message);
            ps.executeUpdate();

            try (ResultSet rs = ps.getGeneratedKeys()) {
                return rs.next() ? rs.getLong(1) : 0;
            }
        }
    }

    public void updateProcessLog(Connection conn, long id, String status, String message) throws SQLException {
        String sql = "UPDATE process_log SET status=?, ended_at=NOW(), message=? WHERE process_id=?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, status);
            ps.setString(2, message);
            ps.setLong(3, id);
            ps.executeUpdate();
        }
    }

    public int loadToWarehouse(Connection connStaging, Connection connWarehouse,
                               String dbStaging, String tableStaging) throws SQLException {

        String sqlStage = """
            SELECT number_value, full_date, range_group, is_weekend, is_even
            FROM %s.%s
        """.formatted(dbStaging, tableStaging);

        List<Map<String, Object>> stageData = new ArrayList<>();

        try (Statement st = connStaging.createStatement();
             ResultSet rs = st.executeQuery(sqlStage)) {

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();

                LocalDate fullDate = rs.getDate("full_date").toLocalDate();
                int dateKey = Integer.parseInt(fullDate.toString().replace("-", ""));

                row.put("number_value", normalizeNumberValue(rs.getString("number_value")));
                row.put("full_date", fullDate);
                row.put("is_weekend", rs.getInt("is_weekend"));
                row.put("is_even", rs.getInt("is_even"));
                row.put("date_key", dateKey);

                stageData.add(row);
            }
        }

        int before = countRows(connWarehouse, "fact_prize");

        loadDimDate(stageData, connWarehouse);
        Map<String, Integer> numberKeyMap = loadDimNumber(stageData, connWarehouse);
        loadFactPrize(stageData, connWarehouse, numberKeyMap);

        int after = countRows(connWarehouse, "fact_prize");

        return after - before;
    }

    public void loadDimDate(List<Map<String, Object>> stageData, Connection conn) throws SQLException {
        Set<Integer> existing = new HashSet<>();

        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT date_key FROM dim_date")) {
            while (rs.next()) existing.add(rs.getInt(1));
        }

        String sql = """
        INSERT IGNORE INTO dim_date (
            date_key, full_date, day_of_month, month_of_year,
            year_value, year_month_value, day_name, is_weekend
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """;

        try (PreparedStatement ps = conn.prepareStatement(sql)) {

            for (var row : stageData) {
                int dateKey = (int) row.get("date_key");
                if (existing.contains(dateKey)) continue;

                LocalDate d = (LocalDate) row.get("full_date");

                ps.setInt(1, dateKey);
                ps.setDate(2, Date.valueOf(d));
                ps.setInt(3, d.getDayOfMonth());
                ps.setInt(4, d.getMonthValue());
                ps.setInt(5, d.getYear());
                ps.setString(6, "%04d-%02d".formatted(d.getYear(), d.getMonthValue()));
                ps.setString(7, toVietnameseDayName(d.getDayOfWeek()));
                ps.setInt(8, (int) row.get("is_weekend"));

                ps.addBatch();
            }

            ps.executeBatch();
        }
    }

    private static String toVietnameseDayName(DayOfWeek dow) {
        return switch (dow) {
            case MONDAY -> "Thứ 2";
            case TUESDAY -> "Thứ 3";
            case WEDNESDAY -> "Thứ 4";
            case THURSDAY -> "Thứ 5";
            case FRIDAY -> "Thứ 6";
            case SATURDAY -> "Thứ 7";
            case SUNDAY -> "Chủ nhật";
        };
    }

    public Map<String, Integer> loadDimNumber(List<Map<String, Object>> stageData,
                                              Connection connWarehouse) throws SQLException {

        Set<String> existing = new HashSet<>();

        try (Statement st = connWarehouse.createStatement();
             ResultSet rs = st.executeQuery("SELECT number_value FROM dim_number")) {
            while (rs.next()) existing.add(rs.getString(1));
        }

        String sql = """
        INSERT INTO dim_number (
            number_value, is_even, last_digit
        ) VALUES (?, ?, ?)
        """;

        Set<String> normalizedValuesToInsert = new HashSet<>();

        try (PreparedStatement ps = connWarehouse.prepareStatement(sql)) {
            for (Map<String, Object> row : stageData) {
                String val = (String) row.get("number_value");
                if (val == null) continue;
                if (existing.contains(val) || normalizedValuesToInsert.contains(val)) continue;

                int lastDigit = 0;
                try { lastDigit = Integer.parseInt(val) % 10; } catch (Exception ignore) {}

                ps.setString(1, val);
                ps.setInt(2, (int) row.get("is_even"));
                ps.setInt(3, lastDigit);
                ps.addBatch();

                normalizedValuesToInsert.add(val);
            }
            ps.executeBatch();
        }

        // Load map number_value -> number_key
        Map<String, Integer> map = new HashMap<>();

        try (Statement st = connWarehouse.createStatement();
             ResultSet rs = st.executeQuery("SELECT number_key, number_value FROM dim_number")) {

            while (rs.next())
                map.put(rs.getString("number_value"), rs.getInt("number_key"));
        }
        return map;
    }


    public static void loadFactPrize(List<Map<String, Object>> stageData,
                                     Connection connWarehouse,
                                     Map<String, Integer> numMap) throws SQLException {

        // Lấy danh sách record đã tồn tại để tránh insert trùng
        Set<String> existingFacts = new HashSet<>();

        try (Statement st = connWarehouse.createStatement();
             ResultSet rs = st.executeQuery("SELECT date_key, number_key FROM fact_prize")) {

            while (rs.next())
                existingFacts.add(rs.getInt("date_key") + "_" + rs.getInt("number_key"));
        }

        // Sort theo ngày tăng dần để xử lý đúng thứ tự thời gian
        // ví dụ: ngày 05/01 xuất hiện trước ngày 01/01 → sai logic
        stageData.sort(Comparator.comparing(r -> (LocalDate) r.get("full_date")));

        Map<Integer, LocalDate> lastSeen = new HashMap<>();
        Map<Integer, Integer> totalDrawsPerDate = new HashMap<>();

        for (Map<String, Object> row : stageData) {
            int dateKey = (int) row.get("date_key");
            totalDrawsPerDate.put(dateKey, totalDrawsPerDate.getOrDefault(dateKey, 0) + 1);
        }

        String insertSql = """
            INSERT INTO fact_prize 
            (date_key, number_key, occurrence_count, total_draws, probability_value, days_since_last)
            VALUES (?, ?, ?, ?, ?, ?)
        """;

        try (PreparedStatement ps = connWarehouse.prepareStatement(insertSql)) {
            for (Map<String, Object> row : stageData) {

                int dateKey = (int) row.get("date_key");
                LocalDate currentDate = (LocalDate) row.get("full_date");

                String numberValue = (String) row.get("number_value");
                Integer numberKey = numMap.get(numberValue);
                if (numberKey == null) continue;

                String key = dateKey + "_" + numberKey;
                if (existingFacts.contains(key)) continue;

                int totalDraws = totalDrawsPerDate.get(dateKey);
                BigDecimal probability = BigDecimal.valueOf(1.0 / totalDraws);

                // Tính days_since_last:
                // Nếu số chưa từng xuất hiện → NULL
                // Nếu đã xuất hiện → số ngày chênh lệch
                LocalDate lastDate = lastSeen.get(numberKey);
                Integer daysSinceLast = (lastDate == null)
                        ? null
                        : (int) ChronoUnit.DAYS.between(lastDate, currentDate);

                ps.setInt(1, dateKey);
                ps.setInt(2, numberKey);
                ps.setInt(3, 1);
                ps.setInt(4, totalDraws);
                ps.setBigDecimal(5, probability);

                if (daysSinceLast == null)
                    ps.setNull(6, Types.INTEGER);
                else
                    ps.setInt(6, daysSinceLast);

                ps.addBatch();
                // Cập nhật lastSeen
                lastSeen.put(numberKey, currentDate);
            }
            ps.executeBatch();
        }

        //  // Update dim_number để lưu last appeared của từng số
        String updateDim = "UPDATE dim_number SET last_appeared_date=? WHERE number_key=?";
        try (PreparedStatement ps2 = connWarehouse.prepareStatement(updateDim)) {
            for (Map.Entry<Integer, LocalDate> e : lastSeen.entrySet()) {
                ps2.setDate(1, Date.valueOf(e.getValue()));
                ps2.setInt(2, e.getKey());
                ps2.addBatch();
            }
            ps2.executeBatch();
        }
    }

    public static String normalizeNumberValue(String raw) {
        if (raw == null) return null;
        raw = raw.trim();
        try {
            int v = Integer.parseInt(raw);
            return String.valueOf(v);
        } catch (NumberFormatException ex) {
            return raw;
        }
    }

    public int countRows(Connection conn, String table) throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + table)) {
            return rs.next() ? rs.getInt(1) : 0;
        }
    }


    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("java -jar load_warehouse.jar <control.xml> <source_id>");
            System.exit(1);
        }
        ETLMain wc = new ETLMain(args[0], Integer.parseInt(args[1]), null);
        wc.run();
    }
}
