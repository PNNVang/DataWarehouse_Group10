import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class ETLMain {

    public static void main(String[] args) {
        try {
            if (args.length < 2) {
                System.err.println("java -jar load_warehouse.jar <control.xml> <source_id>");
                System.exit(1);
            }

            final String xmlPath = args[0];
            final int sourceId = Integer.parseInt(args[1]);

            // 1. Load control.xml ---
            DbConfig cfg = XMLConfigReader.readConfig(xmlPath);
            final String baseUrl = "jdbc:mysql://" + cfg.getHost() + ":" + cfg.getPort()
                    + "/%s?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";

            // 2. Kết nối với Database: Control
            try (Connection connControl = DriverManager.getConnection(
                    String.format(baseUrl, "control"), cfg.getUsername(), cfg.getPassword())) {

                System.out.println("Kết nối database control thành công");

                // 3. Lấy thông tin config_source và staging
                SourceInfo srcInfo = getSourceInfo(connControl, sourceId);
                String tableStaging = getStagingTable(connControl, sourceId, srcInfo.destinationStaging);
                final String dbStaging = "staging";
                final String dbWarehouse = "warehouse";

                // 4. Kiểm tra database warehouse có tồn tại không?
                checkDbWarehouse(cfg, dbWarehouse);

                runETLProcess(cfg, baseUrl, connControl, dbStaging, dbWarehouse, tableStaging, sourceId, srcInfo);
            }

        } catch (Exception e) {
            System.err.println("Lỗi" + e.getMessage());
            e.printStackTrace();
        }
    }

    private static SourceInfo getSourceInfo(Connection connControl, int sourceId) throws SQLException {
        String sql = """
            SELECT source_name, scaping_script_path, destination_staging, aggregate_table
            FROM config_source WHERE source_id = ?
        """;
        try (PreparedStatement ps = connControl.prepareStatement(sql)) {
            ps.setInt(1, sourceId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) throw new RuntimeException("Không tìm thấy source_id=" + sourceId);
                return new SourceInfo(
                        rs.getString("source_name"),
                        rs.getString("scaping_script_path"),
                        rs.getString("destination_staging"),
                        rs.getString("aggregate_table")
                );
            }
        }
    }

    private static String getStagingTable(Connection connControl, int sourceId, String defaultStaging) throws SQLException {
        if (sourceId != 4) return defaultStaging;

        try (Statement st = connControl.createStatement();
             ResultSet rs = st.executeQuery("SELECT destination_staging FROM config_source WHERE source_id = 3")) {
            if (!rs.next())
                throw new RuntimeException("Thiếu config source_id=3 cho bảng staging (transform).");
            return rs.getString("destination_staging");
        }
    }

    private static void checkDbWarehouse(DbConfig cfg, String dbWarehouse) throws SQLException {
        DbConfig rootCfg = new DbConfig();
        rootCfg.setHost(cfg.getHost());
        rootCfg.setPort(cfg.getPort());
        rootCfg.setUsername(cfg.getUsername());
        rootCfg.setPassword(cfg.getPassword());

        DbConfig warehouseCfg = new DbConfig();
        warehouseCfg.setHost(cfg.getHost());
        warehouseCfg.setPort(cfg.getPort());
        warehouseCfg.setDatabase(dbWarehouse);
        warehouseCfg.setUsername(cfg.getUsername());
        warehouseCfg.setPassword(cfg.getPassword());
        Util.createWarehouse(rootCfg, warehouseCfg);
    }

    private static void runETLProcess(DbConfig cfg, String baseUrl, Connection connControl,
                                      String dbStaging, String dbWarehouse, String tableStaging,
                                      int sourceId, SourceInfo srcInfo) throws SQLException {

        System.out.println("[INFO] ETL: " + srcInfo.sourceName);
        System.out.println("[INFO] Script: " + srcInfo.scriptPath);
        System.out.println("[INFO] Staging: " + dbStaging + "." + tableStaging);
        System.out.println("[INFO] Warehouse: " + dbWarehouse + " (dim_date, dim_number, fact_prize)");

        // 5. Kết nối database: staging
        try (Connection connStaging = DriverManager.getConnection(
                String.format(baseUrl, dbStaging), cfg.getUsername(), cfg.getPassword());
             Connection connWarehouse = DriverManager.getConnection(
                     String.format(baseUrl, dbWarehouse), cfg.getUsername(), cfg.getPassword())) {

            System.out.println("Đã kết nối: " + dbStaging + ", " + dbWarehouse);

            long logId = insertProcessLog(connControl, sourceId, "P" + sourceId);

            if (sourceId == 4 && !checkPreviousProcesses(connControl)) {
                updateProcessLog(connControl, logId, "FAILED");
                System.err.println("[ERROR] Dừng ETL vì 1–3 chưa SUCCESS.");
                return;
            }

            try {
                // 6. Load dữ liệu từ transform qua warehouse
                loadToWarehouse(connStaging, connWarehouse, dbStaging, tableStaging, LocalDate.now());
                updateProcessLog(connControl, logId, "SUCCESS");
                System.out.println("[OK] Hoàn tất" + sourceId);
            } catch (Exception ex) {
                updateProcessLog(connControl, logId, "FAILED");
                throw ex;
            }
        }
    }

    private static long insertProcessLog(Connection conn, int sourceId, String processCode) throws SQLException {
        String sql = "INSERT INTO process_log (source_id, status, started_at, process_code) VALUES (?, 'RUNNING', NOW(), ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            ps.setInt(1, sourceId);
            ps.setString(2, processCode);
            ps.executeUpdate();
            try (ResultSet rs = ps.getGeneratedKeys()) {
                return rs.next() ? rs.getLong(1) : 0L;
            }
        }
    }

    private static void updateProcessLog(Connection conn, long processId, String status) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "UPDATE process_log SET status=?, ended_at=NOW() WHERE process_id=?")) {
            ps.setString(1, status);
            ps.setLong(2, processId);
            ps.executeUpdate();
        }
    }

    private static void loadToWarehouse(Connection connStaging, Connection connWarehouse,
                                        String dbStaging, String tableStaging, LocalDate date) throws SQLException {
        String sqlStage = "SELECT number_value, full_date, range_group, is_weekend, is_even FROM "
                + dbStaging + "." + tableStaging;

        List<Map<String, Object>> stageData = new ArrayList<>();
        try (Statement stmt = connStaging.createStatement();
             ResultSet rs = stmt.executeQuery(sqlStage)) {
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                LocalDate fullDate = rs.getDate("full_date").toLocalDate();
                int dateKey = Integer.parseInt(fullDate.toString().replace("-", ""));
                row.put("number_value", rs.getString("number_value"));
                row.put("full_date", fullDate);
                row.put("is_weekend", rs.getInt("is_weekend"));
                row.put("is_even", rs.getInt("is_even"));
                row.put("date_key", dateKey);
                stageData.add(row);
            }
        }

        loadDimDate(stageData, connWarehouse);
        Map<String, Integer> numberKeyMap = loadDimNumber(stageData, connWarehouse);
        loadFactPrize(stageData, connWarehouse, numberKeyMap, "fact_prize");

        System.out.println("[OK] ETL hoàn tất");
    }

    // --- DIM DATE ---
    private static void loadDimDate(List<Map<String, Object>> stageData, Connection connWarehouse) throws SQLException {
        Set<Integer> existingDateKeys = new HashSet<>();
        try (Statement st = connWarehouse.createStatement();
             ResultSet rs = st.executeQuery("SELECT date_key FROM dim_date")) {
            while (rs.next()) existingDateKeys.add(rs.getInt("date_key"));
        }

        String insert = "INSERT INTO dim_date (date_key, full_date, last_appeared_date, is_weekend) VALUES (?,?,?,?)";
        try (PreparedStatement ps = connWarehouse.prepareStatement(insert)) {
            Set<Integer> inserted = new HashSet<>();
            for (Map<String, Object> row : stageData) {
                int dateKey = (int) row.get("date_key");
                if (!existingDateKeys.contains(dateKey) && !inserted.contains(dateKey)) {
                    ps.setInt(1, dateKey);
                    ps.setDate(2, Date.valueOf((LocalDate) row.get("full_date")));
                    ps.setNull(3, Types.DATE);
                    ps.setInt(4, (int) row.get("is_weekend"));
                    ps.addBatch();
                    inserted.add(dateKey);
                }
            }
            ps.executeBatch();
        }
    }

    // --- DIM NUMBER ---
    private static Map<String, Integer> loadDimNumber(List<Map<String, Object>> stageData, Connection connWarehouse) throws SQLException {
        Set<String> existing = new HashSet<>();
        try (Statement st = connWarehouse.createStatement();
             ResultSet rs = st.executeQuery("SELECT number_value FROM dim_number")) {
            while (rs.next()) existing.add(rs.getString("number_value"));
        }

        String insert = "INSERT INTO dim_number (number_value, is_even, last_digit) VALUES (?, ?, ?)";
        try (PreparedStatement ps = connWarehouse.prepareStatement(insert)) {
            for (Map<String, Object> row : stageData) {
                String val = (String) row.get("number_value");
                if (!existing.contains(val)) {
                    int isEven = (int) row.get("is_even");
                    int lastDigit = Integer.parseInt(val) % 10;
                    ps.setString(1, val);
                    ps.setInt(2, isEven);
                    ps.setInt(3, lastDigit);
                    ps.addBatch();
                }
            }
            ps.executeBatch();
        }

        Map<String, Integer> map = new HashMap<>();
        try (Statement st = connWarehouse.createStatement();
             ResultSet rs = st.executeQuery("SELECT number_key, number_value FROM dim_number")) {
            while (rs.next()) map.put(rs.getString("number_value"), rs.getInt("number_key"));
        }
        return map;
    }

    // --- FACT PRIZE ---
    private static void loadFactPrize(List<Map<String, Object>> stageData, Connection connWarehouse,
                                      Map<String, Integer> numMap, String tableName) throws SQLException {

        stageData.sort(Comparator.comparing((Map<String, Object> r) -> (String) r.get("number_value"))
                .thenComparing(r -> (LocalDate) r.get("full_date")));

        Map<String, LocalDate> lastSeen = new HashMap<>();
        String insert = "INSERT INTO " + tableName +
                " (date_key, number_key, occurrence_count, total_draws, probability_value, days_since_last) " +
                "VALUES (?, ?, ?, ?, ?, ?)";

        int totalDraws = (int) stageData.stream().map(r -> r.get("full_date")).distinct().count();

        try (PreparedStatement ps = connWarehouse.prepareStatement(insert)) {
            for (Map<String, Object> row : stageData) {
                String num = (String) row.get("number_value");
                LocalDate date = (LocalDate) row.get("full_date");
                int dateKey = (int) row.get("date_key");
                int numKey = numMap.getOrDefault(num, 0);
                int days = lastSeen.containsKey(num)
                        ? (int) ChronoUnit.DAYS.between(lastSeen.get(num), date)
                        : 0;

                lastSeen.put(num, date);
                ps.setInt(1, dateKey);
                ps.setInt(2, numKey);
                ps.setInt(3, 1);
                ps.setInt(4, totalDraws);
                ps.setBigDecimal(5, BigDecimal.valueOf(1.0 / totalDraws));
                ps.setInt(6, days);
                ps.addBatch();
            }
            ps.executeBatch();
        }

        try (PreparedStatement ps2 = connWarehouse.prepareStatement(
                "UPDATE dim_date SET last_appeared_date=? WHERE date_key=?")) {
            for (LocalDate d : lastSeen.values()) {
                int key = Integer.parseInt(d.toString().replace("-", ""));
                ps2.setDate(1, Date.valueOf(d));
                ps2.setInt(2, key);
                ps2.addBatch();
            }
            ps2.executeBatch();
        }
    }

    private static boolean checkPreviousProcesses(Connection conn) throws SQLException {
        List<String> required = List.of("P1", "P2", "P3");
        Map<String, String> statusMap = new HashMap<>();

        String sql = "SELECT process_code, status FROM process_log WHERE process_code IN ('P1','P2','P3')";
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                statusMap.put(rs.getString("process_code"), rs.getString("status"));
            }
        }

        for (String p : required) {
            String status = statusMap.get(p);
            if (status == null || !status.equalsIgnoreCase("SUCCESS")) {
                System.err.println("[WARN] Process " + p + " chưa SUCCESS");
                return false;
            }
        }

        System.out.println("[OK] 1-3 SUCCESS, tiếp tục P4...");
        return true;
    }

    private static class SourceInfo {
        String sourceName, scriptPath, destinationStaging, aggregateTable;

        public SourceInfo(String n, String s, String d, String a) {
            this.sourceName = n;
            this.scriptPath = s;
            this.destinationStaging = d;
            this.aggregateTable = a;
        }
    }
}
