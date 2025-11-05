package vn.edu.hcmuaf.fit;

import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class LotteryTransform {

    public static void main(String[] args) {
        try {
            // === Nhận tham số ===
            String processCode = (args.length > 1) ? args[1] : "P3";
            String loadDate = (args.length > 0 && !args[0].isBlank())
                    ? args[0]
                    : LocalDate.now().toString();

            System.out.println("📅 Ngày load: " + loadDate);
            System.out.println("⚙️  Mã tiến trình: " + processCode);

            // === Kết nối DB control ===
            DatabaseConnector db = new DatabaseConnector("control.xml");
            try (Connection controlConn = db.getConnection()) {
                if (controlConn == null) return;

                // === Kiểm tra tiến trình P2 đã thành công hôm nay chưa ===
                String checkPrev = """
                    SELECT COUNT(*) 
                    FROM process_log 
                    WHERE process_code = 'P2' 
                      AND status = 'SUCCESS'
                      AND DATE(ended_at) = CURDATE()
                """;
                try (Statement stmt = controlConn.createStatement();
                     ResultSet rs = stmt.executeQuery(checkPrev)) {
                    rs.next();
                    if (rs.getInt(1) == 0) {
                        System.out.println("⚠️ Tiến trình P2 chưa hoàn thành hôm nay. Dừng ETL P3.");
                        return;
                    }
                }

                // === Lấy cấu hình staging & transform từ bảng config_source ===
                String getStagingTable = "SELECT destination_staging FROM config_source WHERE source_name = 'load_staging_data'";
                String getTransformTable = "SELECT destination_staging FROM config_source WHERE source_name = 'transform_staging_data'";

                String stagingTable = null;
                String transformTable = null;

                try (Statement stmt = controlConn.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery(getStagingTable)) {
                        if (rs.next()) stagingTable = rs.getString("destination_staging");
                    }
                    try (ResultSet rs = stmt.executeQuery(getTransformTable)) {
                        if (rs.next()) transformTable = rs.getString("destination_staging");
                    }
                }

                if (stagingTable == null || transformTable == null) {
                    System.err.println("❌ Không tìm thấy cấu hình staging/transform trong bảng config_source.");
                    return;
                }

                // === Kết nối tới DB staging ===
                String jdbcUrl = "jdbc:mysql://localhost:3306/staging";
                String username = "root";
                String password = "";

                try (Connection stagingConn = DriverManager.getConnection(jdbcUrl, username, password)) {
                    System.out.println("✅ Kết nối thành công đến DB staging!");

                    // Tạo bảng transform nếu chưa có
                    createTransformTable(stagingConn, transformTable);

                    // Thực hiện transform dữ liệu
                    int totalRows = transformData(stagingConn, stagingTable, transformTable);

                    // Ghi log thành công vào DB control
                    insertProcessLog(controlConn, processCode, 3, "SUCCESS"); // source_id = 3
                    System.out.println("✅ Transform hoàn tất thành công! " + totalRows + " bản ghi được xử lý.");
                } catch (Exception e) {
                    insertProcessLog(controlConn, processCode, 3, "FAILED");
                    System.err.println("❌ Lỗi khi transform: " + e.getMessage());
                    e.printStackTrace();
                }

            } finally {
                db.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // === Tạo bảng transform nếu chưa có ===
    private static void createTransformTable(Connection conn, String transformTable) throws SQLException {
        String createSQL = "CREATE TABLE IF NOT EXISTS " + transformTable + " (" +
                "id INT AUTO_INCREMENT PRIMARY KEY," +
                "number_value INT," +
                "full_date DATE," +
                "range_group VARCHAR(20)," +
                "is_weekend TINYINT," +
                "is_even TINYINT," +
                "load_timestamp TIMESTAMP)";
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(createSQL);
            System.out.println("🧩 Bảng " + transformTable + " đã sẵn sàng!");
        }
    }

    // === Transform dữ liệu từ bảng staging_table ===
    private static int transformData(Connection conn, String stagingTable, String transformTable) throws SQLException {
        String selectSQL = "SELECT prize, number_value, full_date, is_weekend, is_even, load_timestamp FROM "
                + stagingTable + " WHERE prize = 'Giải Bảy'";

        String insertSQL = "INSERT INTO " + transformTable +
                " (number_value, full_date, range_group, is_weekend, is_even, load_timestamp) " +
                "VALUES (?, ?, ?, ?, ?, ?)";

        int count = 0;
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(selectSQL);
             PreparedStatement ps = conn.prepareStatement(insertSQL)) {

            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy");
            while (rs.next()) {
                try {
                    String numStr = rs.getString("number_value").trim();
                    if (numStr.length() < 2) continue;
                    int lastTwo = Integer.parseInt(numStr.substring(numStr.length() - 2));
                    LocalDate date = LocalDate.parse(rs.getString("full_date").trim(), formatter);

                    int lower = (lastTwo / 10) * 10;
                    int upper = lower + 9;
                    String range = String.format("%02d-%02d", lower, upper);

                    int isWeekend = rs.getBoolean("is_weekend") ? 1 : 0;
                    int isEven = rs.getBoolean("is_even") ? 1 : 0;
                    Timestamp loadTs = rs.getTimestamp("load_timestamp");

                    ps.setInt(1, lastTwo);
                    ps.setDate(2, Date.valueOf(date));
                    ps.setString(3, range);
                    ps.setInt(4, isWeekend);
                    ps.setInt(5, isEven);
                    ps.setTimestamp(6, loadTs);

                    ps.addBatch();
                    count++;
                } catch (Exception ex) {
                    System.err.println("⚠️ Bỏ qua bản ghi lỗi: " + ex.getMessage());
                }
            }
            ps.executeBatch();
        }

        System.out.println("✅ Đã transform " + count + " bản ghi Giải 7 từ bảng " + stagingTable + "!");
        return count;
    }

    // === Ghi log vào bảng process_log ===
    private static void insertProcessLog(Connection conn, String processCode, int sourceId, String status) throws SQLException {
        String sql = """
            INSERT INTO process_log (source_id, status, started_at, ended_at, process_code)
            VALUES (?, ?, NOW(), NOW(), ?)
        """;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, sourceId);
            ps.setString(2, status);
            ps.setString(3, processCode);
            ps.executeUpdate();
            System.out.println("🧾 Ghi log tiến trình " + processCode + " (" + status + ") thành công!");
        }
    }
}
