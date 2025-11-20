import java.sql.*;
import java.util.HashMap;
import java.util.Map;

class Util {
    // Lấy cấu hình từ bảng config_database
    public static Map<String, String> getConfigDatabase(Connection conn) throws SQLException {
        Map<String, String> config = new HashMap<>();
        String sql = "SELECT config_key, config_value FROM config_database";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                config.put(rs.getString("config_key"), rs.getString("config_value"));
            }
        }
        return config;
    }

    // Tạo database + gọi Stored Procedure tạo bảng warehouse
    public static void createWarehouse(DbConfig rootConfig, DbConfig warehouseConfig) throws SQLException {

        String dbName = warehouseConfig.getDatabase();
        String urlRoot = "jdbc:mysql://" + rootConfig.getHost() + ":" + rootConfig.getPort() + "/";
        String urlWarehouse = "jdbc:mysql://" + rootConfig.getHost() + ":" + rootConfig.getPort() + "/" + dbName;

        // 1. Tạo database nếu chưa có
        try (Connection conn = DriverManager.getConnection(urlRoot, rootConfig.getUsername(), rootConfig.getPassword())) {
            Statement stmt = conn.createStatement();
            stmt.execute(
                    "CREATE DATABASE IF NOT EXISTS " + dbName +
                            " CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci"
            );
            System.out.println("[OK] Database " + dbName + " đã tồn tại hoặc vừa được tạo mới.");
        }

        // Gọi stored procedure sp_create_warehouse_tables để tạo bảng
        try (Connection conn = DriverManager.getConnection(urlWarehouse, warehouseConfig.getUsername(), warehouseConfig.getPassword())) {

            CallableStatement cs = conn.prepareCall("{CALL sp_create_warehouse_tables()}");
            cs.execute();

            System.out.println("[OK] Các bảng warehouse đã được tạo qua Stored Procedure.");
        }
    }
}
