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

    // Tạo database và các bảng trong warehouse (nếu chưa có)
    public static void createWarehouse(DbConfig rootConfig, DbConfig warehouseConfig) throws SQLException {
        String dbName = warehouseConfig.getDatabase();
        String urlRoot = "jdbc:mysql://" + rootConfig.getHost() + ":" + rootConfig.getPort() + "/";
        String urlWarehouse = "jdbc:mysql://" + rootConfig.getHost() + ":" + rootConfig.getPort() + "/" + dbName;

        try (Connection conn = DriverManager.getConnection(urlRoot, rootConfig.getUsername(), rootConfig.getPassword())) {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE DATABASE IF NOT EXISTS " + dbName +
                    " CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci");
            System.out.println("[OK] Database " + dbName + " đã tồn tại hoặc vừa được tạo mới.");
        }

        try (Connection conn = DriverManager.getConnection(urlWarehouse, warehouseConfig.getUsername(), warehouseConfig.getPassword())) {
            Statement stmt = conn.createStatement();

            stmt.execute("""
                CREATE TABLE IF NOT EXISTS dim_date (
                    date_key INT PRIMARY KEY,
                    full_date DATE,
                    day_of_month INT,
                    month_of_year INT,
                    year_value INT,
                    year_month_value VARCHAR(7),
                    day_name VARCHAR(20),
                    is_weekend TINYINT
                )
            """);

            stmt.execute("""
                CREATE TABLE IF NOT EXISTS dim_number (
                    number_key INT AUTO_INCREMENT PRIMARY KEY,
                    number_value VARCHAR(50),
                    is_even TINYINT,
                    last_digit INT,
                    last_appeared_date DATE
                )
            """);

            stmt.execute("""
                CREATE TABLE IF NOT EXISTS fact_prize (
                    fact_key INT AUTO_INCREMENT PRIMARY KEY,
                    date_key INT,
                    number_key INT,
                    occurrence_count INT,
                    total_draws INT,
                    probability_value DECIMAL(10,6),
                    days_since_last INT,
                    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
                    FOREIGN KEY (number_key) REFERENCES dim_number(number_key)
                )
            """);

            System.out.println("[OK] Các bảng warehouse đã được tạo thành công (nếu chưa có).");
        }
    }
}
