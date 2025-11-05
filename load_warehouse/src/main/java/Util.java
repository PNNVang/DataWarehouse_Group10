import java.sql.*;

class Util {
    public static void createWarehouse(DbConfig rootConfig, DbConfig warehouseConfig) throws SQLException {
        String dbName = warehouseConfig.getDatabase();
        String urlRoot = "jdbc:mysql://" + rootConfig.getHost() + ":" + rootConfig.getPort() + "/";
        String urlWarehouse = "jdbc:mysql://" + rootConfig.getHost() + ":" + rootConfig.getPort() + "/" + dbName;

        // --- Tạo database nếu chưa có ---
        try (Connection conn = DriverManager.getConnection(
                urlRoot, rootConfig.getUsername(), rootConfig.getPassword())) {
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE DATABASE IF NOT EXISTS " + dbName +
                    " CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci");
            System.out.println("[OK] Database " + dbName + " đã tồn tại hoặc vừa được tạo mới.");
        }

        // --- Tạo bảng trong warehouse ---
        try (Connection conn = DriverManager.getConnection(
                urlWarehouse, warehouseConfig.getUsername(), warehouseConfig.getPassword())) {
            Statement stmt = conn.createStatement();

            stmt.execute("""
                CREATE TABLE IF NOT EXISTS dim_date (
                    date_key INT PRIMARY KEY,
                    full_date DATE,
                    last_appeared_date DATE,
                    is_weekend TINYINT
                )
            """);

            stmt.execute("""
                CREATE TABLE IF NOT EXISTS dim_number (
                    number_key INT AUTO_INCREMENT PRIMARY KEY,
                    number_value VARCHAR(10),
                    is_even TINYINT,
                    last_digit INT
                )
            """);

            stmt.execute("""
                CREATE TABLE IF NOT EXISTS fact_prize (
                    fact_key INT AUTO_INCREMENT PRIMARY KEY,
                    date_key INT,
                    number_key INT,
                    occurrence_count INT,
                    total_draws INT,
                    probability_value DECIMAL(5,4),
                    days_since_last INT,
                    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
                    FOREIGN KEY (number_key) REFERENCES dim_number(number_key)
                )
            """);

            System.out.println("[OK] Các bảng warehouse đã được tạo thành công (nếu chưa có).");
        }
    }
}