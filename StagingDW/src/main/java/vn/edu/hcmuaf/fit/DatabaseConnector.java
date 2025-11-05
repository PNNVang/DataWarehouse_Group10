package vn.edu.hcmuaf.fit;

import java.sql.Connection;
import java.sql.DriverManager;

public class DatabaseConnector {
    private Connection conn;
    private DbConfig config;

    // 2. Constructor đọc config từ file XML (dùng cho DB control) ---
    public DatabaseConnector(String configFilePath) {
        this.config = XMLConfigReader.readConfig(configFilePath);
        connect();
    }

    // Constructor nhận DbConfig trực tiếp (dùng cho DB staging) ---
    public DatabaseConnector(DbConfig config) {
        this.config = config;
        connect();
    }

    // 3. Mở kết nối đến database qua JDBC
    public void connect() {
        try {
            String url = "jdbc:mysql://" + config.getHost() + ":" + config.getPort() + "/" + config.getDatabase()
                    + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
            conn = DriverManager.getConnection(url, config.getUsername(), config.getPassword());
            System.out.println("✅ Kết nối thành công tới DB: " + config.getDatabase());
        } catch (Exception e) {
            System.err.println("❌ Lỗi kết nối DB " + config.getDatabase() + ": " + e.getMessage());
        }

    }

    public DbConfig getConfig() {
        return config;
    }

    public Connection getConnection() {
        return conn;
    }

    public void close() {
        try {
            if (conn != null && !conn.isClosed()) {
                conn.close();
                System.out.println("🔒 Đã đóng kết nối DB: " + config.getDatabase());
            }
        } catch (Exception ignored) {}
    }
}

