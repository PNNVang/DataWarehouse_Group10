package python2;

import connection.DbConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseConnector {
    private Connection conn = null;
    private DbConfig config;

    public DatabaseConnector(String pathFile) {
        if (!loadConfigFile(pathFile)) {
            return;
        }
        // 1.1.2 Kết nối với Database
        connectToDatabase();
    }

    // 1.1.1 Load file
    private boolean loadConfigFile(String pathFile) {
        config = XMLConfigReader.readConfig(pathFile);
        if (config == null) {
            // 1.2.2 Thông báo "Đã xảy ra lỗi"
            System.err.println("Đã xảy ra lỗi");
            return false;
        }
        return true;
    }

    // 1.1.2 Kết nối với Database
    private void connectToDatabase() {
        try {
            String url = buildConnectionUrl();
            conn = DriverManager.getConnection(url, config.getUsername(), config.getPassword());
        } catch (SQLException e) {
            // 1.2.3 Thông báo "Kết nối database thất bại"
            System.err.println("Kết nối database thất bại: " + e.getMessage());
            conn = null;
        }
    }

    // Build database connection URL
    private String buildConnectionUrl() {
        return "jdbc:mysql://" + config.getHost() + ":" + config.getPort() + "/"
                + config.getDatabase() + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
    }

    public Connection getConnection() {
        return conn;
    }

    public void closeConnection() {
        try {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        } catch (SQLException e) {
            System.err.println("Lỗi khi đóng kết nối database: " + e.getMessage());
        }
    }
}