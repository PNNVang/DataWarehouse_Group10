package python2;

import connection.DbConfig;

import java.sql.Connection;
import java.sql.DriverManager;

public class DatabaseConnector {
    Connection conn = null;
    DbConfig config ;
    public DatabaseConnector(String pathFile) {
        try {
            config = XMLConfigReader.readConfig(pathFile);
            if (config == null) {
                return;
            }
            String url = "jdbc:mysql://" + config.getHost() + ":" + config.getPort() + "/" + config.getDatabase()
                    + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
            conn = DriverManager.getConnection(url, config.getUsername(), config.getPassword());
            System.out.println("Kết nối thành công tới Database Control!");

        } catch (Exception e) {
            System.err.println("Lỗi kết nối Database Control: " + e.getMessage());
        }
    }

    public Connection getConnection() {
        return conn;
    }
}

