package vn.edu.hcmuaf.fit;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.sql.*;

public class Util {
    // Tạo bảng staging nếu chưa có
    public static void createStagingTable(Connection conn, String tableName) throws SQLException {
        String sql = String.format("""
                CREATE TABLE IF NOT EXISTS %s (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    prize VARCHAR(50),
                    number_value VARCHAR(50),
                    full_date VARCHAR(50),
                    load_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """, tableName);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            System.out.println("📋 Bảng staging đã sẵn sàng: " + tableName);
        }
    }
}
