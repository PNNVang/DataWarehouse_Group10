package vn.edu.hcmuaf.fit;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Util {

    /**
     * 🔹 Tách giá trị từ chuỗi cấu hình trong cột database_connect
     * Ví dụ: "host: localhost, port: 3306, username: root, password:"
     * extractValue(config, "host") -> "localhost"
     */
    public static String extractValue(String config, String field) {
        if (config == null || field == null) return "";

        String[] parts = config.split(",");
        for (String part : parts) {
            String[] kv = part.trim().split(":", 2); // ⚙️ dùng split(":", 2) để không lỗi khi có dấu ":" trong password
            if (kv.length == 2 && kv[0].trim().equalsIgnoreCase(field)) {
                return kv[1].trim();
            }
        }
        return "";
    }

    /**
     * 🔹 Lấy giá trị từ bảng config theo khóa config_key
     * Ví dụ: getConfigValue(conn, "staging_table") → "stg_lottery_data"
     */
    public static String getConfigValue(Connection conn, String key) throws SQLException {
        if (conn == null || key == null) return null;

        String sql = "SELECT config_value FROM config WHERE config_key = ?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("config_value") != null ? rs.getString("config_value").trim() : null;
                }
            }
        }
        return null;
    }
}
