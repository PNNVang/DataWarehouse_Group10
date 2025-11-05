package vn.edu.hcmuaf.fit;

import java.sql.*;
import java.time.LocalDateTime;

public class ProcessLogger {
    public static void log(Connection conn, int sourceId, String status, LocalDateTime startTime, java.time.LocalDateTime endTime, String processCode) {
        try {
            // Ghi log tiến trình
            String insertSql = "INSERT INTO process_log (source_id, status, started_at, ended_at, process_code) VALUES (?, ?, ?, ?, ?)";
            try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                pstmt.setInt(1, sourceId);
                pstmt.setString(2, status);
                pstmt.setString(3, startTime.toString());
                pstmt.setString(4, endTime.toString());
                pstmt.setString(5, processCode);
                pstmt.executeUpdate();
            }

            System.out.println("🧾 Ghi log thành công cho tiến trình " + processCode);
        } catch (SQLException e) {
            System.err.println("⚠️ Không thể ghi log tiến trình!");
            e.printStackTrace();
        }
    }

}
