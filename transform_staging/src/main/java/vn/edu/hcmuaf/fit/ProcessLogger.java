package vn.edu.hcmuaf.fit;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ProcessLogger {

    public static void log(Connection conn, String processId, int rows, String status, String errorMsg) {
        try {
            // 🔹 Cập nhật thời gian chạy cuối trong bảng process_config
            String updateSql = "UPDATE process_config SET last_run = NOW() WHERE id = ?";
            try (PreparedStatement pstmt = conn.prepareStatement(updateSql)) {
                pstmt.setString(1, processId);
                pstmt.executeUpdate();
            }

            // 🔹 Ghi log tiến trình transform
            String insertSql = """
                INSERT INTO log (process_id, start_time, end_time, status, message)
                VALUES (?, NOW(), NOW(), ?, ?)
            """;

            String message;
            if ("SUCCESS".equalsIgnoreCase(status)) {
                message = "Đã transform thành công " + rows + " bản ghi Giải 7 sang bảng transform.";
            } else {
                message = "Lỗi khi transform dữ liệu: " + (errorMsg != null ? errorMsg : "Không rõ nguyên nhân.");
            }

            try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
                pstmt.setString(1, processId);
                pstmt.setString(2, status);
                pstmt.setString(3, message);
                pstmt.executeUpdate();
            }

            System.out.println("🧾 Ghi log thành công cho tiến trình " + processId + " (" + status + ")");
        } catch (SQLException e) {
            System.err.println("⚠️ Không thể ghi log tiến trình " + processId);
            e.printStackTrace();
        }
    }
}
