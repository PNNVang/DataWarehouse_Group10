package vn.edu.hcmuaf.fit;

import java.io.*;
import java.sql.*;
import java.time.LocalDateTime;

public class CsvLoader {
    public static int loadAll(Connection conn, File folder) throws Exception {
        if (!folder.exists() || !folder.isDirectory())
            throw new Exception("❌ Thư mục CSV không tồn tại: " + folder.getAbsolutePath());

        File[] files = folder.listFiles((dir, name) -> name.toLowerCase().endsWith(".csv"));
        if (files == null || files.length == 0) {
            System.out.println("⚠️ Không có file CSV nào trong thư mục.");
            return 0;
        }

        int total = 0;
        for (File f : files) {
            System.out.println("📥 Đang load file: " + f.getName());
            total += loadCsv(conn, f);
        }
        return total;
    }

    private static int loadCsv(Connection conn, File csvFile) throws Exception {
        int count = 0;
        String sql = """
                INSERT INTO stg_lottery_data (prize, number_value, full_date, is_weekend, is_even, load_timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
                """;
        try (BufferedReader reader = new BufferedReader(new FileReader(csvFile));
             PreparedStatement ps = conn.prepareStatement(sql)) {
            String line;
            boolean skip = true;
            while ((line = reader.readLine()) != null) {
                if (skip) { skip = false; continue; }
                String[] p = line.split(",");
                if (p.length < 5) continue;
                ps.setString(1, p[0].trim());
                ps.setString(2, p[1].trim());
                ps.setString(3, p[2].trim());
                ps.setString(4, p[3].trim());
                ps.setString(5, p[4].trim());
                ps.setTimestamp(6, Timestamp.valueOf(LocalDateTime.now()));
                ps.addBatch();
                count++;
            }
            ps.executeBatch();
        }
        return count;
    }
}
