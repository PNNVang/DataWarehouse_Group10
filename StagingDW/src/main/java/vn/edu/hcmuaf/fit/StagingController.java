package vn.edu.hcmuaf.fit;

import java.io.File;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;

public class StagingController {
    private final String pathFile;
    private final int sourceId;
    private final String dateStr;

    public StagingController(String pathFile, int sourceId, String dateStr) {
        this.pathFile = pathFile;
        this.sourceId = sourceId;
        this.dateStr = (dateStr == null || dateStr.isBlank())
                ? LocalDate.now().toString()
                : dateStr;
    }

    // Hàm chính xử lý việc đọc CSV và extract vào trong db staging
    public void run() {
        try {
            System.out.println("Ngày load: " + dateStr);
            System.out.println("Source ID: " + sourceId);
            System.out.println("File cấu hình: " + pathFile);

            // 2. Kết nối DB control
            DatabaseConnector db = new DatabaseConnector(pathFile);
            try (Connection conn = db.getConnection()) {
                if (conn == null) return;

                // 3. Kết nối vào bảng process_log lấy ra tiến trình chạy gần đây nhất của P1
                String checkPrevProcess = "SELECT 1 FROM process_log " +
                        "WHERE process_code='P1' AND status='SUCCESS' " +
                        "ORDER BY ended_at DESC " +
                        "LIMIT 1";

                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(checkPrevProcess)) {
                    if (rs.next()) {
                        System.out.println("✅ Tiến trình P1 đã hoàn thành. Bắt đầu ETL P2...");
                        LocalDateTime startTime = LocalDateTime.now();

                        // 4. Đọc cấu hình từ file XML và tạo database staging nếu chưa có
                        DbConfig cfg = db.getConfig(); // Lấy ra thông tin host, port, user, password

                        // Chuẩn bị cấu hình root để tạo DB mới
                        DbConfig rootConfig = new DbConfig();
                        rootConfig.setHost(cfg.getHost());
                        rootConfig.setPort(cfg.getPort());
                        rootConfig.setDatabase(""); // kết nối đến server gốc
                        rootConfig.setUsername(cfg.getUsername());
                        rootConfig.setPassword(cfg.getPassword());

                        DatabaseConnector rootConnector = new DatabaseConnector(rootConfig);
                        String dbStaging = "staging";
                        try (Connection rootConn = rootConnector.getConnection();
                             Statement createStagingStmt = rootConn.createStatement()) {

                            if (rootConn == null) {
                                System.err.println("❌ Không thể kết nối tới MySQL root để tạo DB staging.");
                                return;
                            }

                            // Tạo database staging nếu chưa tồn tại
                            createStagingStmt.executeUpdate("CREATE DATABASE IF NOT EXISTS " + dbStaging);
                            System.out.println("✅ Đã kiểm tra hoặc tạo database 'staging' thành công.");

                        } catch (Exception e) {
                            System.err.println("❌ Lỗi khi tạo DB staging: " + e.getMessage());
                            return;
                        } finally {
                            rootConnector.close();
                        }

                        // 5. Đọc cấu hình thông tin table trong config_source dựa trên sourceId
                        String configQuery = "SELECT destination_staging, file_location FROM config_source WHERE source_id = ?";

                        try (PreparedStatement pstmt = conn.prepareStatement(configQuery)) {
                            pstmt.setInt(1, sourceId);
                            try (ResultSet rsStaging = pstmt.executeQuery()) {
                                if (rsStaging.next()) {
                                    // Lấy ra tên bảng staging cần tạo
                                    String destinationStaging = rsStaging.getString("destination_staging");
                                    String fileLocation = rsStaging.getString("file_location");

                                    // 6. Kết nối DB staging và sau đó tạo bảng staging (nếu chưa có) cho bước load csv
                                    DbConfig stagingConfig = new DbConfig();
                                    stagingConfig.setHost(cfg.getHost());
                                    stagingConfig.setPort(cfg.getPort());
                                    stagingConfig.setDatabase(dbStaging);
                                    stagingConfig.setUsername(cfg.getUsername());
                                    stagingConfig.setPassword(cfg.getPassword());

                                    DatabaseConnector stagingConnector = new DatabaseConnector(stagingConfig);
                                    try (Connection stagingConn = stagingConnector.getConnection()) {

                                        if (stagingConn == null) {
                                            return;
                                        }

                                        Util.createStagingTable(stagingConn, destinationStaging);
                                        System.out.println("✅ Đã kiểm tra hoặc tạo bảng staging: " + destinationStaging);

                                        // 7. Load dữ liệu từ CSV vào staging
                                        int totalRows = CsvLoader.loadAll(stagingConn, new File(fileLocation));
                                        System.out.println("📦 Đã load " + totalRows + " dòng từ file " + fileLocation);
                                        ProcessLogger.log(conn, sourceId, "SUCCESS", startTime, LocalDateTime.now(), "P2");

                                    } catch (Exception e) {
                                        System.err.println("❌ Lỗi trong quá trình ETL: " + e.getMessage());
                                        ProcessLogger.log(conn, sourceId, "FAILED", startTime, LocalDateTime.now(), "P2");
                                        e.printStackTrace();
                                    }
                                    } else {
                                    System.out.println("⚠️ Không tìm thấy cấu hình staging cho sourceId = " + this.sourceId);
                                }
                            }
                        }

                    } else {
                        System.out.println("⚠️ Tiến trình P1 chưa hoàn thành hoặc bị lỗi. Dừng ETL P2.");
                        return;
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                db.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Khai báo hàm main để chạy bằng command line ===
    public static void main(String[] args) {
        String pathFile = (args.length > 0) ? args[0] : "control.xml";
        int sourceId = 2;
        String dateStr = (args.length > 1) ? args[1] : null;

        StagingController staging = new StagingController(pathFile, sourceId, dateStr);
        staging.run();
    }

}
