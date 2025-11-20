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
            LocalDateTime startTime = LocalDateTime.now();
            System.out.println("Source ID: " + sourceId);
            System.out.println("File cấu hình: " + pathFile);

            // 4. Kết nối DB control
            DatabaseConnector db = new DatabaseConnector(pathFile);
            Connection conn = null;
            try {
                conn = db.getConnection();
                if (conn == null) {
                    System.err.println("❌ Không thể kết nối DB control.");
                    // 4.1. Log FAILED cho lỗi kết nối
                    ProcessLogger.log(null, sourceId, "FAILED", startTime, LocalDateTime.now(), "P2", "Không thể kết nối DB control.");
                    return;
                }

                // 5. Kiểm tra điều kiện chạy tiến trình
                String checkPrevProcess = "SELECT 1 FROM process_log " +
                        "WHERE process_code='P1' AND status='SUCCESS' " +
                        "ORDER BY ended_at DESC " +
                        "LIMIT 1";

                String checkCurrentProcess = "SELECT 1 FROM process_log " +
                        "WHERE process_code='P2' AND status='RUNNING' AND started_at = NOW()" +
                        "LIMIT 1";

                try (Statement stmt = conn.createStatement();
                     // 5.1. Kiểm tra tiến trình P1
                     ResultSet rs = stmt.executeQuery(checkPrevProcess)) {
                    if (rs.next()) {
                        System.out.println("✅ Tiến trình P1 đã hoàn thành.");
                        // 5.2. Kiểm tra tiến trình P2
                        try (ResultSet rsCurr = stmt.executeQuery(checkCurrentProcess)) {
                            if (rsCurr.next()) {
                                System.out.println("⚠️ Tiến trình P2 đang chạy. Dừng tiến trình hiện tại.");
                                return;
                            } else {
                                System.out.println("✅ Không có tiến trình P2 nào đang chạy. Bắt đầu ETL P2...");
                            }
                        }
                    } else {
                        System.out.println("⚠️ Tiến trình P1 chưa hoàn thành hoặc bị lỗi. Dừng tiến trình P2.");
                        return;
                    }

                    // 5.3. Log RUNNING vào process_log
                    ProcessLogger.log(conn, sourceId, "RUNNING", startTime, LocalDateTime.now(), "P2", "Bắt đầu tiến trình P2");

                    // 6. Gọi procedure sp_prepare_staging_db
                    String dbStaging = null;
                    String callProc = "{CALL sp_prepare_staging_db(?, ?)}";

                    try (CallableStatement cstmt = conn.prepareCall(callProc)) {
                        //  6.2. Set input là sourceId và Register Output
                        cstmt.setInt(1, sourceId);              // input
                        cstmt.registerOutParameter(2, Types.VARCHAR); // output

                        // 6.3. Thực thi Procedure và lấy tên DB staging
                        cstmt.execute();
                        dbStaging = cstmt.getString(2);

                        // 7. Gọi procedure sp_get_staging_info
                        String callProcInfo = "{CALL sp_get_staging_info(?, ?, ?)}";
                        String destinationStaging = null;
                        String fileLocation = null;

                        try (CallableStatement callableStatement = conn.prepareCall(callProcInfo)) {
                            //  7.2. Set input là sourceId và Register Output
                            callableStatement.setInt(1, sourceId);
                            callableStatement.registerOutParameter(2, Types.VARCHAR); // destination_staging
                            callableStatement.registerOutParameter(3, Types.VARCHAR); // file_location

                            // 7.3. Thực thi Procedure và lấy destinationStaging, fileLocation
                            callableStatement.execute();
                            destinationStaging = callableStatement.getString(2);
                            fileLocation = callableStatement.getString(3);

                            if (destinationStaging == null || fileLocation == null) {
                                // 7.4. Kiểm tra dữ liệu destinationStaging, fileLocation
                                System.out.println("⚠️ Không tìm thấy cấu hình staging cho sourceId = " + sourceId);
                                return;
                            }

                            // 7.5. Kết nối DB staging
                            DbConfig cfg = db.getConfig();
                            DbConfig stagingConfig = new DbConfig();
                            stagingConfig.setHost(cfg.getHost());
                            stagingConfig.setPort(cfg.getPort());
                            stagingConfig.setDatabase(dbStaging);
                            stagingConfig.setUsername(cfg.getUsername());
                            stagingConfig.setPassword(cfg.getPassword());

                            DatabaseConnector stagingConnector = new DatabaseConnector(stagingConfig);
                            try (Connection stagingConn = stagingConnector.getConnection()) {

                                if (stagingConn == null) {
                                    System.err.println("❌ Không thể kết nối DB staging");
                                    return;
                                }

                                // 7.7. Tạo bảng stg_lottery_data
                                Util.createStagingTable(stagingConn, destinationStaging);
                                System.out.println("✅ Đã kiểm tra hoặc tạo bảng staging: " + destinationStaging);

                                // 7.8. Load dữ liệu từ CSV vào staging
                                int totalRows = CsvLoader.loadAll(stagingConn, new File(fileLocation));
                                String message = "Đã load " + totalRows + " dòng từ file " + fileLocation;

                                // 7.9. Log SUCCESS vào process_log
                                ProcessLogger.log(conn, sourceId, "SUCCESS", startTime, LocalDateTime.now(), "P2", message);

                            } catch (Exception ex) {
                                System.err.println("❌ Lỗi trong quá trình ETL: " + ex.getMessage());
                                // 7.6. Log FAILED vào process_log
                                ProcessLogger.log(conn, sourceId, "FAILED", startTime, LocalDateTime.now(), "P2", ex.getMessage());
                                ex.printStackTrace();
                            }

                        } catch (SQLException exception) {
                            // 6.1. Log FAILED cho lỗi khi gọi procedure sp_get_staging_info
                            System.err.println("❌ Lỗi khi gọi sp_get_staging_info: " + exception.getMessage());
                            ProcessLogger.log(conn, sourceId, "FAILED", startTime, LocalDateTime.now(), "P2", exception.getMessage());
                        }

                    } catch (SQLException e) {
                        // 5.1. Log FAILED cho lỗi khi gọi procedure sp_prepare_staging_db
                        System.err.println("❌ Lỗi khi gọi procedure sp_prepare_staging_db: " + e.getMessage());
                        ProcessLogger.log(conn, sourceId, "FAILED", startTime, LocalDateTime.now(), "P2", e.getMessage());
                    }
                }
            } catch (SQLException e) {
                System.err.println("❌ Lỗi xảy ra khi làm việc với DB control: " + e.getMessage());
                e.printStackTrace();

            } finally {
                // 8. Đóng kết nối DB Control
                db.close();
                if (conn != null) {
                    conn.close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 1. JVM khởi động và gọi StagingController.main()
    public static void main(String[] args) {
    // 1.1. Xử lý tham số và khởi tạo StagingController
        String pathFile = (args.length > 0) ? args[0] : "control.xml";
        int sourceId = (args.length > 1) ? Integer.parseInt(args[1]) : 2;
        String dateStr = (args.length > 2) ? args[2] : null;

        StagingController staging = new StagingController(pathFile, sourceId, dateStr);
    // 1.2. Gọi hàm run() trong StagingController
        staging.run();
    }

}
