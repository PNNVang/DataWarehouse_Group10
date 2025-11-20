package python2;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.openqa.selenium.support.ui.ExpectedConditions;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class LotteryCrawler {
    private static final DateTimeFormatter INPUT_DATE_FORMAT = DateTimeFormatter.ofPattern("dd-MM-yyyy");
    private static final DateTimeFormatter FILE_DATE_FORMAT = DateTimeFormatter.ofPattern("ddMMyyyy");
    private static final DateTimeFormatter CREATED_AT_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    private static final LocalTime DRAW_TIME = LocalTime.of(19, 0);

    DatabaseConnector connection;
    Connection conn;

    public LotteryCrawler(String pathFile, int sourceId, String dateStr) {
        connection = new DatabaseConnector(pathFile);
        conn = connection.getConnection();
        if (conn == null || connection == null) {
            System.err.println("Kết nối database thất bại");
            return;
        }
        try {
            LocalDateTime startTime = LocalDateTime.now();
            DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("dd-MM-yyyy");
            LocalDate date;

            if (dateStr == null) {
                date = LocalDate.now();
            } else {
                date = LocalDate.parse(dateStr, inputFormatter);
            }

            LocalDate today = LocalDate.now();
            LocalTime now = LocalTime.now();

            // 1.1.3 Kiểm tra thông số date
            // 1.3.4 date > hôm nay
            if (date.isAfter(today)) {
                // 1.3.5 insert vào process_log với status= FAIL
                insertProcessLog(sourceId, "FAIL", startTime, LocalDateTime.now(), "VALIDATE_DATE");
                // 1.3.6 Thông báo ngày nhập không hợp lệ
                System.out.println("Ngày nhập không hợp lệ");
                return;
            }
            // 1.2.4 date = hôm nay và giờ < 19h
            if (date.isEqual(today) && now.isBefore(LocalTime.of(19, 0))) {
                // 1.2.5 insert vào process_log với status= FAIL
                insertProcessLog(sourceId, "FAIL", startTime, LocalDateTime.now(), "NOT_PUBLISHED_YET");
                // 1.2.6 Thông báo kết quả sổ xố chưa được công bố
                System.out.println("Hiện tại chưa đến 19h. Kết quả xổ số chưa được công bố.");
                return;
            }
            // 1.1.4 date hợp lệ
            crawlLottery(date.format(inputFormatter), sourceId);

        } catch (Exception e) {
            System.err.println("Lỗi khi chạy chương trình: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Đóng kết nối database
            closeConnection();
        }
    }

    static class LotteryResult {
        String date;
        String prizeName;
        String numberValue;
        String createdAt;

        public LotteryResult(String date, String prizeName, String numberValue, String createdAt) {
            this.date = date;
            this.prizeName = prizeName;
            this.numberValue = numberValue;
            this.createdAt = createdAt;
        }

        public String toCsv() {
            return String.format("%s,%s,%s,%s",
                    prizeName, numberValue, date, createdAt);
        }
    }

    static class ConfigSource {
        int sourceId;
        String sourceName;
        String sourceUrl;
        String fileLocation;

        public ConfigSource(int sourceId, String sourceName, String sourceUrl, String fileLocation) {
            this.sourceId = sourceId;
            this.sourceName = sourceName;
            this.sourceUrl = sourceUrl;
            this.fileLocation = fileLocation;
        }
    }

    public static void main(String[] args) {
        // 1.1.0 Load file control
        // [input: control.xml, source_id, date (default = today)]
        int sourceId = (args.length > 0 && args[1] != null && !args[1].isEmpty())
                ? Integer.parseInt(args[1])
                : 1;
//        String pathFile = args[0];
        String pathFile = "D:\\Datawarehouse\\fileXML\\control.xml";

//        String dateStr = (args.length > 2 && args[2] != null && !args[2].isEmpty())
//                ? args[2]
//                : null;
        String dateStr = "18-11-2025";
        LotteryCrawler crawler = new LotteryCrawler(pathFile, sourceId, dateStr);
    }

    public void crawlLottery(String date, int sourceId) {
        LocalDateTime startTime = LocalDateTime.now();
        WebDriver driver = null;

        try {
            // 1.1.5 Lấy ra các trường thông tin trong config_source với điều kiện source_id nhập vào
            ConfigSource config = getConfigSource(sourceId);
            if (config == null) {
                System.err.println("Không tìm thấy thông tin config cho source_id: " + sourceId);
                // 1.2.6 insert vào process_log với status= FAIL
                insertProcessLog(sourceId, "FAIL", startTime, LocalDateTime.now(), "CONFIG_NOT_FOUND");
                return;
            }
            // 1.1.6 Khởi tạo ChromeDriver và truy cập đến trang crawl
            String url = String.format(config.sourceUrl, date);
            ChromeOptions options = new ChromeOptions();
            options.addArguments("--headless");
            options.addArguments("--no-sandbox");
            options.addArguments("--disable-dev-shm-usage");
            options.addArguments("--disable-gpu");
            options.addArguments("--window-size=1920,1080");
            options.addArguments("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36");

            driver = new ChromeDriver(options);
            driver.manage().timeouts().implicitlyWait(Duration.ofSeconds(10));
            driver.get(url);

            // 1.1.7 Duyệt từng giải thưởng
            WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(15));
            wait.until(ExpectedConditions.presenceOfElementLocated(
                    By.cssSelector("table.bkqtinhmienbac")));
            WebElement mbTable = driver.findElement(By.cssSelector("table.bkqtinhmienbac"));
            if (mbTable == null) {
                insertProcessLog(sourceId, "FAIL", startTime, LocalDateTime.now(), "TABLE_NOT_FOUND");
                return;
            }

            String actualDate = null;
            try {
                WebElement ngayElement = mbTable.findElement(By.cssSelector("td.ngay a"));
                String dateText = ngayElement.getText().trim();
                actualDate = dateText.replace("/", "-");
                System.out.println("Ngày lấy từ web: " + actualDate);
            } catch (Exception e) {
                actualDate = date;
            }

            LocalDate dateObj = LocalDate.parse(actualDate, INPUT_DATE_FORMAT);

            boolean hasResult = false;
            try {
                List<WebElement> giaiDB = mbTable.findElements(By.cssSelector("td.giaidb div.giaiSo"));
                if (!giaiDB.isEmpty() && !giaiDB.get(0).getText().trim().isEmpty()) {
                    hasResult = true;
                }
            } catch (Exception e) {
            }

            if (!hasResult) {
                System.out.println("Warning: Kết quả xổ số chưa được công bố!");
                System.out.println("Vui lòng chạy lại sau 7h tối.");
                insertProcessLog(sourceId, "FAIL", startTime, LocalDateTime.now(), "RESULT_NOT_AVAILABLE");
                return;
            }
            List<LotteryResult> results = new ArrayList<>();
            String createdAt = ZonedDateTime.now().format(CREATED_AT_FORMAT);
            String[][] prizeMappings = {
                    {"giaidb", "Giải Đặc Biệt"},
                    {"giai1", "Giải Nhất"},
                    {"giai2", "Giải Nhì"},
                    {"giai3", "Giải Ba"},
                    {"giai4", "Giải Tư"},
                    {"giai5", "Giải Năm"},
                    {"giai6", "Giải Sáu"},
                    {"giai7", "Giải Bảy"}
            };

            // 1.1.8 Thêm vào danh sách
            for (String[] prizeMapping : prizeMappings) {
                String prizeClass = prizeMapping[0];
                String prizeName = prizeMapping[1];

                try {
                    List<WebElement> prizeRows = mbTable.findElements(
                            By.cssSelector("td." + prizeClass));

                    for (WebElement row : prizeRows) {
                        List<WebElement> numbers = row.findElements(
                                By.cssSelector("div.giaiSo"));

                        for (WebElement numDiv : numbers) {
                            String numberValue = numDiv.getText().trim();

                            if (!numberValue.isEmpty()) {
                                LotteryResult result = new LotteryResult(
                                        actualDate, prizeName, numberValue, createdAt
                                );

                                results.add(result);
                                System.out.printf("%s: %s%n", prizeName, numberValue);
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("Warning: Error crawling " + prizeName + ": " + e.getMessage());
                }
            }
            // 1.1.9 Kiểm tra nếu có dữ liệu
            if (!results.isEmpty()) {
                String fileName = String.format("data_%s.csv", dateObj.format(FILE_DATE_FORMAT));
                String filePath = config.fileLocation + "\\" + fileName;
                // 1.1.10 Xuất file và lưu
                exportToCsv(results, filePath, sourceId, startTime);

                // 1.1.11 insert vào process_log với status= SUCCESS
                insertProcessLog(sourceId, "SUCCESS", startTime, LocalDateTime.now(), "P1");
            } else {
                // 1.2.10 insert vào process_log với status= FAIL
                insertProcessLog(sourceId, "FAIL", startTime, LocalDateTime.now(),
                        "Không có kết quả nào được crawl");
            }

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            insertProcessLog(sourceId, "FAIL", startTime, LocalDateTime.now(),
                    "Lỗi: " + e.getMessage());
        } finally {
            // 1.1.12 Đóng ChromeDriver
            if (driver != null) {
                driver.quit();
            }
        }
    }

    private void exportToCsv(List<LotteryResult> results, String filePath, int sourceId,
                             LocalDateTime startTime) throws IOException {
        try (FileWriter writer = new FileWriter(filePath, java.nio.charset.StandardCharsets.UTF_8)) {

            writer.write('\ufeff');

            // Write header
            writer.write("prize,number_value,full_date,created_at\n");

            // Write data
            for (LotteryResult result : results) {
                writer.write(result.toCsv() + "\n");
            }

            System.out.println("Xuất File CSV thành công: " + filePath);
        }
    }

    /**
     * Lấy thông tin config từ bảng config_source theo source_id
     */
    public ConfigSource getConfigSource(int sourceId) {
        String query = "SELECT source_id, source_name, source_url, file_location " +
                "FROM config_source WHERE source_id = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setInt(1, sourceId);
            ResultSet rs = pstmt.executeQuery();

            if (rs.next()) {
                ConfigSource config = new ConfigSource(
                        rs.getInt("source_id"),
                        rs.getString("source_name"),
                        rs.getString("source_url"),
                        rs.getString("file_location")
                );
                System.out.println("Lấy config thành công cho source_id: " + sourceId);
                rs.close();
                return config;
            } else {
                System.out.println("Không tìm thấy source_id: " + sourceId);
                rs.close();
                return null;
            }
        } catch (SQLException e) {
            System.err.println("Lỗi khi lấy config_source: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Thêm log vào bảng process_log
     */
    public boolean insertProcessLog(int sourceId, String status, LocalDateTime startTime,
                                    LocalDateTime endTime, String processCode) {
        String sql = "INSERT INTO process_log (source_id, process_code, status, started_at, ended_at) " +
                "VALUES (?, ?, ?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, sourceId);
            ps.setString(2, processCode);
            ps.setString(3, status);

            if (startTime != null) {
                ps.setTimestamp(4, Timestamp.valueOf(startTime));
            } else {
                ps.setTimestamp(4, null);
            }

            if (endTime != null) {
                ps.setTimestamp(5, Timestamp.valueOf(endTime));
            } else {
                ps.setTimestamp(5, null);
            }

            ps.executeUpdate();
            System.out.println("Thêm log thành công cho source_id: " + sourceId + ", status: " + status);
            return true;

        } catch (SQLException e) {
            System.err.println("Lỗi khi insert process_log: " + e.getMessage());
            e.printStackTrace();
        }
        return false;
    }

    public boolean updateProcessLog(int processId, int sourceId, String status, String processCode) {
        String query;
        if (processCode != null) {
            query = "UPDATE process_log SET ended_at = NOW(), status = ?, process_code = ? " +
                    "WHERE process_id = ? AND source_id = ?";
        } else {
            query = "UPDATE process_log SET ended_at = NOW(), status = ? " +
                    "WHERE process_id = ? AND source_id = ?";
        }

        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, status);

            if (processCode != null) {
                pstmt.setString(2, processCode);
                pstmt.setInt(3, processId);
                pstmt.setInt(4, sourceId);
            } else {
                pstmt.setInt(2, processId);
                pstmt.setInt(3, sourceId);
            }

            int affectedRows = pstmt.executeUpdate();
            if (affectedRows > 0) {
                System.out.println("Update log thành công! Process ID: " + processId);
                return true;
            }
        } catch (SQLException e) {
            System.err.println("Lỗi khi update process_log: " + e.getMessage());
            e.printStackTrace();
        }
        return false;
    }

    /**
     * Đóng kết nối database
     */
    private void closeConnection() {
        try {
            if (conn != null && !conn.isClosed()) {
                conn.close();
                System.out.println("Đã đóng kết nối database thành công");
            }
        } catch (SQLException e) {
            System.err.println("Lỗi khi đóng kết nối database: " + e.getMessage());
            e.printStackTrace();
        }
    }
}