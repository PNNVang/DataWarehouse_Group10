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

    private DatabaseConnector connection;
    private Connection conn;

    public static void main(String[] args) {
        // 1.1.0 Load file control
//        String pathFile = args[0];
        String pathFile = "D:\\Datawarehouse\\fileXML\\control.xml";
//        int sourceId = Integer.parseInt(args[1]);
        int sourceId = 1;
//        String dateStr = (args.length > 2 && args[2] != null && !args[2].isEmpty()) ? args[2] : null;
        String dateStr = "20-10-2025";
        LotteryCrawler crawler = new LotteryCrawler(pathFile, sourceId, dateStr);
    }

    public LotteryCrawler(String pathFile, int sourceId, String dateStr) {
        // 1.1.1 Thực hiện load file
        if (!loadFileControl(pathFile)) {

            return;
        }
        // 1.1.2 Kết nối với Database: control
        if (!connectToDatabase()) {
            return;
        }

        // Parse date
        LocalDate date = parseDateInput(dateStr);

        // 1.1.3 Kiểm tra thông số date
        if (!validateDate(date)) {
            return;
        }
        // 1.1.4 Kiểm tra source_id
        ConfigSource config = getConfigSource(sourceId);
        if (config == null) {
            // 1.2.5 Thông báo "Không tìm thấy record để thực hiện"
            System.out.println("Không tìm được record để thực hiện");
            return;
        }
        executeCrawlProcess(date, sourceId, config);
    }

    private boolean loadFileControl(String pathFile) {
        connection = new DatabaseConnector(pathFile);
        return connection != null;
    }

    // 1.1.2 Kết nối với Database: control
    private boolean connectToDatabase() {
        conn = connection.getConnection();
        if (conn == null) {
            // 1.2.3 Thông báo "Kết nối database thất bại"
            System.err.println("Kết nối database thất bại");
            return false;
        }
        return true;
    }

    // Parse date input
    private LocalDate parseDateInput(String dateStr) {
        if (dateStr == null) {
            return LocalDate.now();
        }
        return LocalDate.parse(dateStr, INPUT_DATE_FORMAT);
    }

    // 1.1.3 Kiểm tra thông số date
    private boolean validateDate(LocalDate date) {
        LocalDate today = LocalDate.now();
        LocalTime now = LocalTime.now();
        // Nếu date > ngày hôm nay
        if (date.isAfter(today)) {
            // 1.3.4 Thông báo "ngày không hợp lệ"
            System.out.println("Ngày nhập không hợp lệ");
            return false;
        }
        // Nếu date là hôm nay và giờ < 19h
        if (date.isEqual(today) && now.isBefore(DRAW_TIME)) {
            // 1.2.4 Thông báo "Kết quả ngày hôm nay chưa được công bố"
            System.out.println("Kết quả ngày hôm nay chưa được công bố");
            return false;
        }
        return true;
    }

    // 1.1.4 Lấy config từ source_id
    private ConfigSource getConfigSource(int sourceId) {
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
                rs.close();
                return config;
            }
            rs.close();
            return null;
        } catch (SQLException e) {
            System.err.println("Lỗi khi lấy config_source: " + e.getMessage());
            return null;
        }
    }

    // Execute the main crawl process
    private void executeCrawlProcess(LocalDate date, int sourceId, ConfigSource config) {
        LocalDateTime startTime = LocalDateTime.now();

        // 1.1.5 Thêm thông tin process vào control.process_log với status là "RUNNING"
        insertProcessLog(sourceId, "RUNNING", startTime, LocalDateTime.now(), "P1", "Process đang được thực hiện");

        // 1.1.6 Khởi tạo ChromeDriver và truy cập đến trang crawl
        WebDriver driver = initializeWebDriver(config.sourceUrl, date.format(INPUT_DATE_FORMAT));
        if (driver == null) {
            updateProcessLog(sourceId, "FAIL", "Lỗi khởi tạo ChromeDriver");
            return;
        }

        try {
            // 1.1.7 Tìm bảng chứa kết quả xổ số
            WebElement mbTable = findLotteryTable(driver);
            if (mbTable == null) {
                // 1.2.7 Thông báo "Không tìm thấy kết quả"
                System.out.println("Không tìm thấy kết quả");
                // 1.2.8 Cập nhập status của process trong control.process_log thành "FAIL"
                updateProcessLog(sourceId, "FAIL", "Không tìm thấy kết quả kết quả trên web");
                return;
            }

            // 1.1.8 Crawl dữ liệu theo từng giải
            List<LotteryResult> results = crawlLotteryData(mbTable);

            // 1.1.9 Kiểm tra dữ liệu
            if (validateCrawledData(results)) {
                String filePath = buildFilePath(config.fileLocation, date);
                // 1.1.10 Tạo và lưu file csv
                exportToCsv(results, filePath);
                // 1.1.11 Cập nhập status của process trong control.process_log thành "SUCCESS"
                updateProcessLog(sourceId, "SUCCESS", "Thực hiện thành công");
            } else {
                // 1.2.10 Thông báo "Không tìm thấy dữ liệu"
                System.out.println("Không tìm thấy dữ liệu");
                // 1.2.11 Cập nhập status của process trong control.process_log thành "FAIL"
                updateProcessLog(sourceId, "FAIL", "Không tìm thấy dữ liệu");
            }
        } finally {
            // 1.1.12 Đóng ChromeDriver
            closeWebDriver(driver);
            // 1.1.13 Đóng kết nối với database control
            closeConnection();
        }
    }

    // 1.1.5 Thêm thông tin process vào control.process_log
    private void insertProcessLog(int sourceId, String status, LocalDateTime startTime,
                                  LocalDateTime endTime, String processCode, String message) {
        String sql = "INSERT INTO process_log (source_id, process_code, status, started_at, ended_at, message) " +
                "VALUES (?, ?, ?, ?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, sourceId);
            ps.setString(2, processCode);
            ps.setString(3, status);
            ps.setTimestamp(4, Timestamp.valueOf(startTime));
            ps.setTimestamp(5, Timestamp.valueOf(endTime));
            ps.setString(6, message);
            ps.executeUpdate();
            System.out.println("Thêm log thành công cho source_id: " + sourceId + ", status: " + status);
        } catch (SQLException e) {
            System.err.println("Lỗi khi thêm process_log: " + e.getMessage());
        }
    }

    // 1.1.6 Khởi tạo ChromeDriver và truy cập đến trang crawl
    private WebDriver initializeWebDriver(String urlTemplate, String date) {
        try {
            String url = String.format(urlTemplate, date);
            ChromeOptions options = new ChromeOptions();
            options.addArguments("--headless");
            options.addArguments("--no-sandbox");
            options.addArguments("--disable-dev-shm-usage");
            options.addArguments("--disable-gpu");
            options.addArguments("--window-size=1920,1080");
            options.addArguments("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36");

            WebDriver driver = new ChromeDriver(options);
            driver.manage().timeouts().implicitlyWait(Duration.ofSeconds(10));
            driver.get(url);
            return driver;
        } catch (Exception e) {
            System.err.println("Lỗi khi khởi tạo ChromeDriver: " + e.getMessage());
            return null;
        }
    }

    // 1.1.7 Tìm bảng chứa kết quả xổ số
    private WebElement findLotteryTable(WebDriver driver) {
        try {
            WebDriverWait wait = new WebDriverWait(driver, Duration.ofSeconds(15));
            wait.until(ExpectedConditions.presenceOfElementLocated(By.cssSelector("table.bkqtinhmienbac")));
            return driver.findElement(By.cssSelector("table.bkqtinhmienbac"));
        } catch (Exception e) {
            System.err.println("Lỗi khi tìm bảng kết quả: " + e.getMessage());
            return null;
        }
    }

    // 1.1.8 Crawl dữ liệu theo từng giải
    private List<LotteryResult> crawlLotteryData(WebElement mbTable) {
        List<LotteryResult> results = new ArrayList<>();

        try {
            // Extract date from table
            WebElement ngayElement = mbTable.findElement(By.cssSelector("td.ngay a"));
            String dateText = ngayElement.getText().trim().replace("/", "-");
            String createdAt = ZonedDateTime.now().format(CREATED_AT_FORMAT);

            // Define prize mappings
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

            // Extract prizes
            for (String[] prizeMapping : prizeMappings) {
                String prizeClass = prizeMapping[0];
                String prizeName = prizeMapping[1];

                List<WebElement> prizeRows = mbTable.findElements(By.cssSelector("td." + prizeClass));
                for (WebElement row : prizeRows) {
                    List<WebElement> numbers = row.findElements(By.cssSelector("div.giaiSo"));
                    for (WebElement numDiv : numbers) {
                        String numberValue = numDiv.getText().trim();
                        if (!numberValue.isEmpty()) {
                            results.add(new LotteryResult(dateText, prizeName, numberValue, createdAt));
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Lỗi khi crawl dữ liệu: " + e.getMessage());
        }

        return results;
    }

    // 1.1.9 Kiểm tra dữ liệu
    private boolean validateCrawledData(List<LotteryResult> results) {
        return results != null && !results.isEmpty();
    }

    // Build file path
    private String buildFilePath(String fileLocation, LocalDate date) {
        String fileName = String.format("data_%s.csv", date.format(FILE_DATE_FORMAT));
        return fileLocation + "\\" + fileName;
    }

    // 1.1.10 Tạo và lưu file csv
    private void exportToCsv(List<LotteryResult> results, String filePath) {
        try (FileWriter writer = new FileWriter(filePath, java.nio.charset.StandardCharsets.UTF_8)) {
            writer.write('\ufeff');
            writer.write("prize,number_value,full_date,created_at\n");
            for (LotteryResult result : results) {
                writer.write(result.toCsv() + "\n");
            }
        } catch (IOException e) {
            System.err.println("Lỗi khi xuất file CSV: " + e.getMessage());
        }
    }

    // 1.1.11 Cập nhập status của process trong control.process_log
    private void updateProcessLog(int sourceId, String status, String message) {
        String query = "UPDATE process_log SET ended_at = NOW(), status = ? , message =?  " +
                "WHERE source_id = ? AND DATE(started_at) = CURDATE()";
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, status);
            pstmt.setString(2, message);
            pstmt.setInt(3, sourceId);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Lỗi khi update process_log: " + e.getMessage());
        }
    }

    // 1.1.12 Đóng ChromeDriver
    private void closeWebDriver(WebDriver driver) {
        if (driver != null) {
            driver.quit();
        }
    }

    // 1.1.13 Đóng kết nối với database control
    private void closeConnection() {
        try {
            if (conn != null && !conn.isClosed()) {
                conn.close();
                System.out.println("Thực hiện crawl thành công");
            }
        } catch (SQLException e) {
            System.err.println("Lỗi khi đóng kết nối: " + e.getMessage());
        }
    }

    // Inner classes
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
            return String.format("%s,%s,%s,%s", prizeName, numberValue, date, createdAt);
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
}