package vn.edu.hcmuaf.fit;

import org.w3c.dom.Document;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;

public class XMLConfigReader {

    // 1. Đọc cấu hình kết nối database từ file control.xml
    public static DbConfig readConfig(String xmlFilePath) {
        DbConfig config = new DbConfig();
        try {
            File xmlFile = new File(xmlFilePath);
            if (!xmlFile.exists()) {
                throw new RuntimeException("Không tìm thấy file cấu hình: " + xmlFilePath);
            }

            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(xmlFile);
            doc.getDocumentElement().normalize();

            config.setHost(doc.getElementsByTagName("host").item(0).getTextContent());
            config.setPort(doc.getElementsByTagName("port").item(0).getTextContent());
            config.setDatabase(doc.getElementsByTagName("database").item(0).getTextContent());
            config.setUsername(doc.getElementsByTagName("username").item(0).getTextContent());
            config.setPassword(doc.getElementsByTagName("password").item(0).getTextContent());

        } catch (Exception e) {
            System.err.println("❌ Lỗi đọc file cấu hình: " + e.getMessage());
        }
        return config;
    }
}
