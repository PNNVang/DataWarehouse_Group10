import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;


import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.sql.*;
import java.util.*;

class XMLConfigReader {
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

            config.setHost(getTagContent(doc, "host"));
            config.setPort(getTagContent(doc, "port"));
            config.setDatabase(getTagContent(doc, "database"));
            config.setUsername(getTagContent(doc, "username"));
            config.setPassword(getTagContent(doc, "password"));

        } catch (Exception e) {
            System.err.println("Lỗi đọc file cấu hình: " + e.getMessage());
            e.printStackTrace();
        }
        return config;
    }

    private static String getTagContent(Document doc, String tagName) {
        NodeList nl = doc.getElementsByTagName(tagName);
        if (nl == null || nl.getLength() == 0) return null;
        return nl.item(0).getTextContent();
    }
}