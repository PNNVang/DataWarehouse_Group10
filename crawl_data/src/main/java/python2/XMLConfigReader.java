package python2;

import connection.DbConfig;
import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;

public class XMLConfigReader {

    public static DbConfig readConfig(String xmlFilePath) {
        DbConfig config = new DbConfig();
        try {
            File xmlFile = new File(xmlFilePath);
            if (!xmlFile.exists()) {
                return null;
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
            e.printStackTrace();
        }
        return config;
    }
}
