import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class Util {
    // Lấy cấu hình từ bảng config_database
    public static Map<String, String> getConfigDatabase(Connection conn) throws SQLException {
        Map<String, String> config = new HashMap<>();

        String sql = "SELECT config_key, config_value FROM config_database";

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                config.put(rs.getString("config_key"), rs.getString("config_value"));
            }
        }

        return config;
    }

    // 5.1. Gọi procedure sp_prepare_warehouse_db để tạo database warehouse
    public static String createWarehouseDb(Connection connControl, int sourceId) throws SQLException {

        CallableStatement cs = connControl.prepareCall("{CALL sp_prepare_warehouse_db(?, ?)}");
        cs.setInt(1, sourceId);
        cs.registerOutParameter(2, Types.VARCHAR);

        cs.execute();

        String warehouseSchema = cs.getString(2);

        System.out.println("[P4] Đảm bảo schema warehouse tồn tại: " + warehouseSchema);

        return warehouseSchema;
    }

    // GỌI PROCEDURE: Tạo bảng dim_date, dim_number, fact_prize
    public static void createWarehouseTables(Connection connControl, int sourceId, String warehouseSchema)
            throws SQLException {

        CallableStatement cs = connControl.prepareCall("{CALL sp_create_warehouse_tables(?, ?)}");
        cs.setInt(1, sourceId);
        cs.setString(2, warehouseSchema);

        cs.execute();

        System.out.println("[P4] Tạo bảng dim/fact cho warehouse hoàn tất.");
    }
}
