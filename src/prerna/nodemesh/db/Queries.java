package prerna.nodemesh.db;

import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.util.QueryExecutionUtility;
import prerna.util.Utility;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;


public class Queries {

    private static final String dbId = System.getenv("DB_ID");

    static IRDBMSEngine nodeMeshDB = (RDBMSNativeEngine) Utility.getDatabase("c62602ea-0194-4d4b-9902-9f23bcc5b7cd");

    public static List<Map<String, Object>> getUserWorkflows(String userId) {
        String sql = "SELECT id, name, description, created_by, created_at, updated_at " +
                "FROM nodemesh_schema.workflow " +
                "WHERE created_by = ?";

        List<Map<String, Object>> results = new java.util.ArrayList<>();

        try (PreparedStatement ps = nodeMeshDB.getConnection().prepareStatement(sql)) {
            ps.setString(1, userId);
            try (ResultSet rs = ps.executeQuery()) {
                java.sql.ResultSetMetaData meta = rs.getMetaData();
                int columnCount = meta.getColumnCount();

                while (rs.next()) {
                    Map<String, Object> row = new java.util.HashMap<>();
                    for (int i = 1; i <= columnCount; i++) {
                        String colName = meta.getColumnLabel(i);
                        row.put(colName, rs.getObject(i));
                    }
                    results.add(row);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to fetch user workflows", e);
        }

        return results;
    }


    public static int createWorkflow(String name, String description, String createdBy) {
        String sql = "INSERT INTO nodemesh_schema.workflow (name, description, created_by) VALUES (?, ?, ?)";

        try (PreparedStatement ps = nodeMeshDB.getConnection().prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, name);
            ps.setString(2, description);
            ps.setString(3, createdBy);

            ps.executeUpdate();

            try (ResultSet rs = ps.getGeneratedKeys()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to create workflow", e);
        }

        return -1;
    }

}
