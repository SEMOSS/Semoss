package prerna.nodemesh.db;

import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.util.Utility;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import org.postgresql.util.PGobject;


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
    
    /**
     * Create a trigger for a workflow.
     * @param workflowId the workflow.id (INT)
     * @param type e.g. "cron", "webhook", "event", "manual"
     * @param configJson JSON string for trigger config (stored as JSONB)
     * @return the newly created trigger id (INT) or -1 if not created
     */
    public static int createWorkflowTrigger(int workflowId, String type, String configJson) {
        final String sql = "INSERT INTO nodemesh_schema.workflow_trigger (workflow_id, type, config) " +
                           "VALUES (?, ?, ?)";

        // default to empty JSON if null
        if (configJson == null || configJson.isBlank()) {
            configJson = "{}";
        }

        try (PreparedStatement ps = nodeMeshDB.getConnection()
                .prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {

            ps.setInt(1, workflowId);
            ps.setString(2, type);
            ps.setObject(3, toJsonb(configJson)); // JSONB binding

            ps.executeUpdate();

            try (ResultSet rs = ps.getGeneratedKeys()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to create workflow trigger", e);
        }

        return -1;
    }

    /**
     * Fetch all triggers for a given workflow id.
     * Returns: id, workflow_id, type, config
     */
    public static List<Map<String, Object>> getWorkflowTriggers(int workflowId) {
        final String sql = "SELECT id, workflow_id, type, config " +
                           "FROM nodemesh_schema.workflow_trigger " +
                           "WHERE workflow_id = ? " +
                           "ORDER BY id ASC";

        List<Map<String, Object>> results = new java.util.ArrayList<>();

        try (PreparedStatement ps = nodeMeshDB.getConnection().prepareStatement(sql)) {
            ps.setInt(1, workflowId);

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
            throw new RuntimeException("Failed to fetch workflow triggers", e);
        }

        return results;
    }

    // --- helper to bind JSONB cleanly ---
    private static PGobject toJsonb(String json) throws Exception {
        PGobject obj = new PGobject();
        obj.setType("jsonb");
        obj.setValue(json);
        return obj;
    }

}
