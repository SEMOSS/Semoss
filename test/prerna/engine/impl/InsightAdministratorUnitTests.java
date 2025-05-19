package prerna.engine.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.util.sql.AbstractSqlQueryUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;
import static prerna.engine.impl.InsightAdministrator.*;

public class InsightAdministratorUnitTests {

    private InsightAdministrator administrator;

    @Mock
    private AbstractSqlQueryUtil queryUtil;

    @Mock
    private RDBMSNativeEngine nativeEngine;

    @Mock
    private PreparedStatement ps;

    @Mock
    private Connection conn;

    @Mock
    private java.sql.Array sqlArray;

    @BeforeEach
    public void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);
        when(nativeEngine.getQueryUtil()).thenReturn(queryUtil);
        when(queryUtil.allowArrayDatatype()).thenReturn(true);
        when(queryUtil.allowClobJavaObject()).thenReturn(true);

        when(nativeEngine.bulkInsertPreparedStatement(new String[] {
                TABLE_NAME, QUESTION_ID_COL, QUESTION_NAME_COL, QUESTION_LAYOUT_COL,
                HIDDEN_INSIGHT_COL, CACHEABLE_COL, CACHE_MINUTES_COL,
                CACHE_CRON_COL, CACHED_ON_COL, CACHE_ENCRYPT_COL, QUESTION_PKQL_COL,
                SCHEMA_NAME_COL
        })).thenReturn(ps);

        when(ps.getConnection()).thenReturn(conn);
        when(conn.getAutoCommit()).thenReturn(false);

        when(conn.createArrayOf(eq("VARCHAR"), any(String[].class))).thenReturn(sqlArray);
        administrator = new InsightAdministrator(nativeEngine);
    }

    @Test
    void addInsight() throws SQLException {
        String insightId = "testId";
        String insightName = "InsightName";
        String layout = "layout";
        Collection<String> pixelRecipe = new ArrayList<>();
        pixelRecipe.add("pixelRecipe-1");
        pixelRecipe.add("pixelRecipe-2");
        boolean global = true;
        boolean cacheable = true;
        int cacheMinutes = 5;
        String cacheCron = "cacheCron";
        ZonedDateTime cachedOn = ZonedDateTime.of(2020, 5, 5, 5, 5, 5, 0, ZoneId.of("UTC"));
        boolean cacheEncrypt = false;
        String schemaName = "schemaName";

        String val = administrator.addInsight(insightId, insightName, layout, pixelRecipe, global, cacheable, cacheMinutes,
                cacheCron, cachedOn, cacheEncrypt, schemaName);

        verify(ps, times(1)).setString(1, "testId");
        verify(ps, times(1)).setString(2, "InsightName");
        verify(ps, times(1)).setString(3, "layout");
        verify(ps, times(1)).setBoolean(4, false);
        verify(ps, times(1)).setBoolean(5, true);
        verify(ps, times(1)).setInt(6, 5);
        verify(ps, times(1)).setString(7, "cacheCron");

        verify(ps, times(1)).setTimestamp(8, java.sql.Timestamp.valueOf(cachedOn.toLocalDateTime()));
        verify(ps, times(1)).setBoolean(9, false);
        verify(ps, times(1)).setArray(10, sqlArray);

        verify(ps, times(1)).setString(11, "schemaName");
        verify(ps, times(1)).execute();
        verify(conn, times(1)).commit();

        assertEquals("testId", val);
    }


    @Test
    void batchInsight() throws SQLException {
        String insightId = "testId";
        String insightName = "InsightName";
        String layout = "layout";
        List<String> pixelRecipe = new ArrayList<>();
        pixelRecipe.add("pixelRecipe-1");
        pixelRecipe.add("pixelRecipe-2");
        boolean global = true;
        boolean cacheable = true;
        int cacheMinutes = 5;
        String cacheCron = "cacheCron";
        LocalDateTime cachedOn = LocalDateTime.of(2020, 5, 5, 5, 5, 5, 0);
        boolean cacheEncrypt = false;
        String schemaName = "schemaName";

        String val = administrator.batchInsight(ps, insightId, insightName, layout, pixelRecipe, global, cacheable, cacheMinutes,
                cacheCron, cachedOn, cacheEncrypt, schemaName);

        verify(ps, times(1)).setString(1, "testId");
        verify(ps, times(1)).setString(2, "InsightName");
        verify(ps, times(1)).setString(3, "layout");
        verify(ps, times(1)).setBoolean(4, false);
        verify(ps, times(1)).setBoolean(5, true);
        verify(ps, times(1)).setInt(6, 5);
        verify(ps, times(1)).setString(7, "cacheCron");

        verify(ps, times(1)).setTimestamp(8, java.sql.Timestamp.valueOf(cachedOn));
        verify(ps, times(1)).setBoolean(9, false);
        verify(ps, times(1)).setArray(10, sqlArray);

        verify(ps, times(1)).setString(11, "schemaName");
        verify(ps, times(1)).addBatch();

        assertEquals("testId", val);
    }

    @Test
    void updateInsightTags() throws SQLException {
        String insightId = "testId";
        List<String> tags = new ArrayList<>();
        tags.add("tag1");
        tags.add("tag2");

        when(queryUtil.createInsertPreparedStatementString("INSIGHTMETA",
                new String[]{"INSIGHTID", "METAKEY", "METAVALUE", "METAORDER"}))
                .thenReturn("updateQuery");

        when(nativeEngine.getPreparedStatement("updateQuery")).thenReturn(ps);

        administrator.updateInsightTags(insightId, tags);

        verify(nativeEngine, times(1)).insertData("DELETE FROM INSIGHTMETA WHERE METAKEY='tag' AND INSIGHTID='"
                + insightId + "'");
        verify(nativeEngine).commit();

        verify(ps, times(2)).setString(1, "testId");
        verify(ps, times(2)).setString(2, "tag");
        verify(ps, times(1)).setString(3, "tag1");
        verify(ps, times(1)).setString(3, "tag2");
        verify(ps, times(1)).setInt(4, 0);
        verify(ps, times(1)).setInt(4, 1);
        verify(ps, times(2)).addBatch();
        verify(ps, times(1)).executeBatch();
    }

    @Test
    void updateInsightTagsArray() throws SQLException {
        String insightId = "testId";
        String[] tags = new String[]{"tag1", "tag2"};

        when(queryUtil.createInsertPreparedStatementString("INSIGHTMETA",
                new String[]{"INSIGHTID", "METAKEY", "METAVALUE", "METAORDER"}))
                .thenReturn("updateQuery");

        when(nativeEngine.getPreparedStatement("updateQuery")).thenReturn(ps);

        administrator.updateInsightTags(insightId, tags);

        verify(nativeEngine, times(1)).insertData("DELETE FROM INSIGHTMETA WHERE METAKEY='tag' AND INSIGHTID='"
                + insightId + "'");
        verify(nativeEngine).commit();

        verify(ps, times(2)).setString(1, "testId");
        verify(ps, times(2)).setString(2, "tag");
        verify(ps, times(1)).setString(3, "tag1");
        verify(ps, times(1)).setString(3, "tag2");
        verify(ps, times(1)).setInt(4, 0);
        verify(ps, times(1)).setInt(4, 1);
        verify(ps, times(2)).addBatch();
        verify(ps, times(1)).executeBatch();
    }

    @Test
    void updateInsightDescription() throws SQLException {
        String insightId = "testId";
        String description = "testDescription";

        when(nativeEngine.getPreparedStatement("UPDATE INSIGHTMETA SET METAVALUE=? WHERE METAKEY=? AND INSIGHTID=?")).thenReturn(
                ps
        );


        when(ps.getUpdateCount()).thenReturn(0);

        PreparedStatement ps2 = mock(PreparedStatement.class);
        when(nativeEngine.getPreparedStatement("INSERT INTO INSIGHTMETA(INSIGHTID, METAKEY, METAVALUE, METAORDER) VALUES(?,?,?,?)"))
                .thenReturn(ps2);
        Connection conn2 = mock(Connection.class);
        when(ps2.getConnection()).thenReturn(conn2);

        administrator.updateInsightDescription(insightId, description);
        verify(ps, times(1)).setString(1, "testDescription");
        verify(ps, times(1)).setString(2, "description");
        verify(ps, times(1)).setString(3, "testId");
        verify(ps, times(1)).execute();
        verify(conn, times(1)).commit();

        verify(ps2, times(1)).setString(1, "testId");
        verify(ps2, times(1)).setString(2, "description");
        verify(ps2, times(1)).setString(3, "testDescription");
        verify(ps2, times(1)).setInt(4, 0);
        verify(ps2, times(1)).execute();

        verify(conn2, times(1)).commit();

    }

    @Test
    void updateInsight() throws SQLException {
        String insightId = "testId";
        String insightName = "InsightName";
        String layout = "layout";
        Collection<String> pixelRecipe = new ArrayList<>();
        pixelRecipe.add("pixelRecipe-1");
        pixelRecipe.add("pixelRecipe-2");
        boolean global = true;
        boolean cacheable = true;
        int cacheMinutes = 5;
        String cacheCron = "cacheCron";
        ZonedDateTime cachedOn = ZonedDateTime.of(2020, 5, 5, 5, 5, 5, 0, ZoneId.of("UTC"));
        boolean cacheEncrypt = false;
        String schemaName = "schemaName";


        String query = "UPDATE QUESTION_ID SET QUESTION_NAME=?, QUESTION_LAYOUT=?, HIDDEN_INSIGHT=?, CACHEABLE=?, " +
                "CACHE_MINUTES=?, CACHE_CRON=?, CACHED_ON=?, CACHE_ENCRYPT=?, QUESTION_PKQL=?, SCHEMA_NAME=? WHERE ID=?";
        when(nativeEngine.getPreparedStatement(query)).thenReturn(ps);

        administrator.updateInsight(insightId, insightName, layout, pixelRecipe, global, cacheable, cacheMinutes,
                cacheCron, cachedOn, cacheEncrypt, schemaName);



        verify(ps, times(1)).setString(1, "InsightName");
        verify(ps, times(1)).setString(2, "layout");
        verify(ps, times(1)).setBoolean(3, false);
        verify(ps, times(1)).setBoolean(4, true);
        verify(ps, times(1)).setInt(5, 5);
        verify(ps, times(1)).setString(6, "cacheCron");

        verify(ps, times(1)).setTimestamp(7, java.sql.Timestamp.valueOf(cachedOn.toLocalDateTime()));
        verify(ps, times(1)).setBoolean(8, false);
        verify(ps, times(1)).setArray(9, sqlArray);

        verify(ps, times(1)).setString(10, "schemaName");
        verify(ps, times(1)).setString(11, "testId");
        verify(ps, times(1)).execute();
        verify(conn, times(1)).commit();
        verify(ps, times(1)).close();

    }

    @Test
    void updateInsightName() throws SQLException {
        String insightId = "testId";
        String insightName = "InsightName";


        String query = "UPDATE QUESTION_ID SET QUESTION_NAME=? WHERE ID=?";
        when(nativeEngine.getPreparedStatement(query)).thenReturn(ps);

        administrator.updateInsightName(insightId, insightName);

        verify(ps, times(1)).setString(1, "InsightName");
        verify(ps, times(1)).setString(2, "testId");
        verify(ps, times(1)).execute();
        verify(conn, times(1)).commit();
        verify(ps, times(1)).close();

    }

    @Test
    void testUpdateInsightCache() throws SQLException {
        String insightId = "testId";
        boolean cacheable = true;
        int cacheMinutes = 5;
        String cacheCron = "cacheCron";
        LocalDateTime cachedOn = LocalDateTime.of(2020, 5, 5, 5, 5, 5, 0);
        boolean cacheEncrypt = false;


        String query = "UPDATE QUESTION_ID SET CACHEABLE=?, CACHE_MINUTES=?, CACHE_CRON=?, CACHED_ON=?, CACHE_ENCRYPT=? " +
                "WHERE ID=?";
        when(nativeEngine.getPreparedStatement(query)).thenReturn(ps);

        administrator.updateInsightCache(insightId, cacheable, cacheMinutes,
                cacheCron, cachedOn, cacheEncrypt);

        verify(ps, times(1)).setBoolean(1, true);
        verify(ps, times(1)).setInt(2, 5);
        verify(ps, times(1)).setString(3, "cacheCron");
        verify(ps, times(1)).setTimestamp(4, java.sql.Timestamp.valueOf(cachedOn));
        verify(ps, times(1)).setBoolean(5, false);
        verify(ps, times(1)).setString(6, "testId");
        verify(ps, times(1)).execute();
        verify(conn, times(1)).commit();
        verify(ps, times(1)).close();
    }

    @Test
    void testUpdateInsightCachedOn() throws SQLException {
        String insightId = "testId";
        ZonedDateTime cachedOn = ZonedDateTime.of(2020, 5, 5, 5, 5, 5, 0, ZoneId.of("UTC"));

        String query = "UPDATE QUESTION_ID SET CACHED_ON=? WHERE ID=?";
        when(nativeEngine.getPreparedStatement(query)).thenReturn(ps);

        administrator.updateInsightCachedOn(insightId, cachedOn);

        verify(ps, times(1)).setTimestamp(1, java.sql.Timestamp.valueOf(cachedOn.toLocalDateTime()));
        verify(ps, times(1)).setString(2, "testId");
        verify(ps, times(1)).execute();
        verify(conn, times(1)).commit();
        verify(ps, times(1)).close();
    }

    @Test
    void testDropInsight() throws SQLException {
        String insightId = "testId";
        String insightId2 = "testId2";

        when(nativeEngine.getPreparedStatement("DELETE FROM QUESTION_ID WHERE ID=?"))
                .thenReturn(ps);

        PreparedStatement ps2 = mock(PreparedStatement.class);
        when(nativeEngine.getPreparedStatement("DELETE FROM INSIGHTMETA WHERE INSIGHTID=?"))
                .thenReturn(ps2);
        Connection conn2 = mock(Connection.class);
        when(ps2.getConnection()).thenReturn(conn2);

        administrator.dropInsight(insightId, insightId2);

        verify(ps, times(1)).setString(1, "testId");
        verify(ps, times(1)).setString(1, "testId2");

        verify(ps, times(2)).addBatch();
        verify(ps, times(1)).executeBatch();
        verify(conn, times(1)).commit();
        verify(ps, times(1)).close();

        verify(ps2, times(1)).setString(1, "testId");
        verify(ps2, times(1)).setString(1, "testId2");

        verify(ps2, times(2)).addBatch();
        verify(ps2, times(1)).executeBatch();
        verify(conn2, times(1)).commit();
        verify(ps2, times(1)).close();

    }

    @Test
    void testDropInsightCollection() throws Exception {
        String insightId = "testId";
        String insightId2 = "testId2";
        List<String> collection = new ArrayList<>();
        collection.add(insightId);
        collection.add(insightId2);

        when(nativeEngine.getPreparedStatement("DELETE FROM QUESTION_ID WHERE ID=?"))
                .thenReturn(ps);


        administrator.dropInsight(collection);

        verify(ps, times(1)).setString(1, "testId");
        verify(ps, times(1)).setString(1, "testId2");

        verify(ps, times(2)).addBatch();
        verify(ps, times(1)).executeBatch();
        verify(conn, times(1)).commit();
        verify(ps, times(1)).close();
    }

    @Test
    void testGetAddInsightMetaPreparedStatement() throws SQLException {
        when(nativeEngine.bulkInsertPreparedStatement(new String[] {"INSIGHTMETA", "INSIGHTID", "METAKEY", "METAVALUE", "METAORDER"}))
                .thenReturn(ps);
        PreparedStatement ps2 = administrator.getAddInsightMetaPreparedStatement();
        assertEquals(ps2, ps);
    }

    @Test
    void testGetAddInsightMetaPreparedStatementException() throws SQLException {
        when(nativeEngine.bulkInsertPreparedStatement(new String[] {"INSIGHTMETA", "INSIGHTID", "METAKEY", "METAVALUE", "METAORDER"}))
                .thenThrow(new SQLException("FOO BAR"));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> administrator.getAddInsightMetaPreparedStatement());
        assertEquals("Error occurred generating the prepared statement to insert the insight metadata", e.getMessage());
    }

    @Test
    void testGetClobRecipSyntax() {
        String[] a = {"test1", "test2"};

        String ret = InsightAdministrator.getClobRecipeSyntax(a);
        assertEquals("[\"test1\",\"test2\"]", ret);
    }

}
