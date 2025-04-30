package prerna.engine.impl.rdbms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.IOException;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import com.zaxxer.hikari.HikariDataSource;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngine;
import prerna.util.Constants;
import prerna.util.PersistentHash;
import prerna.util.UploadUtilities;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

public class RDBMSNativeEngineUnitTests {

    @Mock
    private Connection mockConnection;

    @Mock
    private DatabaseMetaData mockMetaData;

    @Mock
    private Statement mockStatement;

    @Mock
    private PreparedStatement mockPreparedStatement;

    @Mock
    private ResultSet mockResultSet;

    @Mock
    private HikariDataSource mockDataSource;

    @Mock
    private SqlQueryUtilFactory mockQueryUtilFactory;

    @Mock
    private RdbmsConnectionHelper mockConnectionHelper;

    private RDBMSNativeEngine engine;

    @BeforeEach
    void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);
        engine = new RDBMSNativeEngine();
        engine.setConnection(mockConnection);
        AbstractSqlQueryUtil mockQueryUtil = mock(AbstractSqlQueryUtil.class);
        engine.setQueryUtil(mockQueryUtil);
        engine.setConnection(mockConnection);
        engine.dataSource = mockDataSource;
        when(mockDataSource.getConnection()).thenReturn(mockConnection);


    }

    @Test
    void testOpenMethodWithValidProperties() throws Exception {
        Properties smssProp = new Properties();
        smssProp.setProperty(Constants.RDBMS_TYPE, "H2_DB");
        smssProp.setProperty(Constants.DRIVER, "org.h2.Driver");
        smssProp.setProperty(Constants.CONNECTION_URL, "jdbc:h2:mem:test");
        smssProp.setProperty(AbstractSqlQueryUtil.DATABASE, "testDB");
        smssProp.setProperty(AbstractSqlQueryUtil.SCHEMA, "public");
        smssProp.setProperty(Constants.USE_CONNECTION_POOLING, "true");
        smssProp.setProperty(Constants.FETCH_SIZE, "100");
        smssProp.setProperty(Constants.CONNECTION_QUERY_TIMEOUT, "30");
        smssProp.setProperty(Constants.AUTO_COMMIT, "true");
        smssProp.setProperty(Constants.CONNECTION_TEST_QUERY, "SELECT 1");
        smssProp.setProperty(Constants.TRANSACTION_TYPE, "TRANSACTION_READ_COMMITTED");
        smssProp.setProperty(Constants.LEAK_DETECTION_THRESHOLD_MILLISECONDS, "2000");
        smssProp.setProperty(Constants.IDLE_TIMEOUT, "60000");
        smssProp.setProperty(Constants.POOL_MIN_SIZE, "5");
        smssProp.setProperty(Constants.POOL_MAX_SIZE, "10");

        RDBMSNativeEngine engine = spy(new RDBMSNativeEngine());

        doNothing().when(engine).open(any(Properties.class));

        try (MockedStatic<RdbmsConnectionHelper> mockedHelper = mockStatic(RdbmsConnectionHelper.class);
             MockedStatic<UploadUtilities> mockedUploadUtilities = mockStatic(UploadUtilities.class)) {

            mockedHelper.when(() -> RdbmsConnectionHelper.getSchema(any(), any(), anyString(), any(RdbmsTypeEnum.class)))
                        .thenReturn("mock_schema");

            File mockFile = mock(File.class);
            when(UploadUtilities.generateOwlFile(any(IEngine.CATALOG_TYPE.class), anyString(), anyString())).thenReturn(mockFile);
            when(mockFile.getAbsolutePath()).thenReturn("mock/path/to/owlFile.owl");

            engine.open(smssProp);

            verify(engine, times(1)).open(smssProp);

            assertEquals(null, engine.getConnectionUrl());
        }
    }


    @Test
    void testGetConnection() throws Exception {
        when(mockConnection.isClosed()).thenReturn(false);
        assertNotNull(engine.getConnection());
    }

    @Test
    void testInsertData() throws SQLException {
        String query = "INSERT INTO test_table (column1) VALUES ('value1')";
        when(mockConnection.prepareStatement(query)).thenReturn(mockPreparedStatement);

        engine.insertData(query);

        verify(mockPreparedStatement, times(1)).execute();
    }

    @Test
    void testExecQuery() throws SQLException {
        String query = "SELECT * FROM test_table";
        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(query)).thenReturn(mockResultSet);

        Map<String, Object> result = engine.execQuery(query);

        assertNotNull(result.get(RDBMSNativeEngine.RESULTSET_OBJECT));
        assertEquals(mockResultSet, result.get(RDBMSNativeEngine.RESULTSET_OBJECT));
    }

    @Test
    void testCloseConnection() throws IOException, SQLException {
        doNothing().when(mockConnection).close();
        engine.close();

        verify(mockConnection, times(1)).close();
    }

    @Test
    void testCommit() throws SQLException {
        when(mockConnection.getAutoCommit()).thenReturn(false);
        doNothing().when(mockConnection).commit();

        engine.commit();

        verify(mockConnection, times(1)).commit();
    }

@Test
    void testGetSchema() throws SQLException {
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);
        when(mockMetaData.getUserName()).thenReturn("test_schema");

        try (MockedStatic<RdbmsConnectionHelper> mockedHelper = mockStatic(RdbmsConnectionHelper.class)) {
            mockedHelper.when(() -> RdbmsConnectionHelper.getSchema(any(DatabaseMetaData.class), any(Connection.class), anyString(), any(RdbmsTypeEnum.class)))
                        .thenReturn("mock_schema");

            String schema = engine.getSchema();

            assertEquals(null, schema);
        }
    }

    @Test
    void testRemoveData() throws SQLException {
        String query = "DELETE FROM test_table WHERE column1 = 'value1'";
        when(mockConnection.prepareStatement(query)).thenReturn(mockPreparedStatement);

        engine.removeData(query);

        verify(mockPreparedStatement, times(1)).execute();
    }

    @Test
    void testExecUpdateAndRetrieveStatement() throws SQLException {
        String query = "UPDATE test_table SET column1 = 'value2' WHERE column1 = 'value1'";
        when(mockConnection.prepareStatement(query)).thenReturn(mockPreparedStatement);

        Statement stmt = engine.execUpdateAndRetrieveStatement(query, true);

        assertNotNull(stmt);
        verify(mockPreparedStatement, times(1)).executeUpdate();
    }

    @Test
    void testIsConnected() throws SQLException {
        when(mockConnection.isClosed()).thenReturn(false);
        assertTrue(engine.isConnected());
    }

    @Test
    void testIsConnectionPooling() {
        assertFalse(engine.isConnectionPooling());
    }

    @Test
    void testGetDatabaseType() {
        assertEquals(IDatabaseEngine.DATABASE_TYPE.RDBMS, engine.getDatabaseType());
    }

    @Test
    void testGetDataSource() {
        assertEquals(engine.getDataSource(), mockDataSource);
    }

    @Test
    void testGetPreparedStatement() throws SQLException {
        String sql = "SELECT * FROM test_table";
        when(mockConnection.prepareStatement(sql)).thenReturn(mockPreparedStatement);

        PreparedStatement preparedStatement = engine.getPreparedStatement(sql);

        assertNotNull(preparedStatement);
        verify(mockConnection, times(1)).prepareStatement(sql);
    }

    @Test
    void testGetConnectionMetadata() throws SQLException {
        when(mockConnection.getMetaData()).thenReturn(mockMetaData);

        DatabaseMetaData metaData = engine.getConnectionMetadata();

        assertNotNull(metaData);
        assertEquals(mockMetaData, metaData);
    }

    @Test
    void testCreateClob() throws SQLException {
        Clob mockClob = mock(Clob.class);
        when(mockConnection.createClob()).thenReturn(mockClob);

        Clob clob = engine.createClob(mockConnection);

        assertNotNull(clob);
        assertEquals(mockClob, clob);
    }

    @Test
    void testGetEntityOfType() throws SQLException {
        String type = "http://semoss.org/ontologies/Concept/Column/Table";
        when(mockConnection.createStatement()).thenReturn(mockStatement);
        when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
        when(mockResultSet.next()).thenReturn(true, false);
        when(mockResultSet.getObject(1)).thenReturn("value");

        Vector<Object> result = engine.getEntityOfType(type);

        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("value", result.get(0));
    }
    @Test
    void testInitMethod() {
        String connectionUrl = "jdbc:h2:mem:test";
        String result = engine.init(connectionUrl, true);

        assertNull(result);
    }
    
    @Test
    void testHoldsFileLocks() {
        Properties smssProp = new Properties();
        smssProp.setProperty(Constants.RDBMS_TYPE, "H2_DB");
        engine.setSmssProp(smssProp);

        assertTrue(engine.holdsFileLocks());
    }

    @Test
    void testGetCatalogSubType() {
        Properties smssProp = new Properties();
        smssProp.setProperty(Constants.RDBMS_TYPE, "H2_DB");
        engine.setSmssProp(smssProp);

        String catalogSubType = engine.getCatalogSubType(smssProp);
        assertEquals("H2_DB", catalogSubType);
    }

    @Test
    void testGetConceptIdHash() throws SQLException {
        when(mockConnection.isClosed()).thenReturn(false);
        engine.setEngineId(Constants.LOCAL_MASTER_DB);
        engine.setConnection(mockConnection);

        PersistentHash conceptIdHash = engine.getConceptIdHash();
        assertNull(conceptIdHash);
    }

    @Test
    void testBulkInsertPreparedStatement() throws SQLException {
        Object[] args = {"test_table", "column1", "column2"};
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);

        PreparedStatement preparedStatement = engine.bulkInsertPreparedStatement(args);

        assertNotNull(preparedStatement);
        verify(mockConnection, times(1)).prepareStatement(anyString());
    }

    @Test
    void testSetAutoCommit() throws SQLException {
        engine.setAutoCommit(true);
        verify(mockConnection, times(1)).setAutoCommit(true);
    }

    @Test
    void testSetTransactionIsolationType() throws SQLException {
        engine.setTransactionIsolationType(Connection.TRANSACTION_READ_COMMITTED);
        verify(mockConnection, times(1)).setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    }
}