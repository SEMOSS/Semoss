package prerna.engine.impl.rdbms;

import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.zaxxer.hikari.HikariDataSource;

import prerna.util.sql.RdbmsTypeEnum;

public class RdbmsConnectionHelperUnitTests {

    @Mock
    private DatabaseMetaData meta;

    @Mock
    private Connection con;

    @Mock
    private Statement stmt;

    @Mock
    private ResultSet resultSet;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testGetDataSourceFromPool() throws SQLException {
        String driver = "org.h2.Driver";
        String connectURI = "jdbc:h2:mem:test";
        String userName = "sa";
        String password = "password";

        HikariDataSource dataSource = RdbmsConnectionHelper.getDataSourceFromPool(driver, connectURI, userName, password);

        assertNotNull(dataSource);
        assertEquals(driver, dataSource.getDriverClassName());
        assertEquals(connectURI, dataSource.getJdbcUrl());
        assertEquals(userName, dataSource.getUsername());
        assertEquals(password, dataSource.getPassword());
    }

    @Test
    void testGetSchemaForOracle() throws SQLException {
        when(meta.getDriverName()).thenReturn("Oracle JDBC Driver");
        when(meta.getUserName()).thenReturn("TEST_SCHEMA");

        String schema = RdbmsConnectionHelper.getSchema(meta, con, "jdbc:oracle:thin:@localhost:1521:orcl", RdbmsTypeEnum.ORACLE);

        assertEquals("TEST_SCHEMA", schema);
    }

    @Test
    void testGetSchemaForOtherRdbms() throws SQLException {
        when(meta.getDriverName()).thenReturn("H2 JDBC Driver");
        when(meta.getJDBCMajorVersion()).thenReturn(7);
        when(con.getSchema()).thenReturn("TEST_SCHEMA");

        String schema = RdbmsConnectionHelper.getSchema(meta, con, "jdbc:h2:mem:test?currentSchema=TEST_SCHEMA", RdbmsTypeEnum.H2_DB);

        assertNotNull(schema); // Ensure schema is not null
        assertEquals("TEST_SCHEMA", schema);
    }

    @Test
    void testGetSchemaWithUrlSchema() throws SQLException {
        String connectionUrl = "jdbc:h2:mem:test?currentSchema=TEST_SCHEMA";
        String schema = RdbmsConnectionHelper.getSchema(meta, con, connectionUrl, RdbmsTypeEnum.H2_DB);

        assertEquals("TEST_SCHEMA", schema);
    }

    @Test
    void testGetSchemaWithUrlDatabase() throws SQLException {
        String connectionUrl = "jdbc:teradata://localhost/DATABASE=TEST_DB";
        String schema = RdbmsConnectionHelper.getSchema(meta, con, connectionUrl, RdbmsTypeEnum.TERADATA);

        assertEquals("TEST_DB", schema);
    }

    @Test
    void testGetTablesForOracle() throws SQLException {
        when(meta.getUserName()).thenReturn("TEST_SCHEMA");
        when(stmt.executeQuery(anyString())).thenReturn(resultSet);

        ResultSet tables = RdbmsConnectionHelper.getTables(con, stmt, meta, null, null, RdbmsTypeEnum.ORACLE);

        assertNotNull(tables);
    }

    @Test
    void testGetTablesForOtherRdbms() throws SQLException {
        when(meta.getTables(anyString(), anyString(), anyString(), any(String[].class))).thenReturn(resultSet);

        ResultSet tables = RdbmsConnectionHelper.getTables(con, stmt, meta, "catalog", "schema", RdbmsTypeEnum.H2_DB);

        assertNull(tables);
    }

    @Test
    void testGetColumnsForOracle() throws SQLException {
    when(meta.getColumns(anyString(), anyString(), anyString(), anyString())).thenReturn(resultSet);

    ResultSet columns = RdbmsConnectionHelper.getColumns(meta, "table", "catalog", "schema", RdbmsTypeEnum.ORACLE);

    assertNull(columns); 
}

    @Test
    void testGetColumnsForOtherRdbms() throws SQLException {
        when(meta.getColumns(anyString(), anyString(), anyString(), anyString())).thenReturn(resultSet);

        ResultSet columns = RdbmsConnectionHelper.getColumns(meta, "table", "catalog", "schema", RdbmsTypeEnum.H2_DB);

        assertNull(columns);
    }

    @Test
    void testGetTableKeysForSnowflake() {
        String[] keys = RdbmsConnectionHelper.getTableKeys(RdbmsTypeEnum.SNOWFLAKE);

        assertEquals("TABLE_NAME", keys[0]);
        assertEquals("TABLE_TYPE", keys[1]);
        assertEquals("TABLE_SCHEM", keys[2]);
        assertEquals("TABLE_CAT", keys[3]);
    }

    @Test
    void testGetTableKeysForOtherRdbms() {
        String[] keys = RdbmsConnectionHelper.getTableKeys(RdbmsTypeEnum.H2_DB);

        assertEquals("table_name", keys[0]);
        assertEquals("table_type", keys[1]);
        assertEquals("table_schem", keys[2]);
        assertEquals("table_cat", keys[3]);
    }

    @Test
    void testGetColumnKeysForSnowflake() {
        String[] keys = RdbmsConnectionHelper.getColumnKeys(RdbmsTypeEnum.SNOWFLAKE);

        assertEquals("COLUMN_NAME", keys[0]);
        assertEquals("TYPE_NAME", keys[1]);
    }

    @Test
    void testGetColumnKeysForOtherRdbms() {
        String[] keys = RdbmsConnectionHelper.getColumnKeys(RdbmsTypeEnum.H2_DB);

        assertEquals("column_name", keys[0]);
        assertEquals("type_name", keys[1]);
    }
    @Test
    void testPredictSchemaFromUrlWithSemicolonCurrentSchema() {
        String connectionUrl = "jdbc:h2:mem:test;currentSchema=SEMICOLON_SCHEMA";
        String schema = RdbmsConnectionHelper.getSchema(meta, con, connectionUrl, RdbmsTypeEnum.H2_DB);

        assertEquals("SEMICOLON_SCHEMA", schema);
    }

    @Test
    void testPredictSchemaFromUrlWithAmpersandCurrentSchema() {
        String connectionUrl = "jdbc:h2:mem:test&currentSchema=AMPERSAND_SCHEMA";
        String schema = RdbmsConnectionHelper.getSchema(meta, con, connectionUrl, RdbmsTypeEnum.H2_DB);

        assertEquals("AMPERSAND_SCHEMA", schema);
    }

    @Test
    void testPredictSchemaFromUrlWithQuestionMarkSchema() {
        String connectionUrl = "jdbc:h2:mem:test?schema=QUESTION_SCHEMA";
        String schema = RdbmsConnectionHelper.getSchema(meta, con, connectionUrl, RdbmsTypeEnum.H2_DB);

        assertEquals("QUESTION_SCHEMA", schema);
    }

    @Test
    void testPredictSchemaFromUrlWithSemicolonSchema() {
        String connectionUrl = "jdbc:h2:mem:test;schema=SEMICOLON_SCHEMA";
        String schema = RdbmsConnectionHelper.getSchema(meta, con, connectionUrl, RdbmsTypeEnum.H2_DB);

        assertEquals("SEMICOLON_SCHEMA", schema);
    }

    @Test
    void testPredictSchemaFromUrlWithAmpersandSchema() {
        String connectionUrl = "jdbc:h2:mem:test&schema=AMPERSAND_SCHEMA";
        String schema = RdbmsConnectionHelper.getSchema(meta, con, connectionUrl, RdbmsTypeEnum.H2_DB);

        assertEquals("AMPERSAND_SCHEMA", schema);
    }
    @Test
    void testGetColumnsForSnowflakeWithUnderscoreReplacement() throws SQLException {
        when(meta.getColumns(anyString(), anyString(), anyString(), anyString())).thenReturn(resultSet);

        String schemaFilter = "schema_with_underscore";
        String tableOrView = "table_with_underscore";

        ResultSet columns = RdbmsConnectionHelper.getColumns(meta, tableOrView, "catalog", schemaFilter, RdbmsTypeEnum.SNOWFLAKE);

        assertNull(columns);
        verify(meta).getColumns("catalog", "schema\\_with\\_underscore", "table\\_with\\_underscore", null);
    }

    @Test
    void testGetColumnsForCassandraWithEmptyCatalog() throws SQLException {
        when(meta.getColumns(anyString(), anyString(), anyString(), anyString())).thenReturn(resultSet);

        ResultSet columns = RdbmsConnectionHelper.getColumns(meta, "table", "", "schema", RdbmsTypeEnum.CASSANDRA);

        assertNull(columns);
    }
    @Test
    void testPredictSchemaFromTruncatedUrlWithSchemas() throws SQLException {
        when(meta.getSchemas()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true).thenReturn(false);
        when(resultSet.getString(1)).thenReturn("TRUNCATED_SCHEMA");

        String connectionUrl = "jdbc:h2:mem:test/TRUNCATED_SCHEMA";
        String schema = RdbmsConnectionHelper.getSchema(meta, con, connectionUrl, RdbmsTypeEnum.H2_DB);

        assertEquals("TRUNCATED_SCHEMA", schema);
    }

    
}