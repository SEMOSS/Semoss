package prerna.engine.impl.neo4j;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.interpreters.CypherInterpreter;
import prerna.util.Constants;

import java.io.IOException;
import java.sql.*;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class Neo4jEngineUnitTests {

    private Neo4jEngine engine;

    @Mock
    private Connection conn;

    @Mock
    private PreparedStatement stmt;

    @Mock
    private ResultSet rs;

    @BeforeEach
    void setup() throws Exception {
        MockitoAnnotations.openMocks(this);
        engine = new Neo4jEngine();

        Properties props = new Properties();
        props.setProperty(Constants.ENGINE, "engine-01");
        props.setProperty(Constants.ENGINE_ALIAS, "ea");

        props.setProperty(Constants.CONNECTION_URL, "connUrl");
        props.setProperty(Constants.USERNAME, "un");
        props.setProperty(Constants.PASSWORD, "pw");

        String typeMapStr = "{}";
        props.setProperty(Constants.TYPE_MAP, typeMapStr);

        String nameMapStr = "{}";
        props.setProperty(Constants.NAME_MAP, nameMapStr);

        try(MockedStatic<DriverManager> driverManagerMockedStatic = Mockito.mockStatic(DriverManager.class)) {
            driverManagerMockedStatic.when(() -> DriverManager.getConnection("connUrl", "un", "pw")).thenReturn(conn);
            engine.setBasic(true);
            engine.open(props);
        }
    }

    @Test
    void testGetDatabaseType() {
        assertEquals(IDatabaseEngine.DATABASE_TYPE.NEO4J, engine.getDatabaseType());
    }

    @Test
    void testExecQuery() throws SQLException {
        when(conn.prepareStatement("query")).thenReturn(stmt);
        when(stmt.executeQuery()).thenReturn(rs);

        Map<String, Object> results = (Map<String, Object>) engine.execQuery("query");

        assertEquals(rs, results.get(RDBMSNativeEngine.RESULTSET_OBJECT));
        assertNull(results.get(RDBMSNativeEngine.ENGINE_CONNECTION_OBJECT));
        assertEquals(conn, results.get(RDBMSNativeEngine.CONNECTION_OBJECT));
        assertEquals(stmt, results.get(RDBMSNativeEngine.STATEMENT_OBJECT));

        verify(stmt, times(1)).close();
    }

    @Test
    void testExecQueryReturnsNullOnException() throws SQLException {
        when(conn.prepareStatement("query")).thenReturn(stmt);

        doThrow(SQLException.class).when(stmt).executeQuery();

        Object results = engine.execQuery("query");
        assertNull(results);
        verify(stmt, times(1)).close();
    }

    @Test
    void testGetQueryInterpreter() {
        CypherInterpreter ci = (CypherInterpreter) engine.getQueryInterpreter();
        assertNotNull(ci);
    }

    @Test
    void testInsertData() throws Exception {
        engine.insertData("data");
        verifyNoInteractions(conn);
    }

    @Test
    void testRemoveData() throws Exception {
        engine.removeData("data");
        verifyNoInteractions(conn);
    }

    @Test
    void testCommit() throws Exception {
        engine.commit();
        verifyNoInteractions(conn);
    }

    @Test
    void testGetEntityOfType() {
        assertNull(engine.getEntityOfType("test"));
    }

    @Test
    void testClose() throws IOException, SQLException {
        engine.close();
        verify(conn, times(1)).close();
    }

    @Test
    void testHoldsFileLocks() {
        assertFalse(engine.holdsFileLocks());
    }
}
