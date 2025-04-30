package prerna.engine.impl.rdbms;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.MockedConstruction;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.om.ThreadStore;
import prerna.util.sql.AbstractSqlQueryUtil;

public class MultiRDBMSNativeEngineUnitTests {

    private MultiRDBMSNativeEngine multiRDBMSNativeEngine;

    @Mock
    private RDBMSNativeEngine contextEngine;

    @Mock
    private RDBMSNativeEngine rdbmsNativeEngine;

    @Mock
    private User user;

    @Mock
    private AccessToken accessToken;

    @Mock
    private RDFFileSesameEngine baseDataEngine;

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        multiRDBMSNativeEngine = new MultiRDBMSNativeEngine();
        multiRDBMSNativeEngine.setBaseDataEngine(baseDataEngine);

        Properties smssProp = new Properties();
        smssProp.setProperty(MultiRDBMSNativeEngine.CONNECTIONS_TO_FILL, "1,2");
        smssProp.setProperty(MultiRDBMSNativeEngine.SETUP_QUERY_KEY, "SELECT context FROM contexts WHERE user_id = ?");
        smssProp.setProperty("1_url", "jdbc:h2:mem:test1");
        smssProp.setProperty("2_url", "jdbc:h2:mem:test2");
        smssProp.setProperty("DEFAULT_CONTEXT", "default");

        multiRDBMSNativeEngine.setSmssProp(smssProp);
        

        when(user.getAccessToken(null)).thenReturn(accessToken);
        when(accessToken.getId()).thenReturn("userId");


        ThreadStore.setUser(user);

    }

    @Test
    void testOpen(@TempDir Path tempDir) throws Exception {
        Properties smssProp = new Properties();
        smssProp.setProperty(MultiRDBMSNativeEngine.CONNECTIONS_TO_FILL, "1,2");
        smssProp.setProperty(MultiRDBMSNativeEngine.SETUP_QUERY_KEY, "SELECT context FROM contexts WHERE user_id = ?");
        smssProp.setProperty("1_url", "jdbc:h2:mem:test1");
        smssProp.setProperty("2_url", "jdbc:h2:mem:test2");
        smssProp.setProperty("DEFAULT_CONTEXT", "default");

        Set<Object> keySet = new HashSet<>();
        keySet.add("1_url");
        keySet.add("2_url");
        keySet.add("SETUP_query");

        ResultSet mockResultSet = mock(ResultSet.class);
        when(mockResultSet.next()).thenReturn(true).thenReturn(false);
        when(mockResultSet.getObject(1)).thenReturn("mockContext");

        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    
        try (MockedConstruction<RDBMSNativeEngine> engineMockedConstruction = mockConstruction(RDBMSNativeEngine.class, (mock, context) -> {
            when(mock.getPreparedStatement(anyString())).thenReturn(mockPreparedStatement);
            doNothing().when(mock).open(any(Properties.class));
        })) {
            multiRDBMSNativeEngine.open(smssProp);

            when(user.getAccessToken(null)).thenReturn(accessToken);
            when(accessToken.getId()).thenReturn("userId");

            RDBMSNativeEngine context = multiRDBMSNativeEngine.getContext();
            
        }
    }
    @Test
    void testHoldsFileLocks() {
        boolean result = multiRDBMSNativeEngine.holdsFileLocks();
        assertEquals(false, result);
    }      

    @Test
    void testGetDatabaseType() {
        assertEquals(IDatabaseEngine.DATABASE_TYPE.RDBMS, multiRDBMSNativeEngine.getDatabaseType());
    }


    @Test
    void testSetBaseDataEngine() {
        multiRDBMSNativeEngine.setBaseDataEngine(baseDataEngine);
        assertNotNull(multiRDBMSNativeEngine.getBaseDataEngine());
    }

    @Test
    void testGetQueryUtil() throws Exception {
        Properties smssProp = new Properties();
        smssProp.setProperty(MultiRDBMSNativeEngine.CONNECTIONS_TO_FILL, "1,2");
        smssProp.setProperty(MultiRDBMSNativeEngine.SETUP_QUERY_KEY, "SELECT context FROM contexts WHERE user_id = ?");
        smssProp.setProperty("1_url", "jdbc:h2:mem:test1");
        smssProp.setProperty("2_url", "jdbc:h2:mem:test2");
        smssProp.setProperty("DEFAULT_CONTEXT", "default");


        multiRDBMSNativeEngine.open(smssProp);
        AbstractSqlQueryUtil queryUtil = multiRDBMSNativeEngine.getQueryUtil();
        assertNotNull(queryUtil);
    }
}
