package prerna.engine.impl.rdbms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;

import org.apache.commons.io.FilenameUtils;
import org.h2.tools.Server;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockitoAnnotations;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.util.Constants;
import prerna.util.PortAllocator;
import prerna.util.Utility;

public class H2EmbeddedServerEngineUnitTests {

    private H2EmbeddedServerEngine h2EmbeddedServerEngine;

    @Mock
    private Server server;

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        h2EmbeddedServerEngine = new H2EmbeddedServerEngine();

    }

    @Test
    void testInit(@TempDir Path tempDir) throws IOException, SQLException {
        Path dbFile = tempDir.resolve("test_database.mv.db");
        String connectionUrl = "jdbc:h2:nio:" + dbFile.toString();

        try (MockedStatic<Server> serverMockedStatic = mockStatic(Server.class);
             MockedStatic<PortAllocator> portAllocatorMockedStatic = mockStatic(PortAllocator.class);
             MockedStatic<Utility> utilityMockedStatic = mockStatic(Utility.class)) {

            Server mockServer = mock(Server.class);
            serverMockedStatic.when(() -> Server.createTcpServer(anyString(), anyString(), anyString())).thenReturn(mockServer);
            portAllocatorMockedStatic.when(PortAllocator::getInstance).thenReturn(mock(PortAllocator.class));
            when(PortAllocator.getInstance().getNextAvailablePort()).thenReturn(9092);
            utilityMockedStatic.when(() -> Utility.cleanLogString(anyString())).thenReturn("cleanedUrl");

            String serverUrl = h2EmbeddedServerEngine.init(connectionUrl, true);

            assertNotNull(serverUrl);
            verify(mockServer, times(1)).start();

        }
    }


    @Test
    void testGetServerUrl(@TempDir Path tempDir) throws IOException, SQLException {
        Path dbFile = tempDir.resolve("test_database.mv.db");
        String connectionUrl = "jdbc:h2:nio:" + dbFile.toString();

        try (MockedStatic<Server> serverMockedStatic = mockStatic(Server.class);
             MockedStatic<PortAllocator> portAllocatorMockedStatic = mockStatic(PortAllocator.class);
             MockedStatic<Utility> utilityMockedStatic = mockStatic(Utility.class)) {

            Server mockServer = mock(Server.class);
            serverMockedStatic.when(() -> Server.createTcpServer(anyString(), anyString(), anyString())).thenReturn(mockServer);
            portAllocatorMockedStatic.when(PortAllocator::getInstance).thenReturn(mock(PortAllocator.class));
            when(PortAllocator.getInstance().getNextAvailablePort()).thenReturn(9092);
            utilityMockedStatic.when(() -> Utility.cleanLogString(anyString())).thenReturn("cleanedUrl");

            String serverUrl = h2EmbeddedServerEngine.init(connectionUrl, false);

            assertNotNull(serverUrl);
            assertEquals(serverUrl, h2EmbeddedServerEngine.getServerUrl());
            h2EmbeddedServerEngine.close();
        }
    }
    @Test
    void testDbFileNameContainsSemicolon(@TempDir Path tempDir) throws IOException, SQLException, NoSuchFieldException, IllegalAccessException {
        Path dbFile = tempDir.resolve("test_database;extra.mv.db");
        String connectionUrl = "jdbc:h2:nio:" + dbFile.toString();

        try (MockedStatic<Server> serverMockedStatic = mockStatic(Server.class);
             MockedStatic<PortAllocator> portAllocatorMockedStatic = mockStatic(PortAllocator.class);
             MockedStatic<Utility> utilityMockedStatic = mockStatic(Utility.class)) {

            Server mockServer = mock(Server.class);
            serverMockedStatic.when(() -> Server.createTcpServer(anyString(), anyString(), anyString())).thenReturn(mockServer);
            portAllocatorMockedStatic.when(PortAllocator::getInstance).thenReturn(mock(PortAllocator.class));
            when(PortAllocator.getInstance().getNextAvailablePort()).thenReturn(9092);
            utilityMockedStatic.when(() -> Utility.cleanLogString(anyString())).thenReturn("cleanedUrl");

            String serverUrl = h2EmbeddedServerEngine.init(connectionUrl, false);

            File expectedDbFile = new File(tempDir.toString(), "test_database.mv.db");
            assertNotNull(expectedDbFile);
            assertEquals(expectedDbFile.getAbsolutePath(), FilenameUtils.getFullPathNoEndSeparator(dbFile.toString()) + "\\" + "test_database.mv.db");
        }
    }
}
