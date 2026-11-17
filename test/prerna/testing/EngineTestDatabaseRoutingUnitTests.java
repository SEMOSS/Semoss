package prerna.testing;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.apache.logging.log4j.Logger;
import prerna.auth.*;
import prerna.auth.utils.*;
import prerna.engine.api.*;
import prerna.om.*;
import prerna.tcp.PayloadStruct;
import prerna.tcp.client.workers.NativePyEngineWorker;
import prerna.util.*;

class EngineTestDatabaseRoutingUnitTests {
    private User user() {
        User user=new User(); AccessToken token=new AccessToken();token.setId("editor");token.setProvider(AuthProvider.NATIVE);
        user.setAccessToken(token);user.setPrimaryLogin(AuthProvider.NATIVE);return user;
    }
    private IRDBMSEngine engine(String id, Connection connection) throws Exception {
        IRDBMSEngine engine=mock(IRDBMSEngine.class);
        when(engine.getEngineId()).thenReturn(id);when(engine.getEngineName()).thenReturn(id);
        when(engine.getCatalogType()).thenReturn(IEngine.CATALOG_TYPE.DATABASE);
        when(engine.getSmssProp()).thenReturn(new Properties());when(engine.getCatalogSubType(any())).thenReturn("H2_DB");
        when(engine.getEngineLogger(anyString())).thenReturn(mock(Logger.class));
        when(engine.getConnection()).thenReturn(connection);return engine;
    }
    @Test void redirectsEvenPreviouslyCachedEngineProxiesAndLeavesProductionRowsIntact() throws Exception {
        User user=user();
        Class.forName("org.h2.Driver");
        org.apache.logging.log4j.ThreadContext.put("test", "database-routing");
        try (Connection production=DriverManager.getConnection("jdbc:h2:mem:production-routing");
             Connection fixture=DriverManager.getConnection("jdbc:h2:mem:fixture-routing");
             var security=mockStatic(SecurityEngineUtils.class); var projects=mockStatic(SecurityProjectUtils.class)) {
            production.createStatement().execute("CREATE TABLE ORDERS(ID INT)");fixture.createStatement().execute("CREATE TABLE ORDERS(ID INT)");
            DIHelper.getInstance().setEngineProperty("production-routing",engine("production-routing",production));
            DIHelper.getInstance().setEngineProperty("fixture-routing",engine("fixture-routing",fixture));
            var cached=(IRDBMSEngine) Utility.getDatabase("production-routing",false);
            var context=EngineTestContexts.register(EngineTestContexts.owner(user),"session","app","profile",Map.of("production-routing","fixture-routing"),user,60_000);
            projects.when(() -> SecurityProjectUtils.userCanEditProject(user,"app")).thenReturn(true);
            security.when(() -> SecurityEngineUtils.userCanEditEngine(user,"fixture-routing")).thenReturn(true);
            try (var scope=EngineTestContexts.enterRequest(context.token(),user,"session")) {
                try (var statement=cached.getConnection().createStatement()) { statement.executeUpdate("INSERT INTO ORDERS VALUES(1)"); }
                assertEquals("fixture-routing", Utility.getDatabase("production-routing",false).getEngineId());
                assertThrows(IllegalArgumentException.class, () -> Utility.getDatabase("unmapped-production",false));
            } finally { EngineTestContexts.end(context); }
            try (var rows=production.createStatement().executeQuery("SELECT COUNT(*) FROM ORDERS")) { rows.next(); assertEquals(0,rows.getInt(1)); }
            try (var rows=fixture.createStatement().executeQuery("SELECT COUNT(*) FROM ORDERS")) { rows.next(); assertEquals(1,rows.getInt(1)); }
            assertEquals("production-routing",cached.getEngineId());
        } finally { org.apache.logging.log4j.ThreadContext.remove("test"); DIHelper.getInstance().removeEngineProperty("production-routing");DIHelper.getInstance().removeEngineProperty("fixture-routing"); }
    }
    @Test void pythonDatabaseCallbackCannotFallBackToItsGlobalInsightAfterTestExpiry() {
        User user=user();var normal=new Insight();normal.setUser(user);Insight test;
        var context=EngineTestContexts.register(EngineTestContexts.owner(user),"session","app","profile",Map.of("real","fixture"),user,60_000);
        try (var projects=mockStatic(SecurityProjectUtils.class);var security=mockStatic(SecurityEngineUtils.class)) {
            projects.when(() -> SecurityProjectUtils.userCanEditProject(user,"app")).thenReturn(true);
            try (var request=EngineTestContexts.enterRequest(context.token(),user,"session")) {
                test=new Insight();test.setUser(user);try(var execution=EngineTestContexts.enterInsight(test,user)) { }
            }
            InsightStore.getInstance().put(normal);InsightStore.getInstance().put(test);
            EngineTestContexts.end(context);
            PayloadStruct payload=new PayloadStruct();payload.objId="real";payload.insightId=normal.getInsightId();payload.executionInsightId=test.getInsightId();payload.payload=new Object[]{"SELECT * FROM ORDERS","json"};
            var worker=new NativePyEngineWorker(user,payload,normal);worker.run();
            assertNotNull(worker.getOutput().ex);assertTrue(worker.getOutput().ex.contains("expired"));
            security.verifyNoInteractions();
            InsightStore.getInstance().remove(normal.getInsightId());InsightStore.getInstance().remove(test.getInsightId());
        } finally { EngineTestContexts.end(context); }
    }
}
