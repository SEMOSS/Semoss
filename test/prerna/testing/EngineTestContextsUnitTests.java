package prerna.testing;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import java.util.Map;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import prerna.auth.*;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.om.Insight;

class EngineTestContextsUnitTests {
    private User user(AuthProvider provider) {
        User user = new User(); AccessToken token = new AccessToken(); token.setId("editor"); token.setProvider(provider);
        user.setAccessToken(token); user.setPrimaryLogin(provider); return user;
    }
    @Test void bindsCallerSessionAndLoginTypeAndRejectsExpiredContexts() {
        User user = user(AuthProvider.NATIVE);
        var context = EngineTestContexts.register(EngineTestContexts.owner(user), "session", "app", "profile", Map.of("real","test"), user, 60_000);
        try (var projects = mockStatic(SecurityProjectUtils.class)) {
            projects.when(() -> SecurityProjectUtils.userCanEditProject(user, "app")).thenReturn(true);
            assertEquals(context, EngineTestContexts.validate(context.token(),user,"session"));
            assertThrows(IllegalArgumentException.class, () -> EngineTestContexts.validate(context.token(),user,"another-session"));
            assertThrows(IllegalArgumentException.class, () -> EngineTestContexts.validate(context.token(),user(AuthProvider.MICROSOFT),"session"));
            EngineTestContexts.end(context);
            assertThrows(IllegalArgumentException.class, () -> EngineTestContexts.validate(context.token(),user,"session"));
        } finally { EngineTestContexts.end(context); }
    }
    @Test void routesFreshInsightsAndPythonCallbacksAndKeepsNormalInsightsSeparate() throws Exception {
        User user = user(AuthProvider.NATIVE);
        var normal = new Insight(); normal.setUser(user);
        var context = EngineTestContexts.register(EngineTestContexts.owner(user), "session", "app", "profile", Map.of("real","test"), user, 60_000);
        try (var projects = mockStatic(SecurityProjectUtils.class); var engines = mockStatic(SecurityEngineUtils.class)) {
            projects.when(() -> SecurityProjectUtils.userCanEditProject(user,"app")).thenReturn(true);
            engines.when(() -> SecurityEngineUtils.userCanEditEngine(user,"test")).thenReturn(true);
            Insight test;
            try (var request = EngineTestContexts.enterRequest(context.token(),user,"session")) {
                assertThrows(IllegalArgumentException.class, () -> EngineTestContexts.enterInsight(normal,user));
                test = new Insight(); test.setUser(user);
                try (var execution = EngineTestContexts.enterInsight(test,user)) {
                    assertSame(test, EngineTestContexts.executionInsight());
                    assertEquals("test", EngineTestContexts.resolveDatabase("real"));
                    assertEquals("test", EngineTestContexts.resolveDatabase("test"));
                    assertThrows(IllegalArgumentException.class, () -> EngineTestContexts.resolveDatabase("undeclared"));
                }
            }
            assertEquals("real", EngineTestContexts.resolveDatabase("real"));
            try (var callback = EngineTestContexts.enterInsight(test,user)) { assertEquals("test", EngineTestContexts.resolveDatabase("real")); }
            EngineTestContexts.end(context);
            assertThrows(IllegalArgumentException.class, () -> EngineTestContexts.enterInsight(test,user));
            assertEquals("real", EngineTestContexts.resolveDatabase("real"));
        } finally { EngineTestContexts.end(context); }
    }
    @Test void doesNotImplicitlyLeakRoutingIntoNewPoolThreads() throws Exception {
        User user=user(AuthProvider.NATIVE);
        var context=EngineTestContexts.register(EngineTestContexts.owner(user),"session","app","profile",Map.of("real","test"),user,60_000);
        try (var projects=mockStatic(SecurityProjectUtils.class)) {
            projects.when(() -> SecurityProjectUtils.userCanEditProject(user,"app")).thenReturn(true);
            try (var scope=EngineTestContexts.enterRequest(context.token(),user,"session"); var pool=Executors.newSingleThreadExecutor()) {
                assertNull(pool.submit(EngineTestContexts::currentToken).get());
                pool.submit(EngineTestContexts.wrap(() -> assertEquals(context.token(),EngineTestContexts.currentToken()))).get();
                assertNull(pool.submit(EngineTestContexts::currentToken).get());
            }
        } finally { EngineTestContexts.end(context); }
    }
}
