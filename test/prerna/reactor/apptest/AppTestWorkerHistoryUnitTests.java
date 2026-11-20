package prerna.reactor.apptest;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import java.net.*;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import prerna.util.InternalUtility;
import prerna.util.Utility;

import prerna.project.api.IProject;
import prerna.cluster.util.ClusterUtil;

/** Exercises actual asynchronous dispatch and disk history, with no caller-thread static asset mocks. */
class AppTestWorkerHistoryUnitTests {
    @TempDir Path temporary;
    @Test void workerPersistsCompletionAndRejectsOverlappingRunsForTheSameApp() throws Exception {
        org.junit.jupiter.api.Assumptions.assumeFalse(ClusterUtil.IS_CLUSTER);
        Properties prior=InternalUtility.getCoreProp(), properties=new Properties();
        if(prior!=null)properties.putAll(prior);
        properties.setProperty("BaseFolder",temporary.toString());
        Class<?> helper=Class.forName("prerna.util.DIHelper");
        var instance=helper.getDeclaredMethod("getInstance"); instance.setAccessible(true);
        var projectProperties=helper.getDeclaredMethod("getProjectProp"); projectProperties.setAccessible(true);
        Properties projects=(Properties)projectProperties.invoke(instance.invoke(null));
        Object oldProject=projects.get("history-worker");
        
        IProject project=mock(IProject.class);
        when(project.getCatalogType()).thenReturn(prerna.engine.api.IEngine.CATALOG_TYPE.PROJECT);
        when(project.getEngineId()).thenReturn("history-worker");
        when(project.getSmssProp()).thenReturn(new Properties());
        when(project.getProjectName()).thenReturn("Worker");
        when(project.getEngineName()).thenReturn("Worker");
        InternalUtility.setCoreProp(properties);InternalUtility.setLoadedProject("history-worker",project);
        CountDownLatch entered=new CountDownLatch(1), release=new CountDownLatch(1);
        HttpServer server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        server.createContext("/Monolith/public_home/history-worker/portals/",exchange->{
            entered.countDown();
            try { release.await(15,TimeUnit.SECONDS); } catch(InterruptedException e){Thread.currentThread().interrupt();}
            byte[] body="<html><body><div id='result'>Ready</div></body></html>".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type","text/html");exchange.sendResponseHeaders(200,body.length);exchange.getResponseBody().write(body);exchange.close();
        });server.start();
        try {
            Path assets=Path.of(prerna.util.AssetUtility.getProjectAssetsFolder("history-worker"));
            Map<String,Object> test=AppTestRecordings.save(assets.resolve("app-tests"),Map.of("name","Worker","description","Persist a real worker result.","steps",List.of(Map.of("type","ASSERT_VISIBLE","selector","#result"))));
            String application="http://127.0.0.1:"+server.getAddress().getPort()+"/Monolith";
            String id=AppTestRuns.start("NATIVE:editor","history-worker",null,null,List.of(),application,"fixture-session","JSESSIONID",1,List.of((String)test.get("id")),false);
            assertTrue(entered.await(20,TimeUnit.SECONDS));
            assertThrows(IllegalArgumentException.class,()->AppTestRuns.start("NATIVE:editor","history-worker",null,null,List.of(),application,"fixture-session","JSESSIONID",1,List.of((String)test.get("id")),false));
            release.countDown();
            Map<String,Object> result=assertTimeoutPreemptively(Duration.ofSeconds(30),()->{
                Map<String,Object> current;
                do {current=AppTestRuns.get("NATIVE:editor",id);if(List.of("QUEUED","RUNNING").contains(current.get("status")))Thread.sleep(20);}while(List.of("QUEUED","RUNNING").contains(current.get("status")));
                return current;
            });
            assertEquals("COMPLETED",result.get("status"));assertNull(result.get("historyWarning"));
            Map<String,Object> history=AppTestHistory.read(assets.resolve("app-tests/test-runs"),id,"history-worker",Instant.now());
            assertEquals("COMPLETED",history.get("status"));
            assertEquals("PASSED",((Map<?,?>)((List<?>)history.get("tests")).getFirst()).get("status"));
        } finally {release.countDown();server.stop(0);InternalUtility.setCoreProp(prior);if(oldProject==null)projects.remove("history-worker");else projects.put("history-worker",oldProject);}
    }
}
