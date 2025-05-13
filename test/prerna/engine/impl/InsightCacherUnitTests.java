package prerna.engine.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.mockito.*;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.cache.InsightCacheUtility;
import prerna.project.api.IProject;
import prerna.util.Utility;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class InsightCacherUnitTests {

    private InsightCacher insightCacher;

    @Mock
    private IProject workspaceProject;

    @Mock
    private BlockingQueue<List<String>> queue;

    @Mock
    private InsightAdministrator insightAdministrator;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);

        when(workspaceProject.getProjectId()).thenReturn("pid");
        when(workspaceProject.getProjectName()).thenReturn("projectName");
    }

    @Test
    void testRunTwice() throws ParseException, InterruptedException {
        List<String> list = new ArrayList<>();
        list.add("test1");
        list.add("test2");
        when(queue.take()).thenReturn(list).thenReturn(list).thenReturn(null);
        insightCacher = new InsightCacher("savedId", queue, workspaceProject, insightAdministrator, "testName");

        try(MockedStatic<Utility> utilityMockedStatic = Mockito.mockStatic(Utility.class);
            MockedStatic<SecurityInsightUtils> securityInsightUtilsMockedStatic = Mockito.mockStatic(SecurityInsightUtils.class);
            MockedStatic<InsightCacheUtility> insightCacheUtilityMockedStatic = Mockito.mockStatic(InsightCacheUtility.class);) {

            utilityMockedStatic.when(Utility::getApplicationCacheInsightMinutes).thenReturn(5);
            utilityMockedStatic.when(Utility::getApplicationCacheEncrypt).thenReturn(false);
            utilityMockedStatic.when(Utility::getApplicationCacheCron).thenReturn("cron");

            insightCacher.run();

            ArgumentCaptor<String> inName = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<List<String>> lastPixel = ArgumentCaptor.forClass(List.class);
            verify(insightAdministrator, times(1)).addInsight(eq("savedId"), inName.capture(), eq("default"),
                    lastPixel.capture(), eq(false), eq(true), eq(5), eq("cron"), isNull(), eq(false), isNull());

            // verify name is generated correctly
            String in = inName.getValue();
            assertEquals("testName", in.split(" ")[0]);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date parsed = sdf.parse(in.split(" ")[1] + " " + in.split(" ")[2]);
            assertNotNull(parsed);

            // verify last pixel had both
            List<String> lp = lastPixel.getValue();
            assertEquals(2, lp.size());
            assertEquals("test1", lp.get(0));
            assertEquals("test2", lp.get(1));

            String finalIn1 = in;
            securityInsightUtilsMockedStatic.verify(() -> SecurityInsightUtils.addInsight(eq("pid"), eq("savedId"), eq(finalIn1),
                    eq(true), eq("default"), eq(true), eq(5), eq("cron"), isNull(), eq(false), lastPixel.capture(), isNull()), times(1));

            // verify last pixel had both
            lp = lastPixel.getValue();
            assertEquals(2, lp.size());
            assertEquals("test1", lp.get(0));
            assertEquals("test2", lp.get(1));


            // verify second run stuff
            verify(insightAdministrator, times(1)).updateInsight(eq("savedId"), inName.capture(), eq("default"),
                    lastPixel.capture(), eq(false), eq(true), eq(5), eq("cron"), isNull(), eq(false), isNull());

            in = inName.getValue();
            assertEquals("testName", in.split(" ")[0]);
            parsed = sdf.parse(in.split(" ")[1] + " " + in.split(" ")[2]);
            assertNotNull(parsed);

            // verify last pixel had both
            lp = lastPixel.getValue();
            assertEquals(2, lp.size());
            assertEquals("test1", lp.get(0));
            assertEquals("test2", lp.get(1));

            String finalIn = in;
            securityInsightUtilsMockedStatic.verify(() -> SecurityInsightUtils.updateInsight(eq("pid"), eq("savedId"), eq(finalIn),
                    eq(true), eq("default"), eq(true), eq(5), eq("cron"), isNull(), eq(false), lastPixel.capture(), isNull()), times(1));

            // verify last pixel had both
            lp = lastPixel.getValue();
            assertEquals(2, lp.size());
            assertEquals("test1", lp.get(0));
            assertEquals("test2", lp.get(1));

            insightCacheUtilityMockedStatic.verify(() ->
                    InsightCacheUtility.deleteCache("pid", "projectName", "savedId", null, true), times(1));
        }
    }

    @Test
    void testKill() {
        insightCacher = new InsightCacher("savedId", queue, workspaceProject, insightAdministrator, "testName");

        insightCacher.kill();
        verify(queue, times(1)).clear();
        verify(queue, times(1)).add(anyList());
    }

    @ParameterizedTest
    @NullAndEmptySource
    void testSetInsightNameNOE(String insightName) {
        insightCacher = new InsightCacher("savedId", queue, workspaceProject, insightAdministrator, "testName");

        insightCacher.setInsightName(insightName);
        // cannot really assert this
    }



}
