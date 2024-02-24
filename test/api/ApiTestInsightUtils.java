package api;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import prerna.om.Insight;
import prerna.reactor.insights.save.SaveInsightReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ApiTestInsightUtils {

	static void initializeInsight() throws IOException {
		clearInsightCache();
		ApiTests.insight = new Insight();
		
		ApiTestUserUtils.setDefaultTestUser();
		
		String insightId = ApiTests.insight.getInsightId();
		ApiTests.TEST_INSIGHT_CACHE = Paths.get(ApiTests.TEST_BASE_DIRECTORY, "InsightCache", "null", insightId);
		Files.createDirectories(ApiTests.TEST_INSIGHT_CACHE);
	}

	static void clearInsightCache() throws IOException {
		Path p = Paths.get(ApiTests.TEST_BASE_DIRECTORY, "InsightCache", "null");
		if (Files.exists(p)) {
			FileUtils.cleanDirectory(p.toFile());
		}
	}
	
	static void clearInsightCacheDifferently() {
		File dir = ApiTests.TEST_INSIGHT_CACHE.toFile();
    	assertTrue(dir.isDirectory());
    	File[] files = dir.listFiles();
    	for (File f : files) {
    		assertTrue("Could not delete: " + f.getName(), f.delete());
    	}
	}

	@SuppressWarnings("unchecked")
	public static String createInsight(String projectId, String insightName) {
		String pixel = ApiTestUtils.buildPixelCall(SaveInsightReactor.class, "project", projectId, "insightName", insightName);
		NounMetadata nm = ApiTestUtils.processPixel(pixel);
		Map<String, Object> ret = (Map<String, Object>) nm.getValue();
		String insightId = ret.get("app_insight_id").toString();
		return insightId;
	}
}
