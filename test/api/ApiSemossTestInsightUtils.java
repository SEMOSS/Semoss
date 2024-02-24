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

public class ApiSemossTestInsightUtils {

	static void initializeInsight() throws IOException {
		clearInsightCache();
		BaseSemossApiTests.insight = new Insight();
		
		ApiSemossTestUserUtils.setDefaultTestUser();
		
		String insightId = BaseSemossApiTests.insight.getInsightId();
		BaseSemossApiTests.TEST_INSIGHT_CACHE = Paths.get(BaseSemossApiTests.TEST_BASE_DIRECTORY, "InsightCache", "null", insightId);
		Files.createDirectories(BaseSemossApiTests.TEST_INSIGHT_CACHE);
	}

	static void clearInsightCache() throws IOException {
		Path p = Paths.get(BaseSemossApiTests.TEST_BASE_DIRECTORY, "InsightCache", "null");
		if (Files.exists(p)) {
			FileUtils.cleanDirectory(p.toFile());
		}
	}
	
	static void clearInsightCacheDifferently() {
		File dir = BaseSemossApiTests.TEST_INSIGHT_CACHE.toFile();
    	assertTrue(dir.isDirectory());
    	File[] files = dir.listFiles();
    	for (File f : files) {
    		assertTrue("Could not delete: " + f.getName(), f.delete());
    	}
	}

	@SuppressWarnings("unchecked")
	public static String createInsight(String projectId, String insightName) {
		String pixel = ApiSemossTestUtils.buildPixelCall(SaveInsightReactor.class, "project", projectId, "insightName", insightName);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		Map<String, Object> ret = (Map<String, Object>) nm.getValue();
		String insightId = ret.get("app_insight_id").toString();
		return insightId;
	}
}
