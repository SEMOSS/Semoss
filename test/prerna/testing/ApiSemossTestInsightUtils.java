package prerna.testing;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.reactor.insights.save.SaveInsightReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ApiSemossTestInsightUtils {
	
	private static Path TEST_INSIGHT_CACHE = null;
	private static Insight INSIGHT = null;
	
	public static Path getInsightCache() {
		return TEST_INSIGHT_CACHE;
	}
	
	public static Insight getInsight() {
		return INSIGHT;
	}

	static void initializeInsight() throws IOException {
		clearInsightCache();
		INSIGHT = new Insight();
		
		ApiSemossTestUserUtils.setDefaultTestUser();
		
		String insightId = INSIGHT.getInsightId();
		String session = "test";
		ThreadStore.setSessionId(session);
		TEST_INSIGHT_CACHE = Paths.get(ApiTestsSemossConstants.TEST_BASE_DIRECTORY, "InsightCache", session, insightId);
		Files.createDirectories(TEST_INSIGHT_CACHE);
	}

	static void clearInsightCache() throws IOException {
		Path p = Paths.get(ApiTestsSemossConstants.TEST_BASE_DIRECTORY, "InsightCache", "test");
		if (Files.exists(p)) {
			FileUtils.cleanDirectory(p.toFile());
		}
	}
	
	static void clearInsightCacheDifferently() throws IOException {
//		File dir = ApiSemossTestInsightUtils.getInsightCache().toFile();
//    	assertTrue(dir.isDirectory());
//    	File[] files = dir.listFiles();
//    	for (File f : files) {
//    		assertTrue(f.delete(), "Could not delete: " + f.getName());
//    	}
		Path p = Paths.get(ApiTestsSemossConstants.TEST_BASE_DIRECTORY, "InsightCache");
		if (Files.exists(p)) {
			FileUtils.cleanDirectory(p.toFile());
		}
		
		Files.createDirectories(TEST_INSIGHT_CACHE);
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
