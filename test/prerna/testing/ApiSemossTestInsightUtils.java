/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.testing;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.reactor.insights.save.SaveInsightReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ApiSemossTestInsightUtils {

  private static final Logger classLogger = LogManager.getLogger(ApiSemossTestInsightUtils.class);

  private static Path TEST_INSIGHT_CACHE = null;
  private static Insight INSIGHT = null;

  public static Path getInsightCache() {
    return TEST_INSIGHT_CACHE;
  }

  public static Insight getInsight() {
    return INSIGHT;
  }

  static void initializeInsight() throws IOException {
    clearFullInsightCache();
    INSIGHT = new Insight();

    ApiSemossTestUserUtils.setDefaultTestUser();

    String insightId = INSIGHT.getInsightId();
    String session = "test";
    ThreadStore.setSessionId(session);
    TEST_INSIGHT_CACHE =
        Paths.get(ApiTestsSemossConstants.TEST_BASE_DIRECTORY, "InsightCache", session, insightId);
    Files.createDirectories(TEST_INSIGHT_CACHE);
  }

  private static void clearFullInsightCache() throws IOException {
    Path p = Paths.get(ApiTestsSemossConstants.TEST_BASE_DIRECTORY, "InsightCache");
    if (Files.exists(p)) {
      FileUtils.cleanDirectory(p.toFile());
    }
  }

  static void clearInsightCacheDifferently() {
    if (Files.exists(TEST_INSIGHT_CACHE)) {
      try {
        FileUtils.cleanDirectory(TEST_INSIGHT_CACHE.toFile());
      } catch (IOException e) {
        classLogger.error("Could not clear Insight cache", e);
        e.printStackTrace();
      }
    }
  }

  @SuppressWarnings("unchecked")
  public static String createInsight(String projectId, String insightName) {
    String pixel =
        ApiSemossTestUtils.buildPixelCall(
            SaveInsightReactor.class, "project", projectId, "insightName", insightName);
    NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
    Map<String, Object> ret = (Map<String, Object>) nm.getValue();
    String insightId = ret.get("app_insight_id").toString();
    return insightId;
  }
}
