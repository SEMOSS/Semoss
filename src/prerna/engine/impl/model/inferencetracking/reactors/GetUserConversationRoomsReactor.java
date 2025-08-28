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
package prerna.engine.impl.model.inferencetracking.reactors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.User;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.insight.InsightUtility;

public class GetUserConversationRoomsReactor extends AbstractReactor {
  @SuppressWarnings("unused")
  private static final Logger logger = LogManager.getLogger(GetUserConversationRoomsReactor.class);

  public GetUserConversationRoomsReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.PROJECT.getKey()};
    this.keyRequired = new int[] {0};
  }

  @Override
  public NounMetadata execute() {
    organizeKeys();
    User user = this.insight.getUser();
    if (user == null) {
      throw new IllegalArgumentException("You are not properly logged in");
    }
    String projectId = this.keyValue.get(this.keysToGet[0]);
    if (projectId == null) {
      projectId = this.insight.getContextProjectId();
    }
    List<Map<String, Object>> output =
        ModelInferenceLogsUtils.getUserConversations(
            user.getPrimaryLoginToken().getId(), projectId);
    if (output.size() > 0) {
      for (Map<String, Object> convo : output) {
        createInsights((String) convo.get("ROOM_ID"));
        // NounStore ns = new NounStore(ReactorKeysEnum.ALL.getKey());
        // ns.makeNoun(ReactorKeysEnum.ID.getKey()).addLiteral();
        //				OpenUserRoomReactor newInsight = new OpenUserRoomReactor();
        //
        //				newInsight.setInsight(this.insight);
        //				newInsight
        //				newInsight.setNounStore(ns);
        //				NounMetadata newInsightCreated = newInsight.execute();
        //				if (newInsightCreated.getNounType() == PixelDataType.ERROR) {
        //					throw new SemossPixelException((String) newInsightCreated.getValue());
        //				}
      }
    }
    return new NounMetadata(output, PixelDataType.VECTOR);
  }

  public NounMetadata createInsights(String insightId) {
    Insight newInsight = new Insight();
    if (insightId != null
        && !insightId.isEmpty()
        && !InsightStore.getInstance().containsKey(insightId)) {
      List<Map<String, Object>> output =
          ModelInferenceLogsUtils.doVerifyConversation(this.insight.getUserId(), insightId);
      if (output.size() > 0) {
        newInsight.setInsightId(insightId);
        String projectId = (String) output.get(0).get("PROJECT_ID");
        if (projectId != null && !projectId.isEmpty()) {
          newInsight.setProjectId(projectId);
        }
      } else {
        if (this.insight.getProjectId() != null && !this.insight.getProjectId().isEmpty()) {
          newInsight.setProjectId(this.insight.getProjectId());
        }
      }
    }

    if (this.insight.getContextProjectId() != null
        && !this.insight.getContextProjectId().isEmpty()) {
      newInsight.setContextProjectId(this.insight.getContextProjectId());
    }

    newInsight.setCacheInWorkspace(true);
    InsightUtility.transferDefaultVars(this.insight, newInsight);
    InsightStore.getInstance().put(newInsight);
    InsightStore.getInstance().addToSessionHash(getSessionId(), newInsight.getInsightId());
    // set the user in the insight
    newInsight.setUser(this.insight.getUser());

    List<String> newRecipe = new Vector<String>();

    // return the recipe steps
    Map<String, Object> runnerWraper = new HashMap<String, Object>();
    runnerWraper.put("runner", newInsight.runPixel(newRecipe));
    return new NounMetadata(
        runnerWraper, PixelDataType.PIXEL_RUNNER, PixelOperationType.NEW_EMPTY_INSIGHT);
  }
}
