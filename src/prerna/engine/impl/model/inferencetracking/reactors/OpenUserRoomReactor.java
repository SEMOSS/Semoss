/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.engine.impl.model.inferencetracking.reactors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.reactor.insights.AbstractInsightReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.util.insight.InsightUtility;

public class OpenUserRoomReactor extends AbstractInsightReactor {

  public OpenUserRoomReactor() {
    this.keysToGet =
        new String[] {
          ReactorKeysEnum.RECIPE.getKey(),
          ReactorKeysEnum.PARAM_KEY.getKey(),
          ReactorKeysEnum.ID.getKey()
        };
    this.keyRequired = new int[] {0, 0, 0};
  }

  @Override
  public NounMetadata execute() {
    // create a new empty insight
    this.organizeKeys();
    Insight newInsight = new Insight();

    String insightId = this.keyValue.get(this.keysToGet[2]);
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
    try {
      List<String> recipe = getRecipe();
      if (recipe != null) {
        for (String r : recipe) {
          newRecipe.add(Utility.decodeURIComponent(r));
        }
      }
    } catch (IllegalArgumentException e) {
      // ignore
      // by default we throw error when recipe is not passed in
    }

    // return the recipe steps
    Map<String, Object> runnerWraper = new HashMap<String, Object>();
    runnerWraper.put("runner", newInsight.runPixel(newRecipe));
    runnerWraper.put("params", getExecutionParams());
    return new NounMetadata(
        runnerWraper, PixelDataType.PIXEL_RUNNER, PixelOperationType.NEW_EMPTY_INSIGHT);
  }
}
