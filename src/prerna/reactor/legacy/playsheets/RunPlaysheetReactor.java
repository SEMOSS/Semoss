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
package prerna.reactor.legacy.playsheets;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.om.OldInsight;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.ui.helpers.OldInsightProcessor;
import prerna.util.Utility;
import prerna.util.insight.InsightUtility;

public class RunPlaysheetReactor extends AbstractReactor {

	public RunPlaysheetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.ID.getKey(),
				ReactorKeysEnum.PARAM_KEY.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		// TODO: ACCOUNTING FOR LEGACY PLAYSHEETS
		if (projectId == null) {
			projectId = this.store.getGenRowStruct("app").get(0) + "";
		}
		String insightId = this.keyValue.get(this.keysToGet[1]);
		projectId = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), projectId);
		if (!SecurityInsightUtils.userCanViewInsight(this.insight.getUser(), projectId, insightId)) {
			throw new IllegalArgumentException("User does not have access to this insight");
		}
		IProject project = Utility.getProject(projectId);
		Insight insightObj = project.getInsight(insightId).get(0);
		InsightUtility.transferDefaultVars(this.insight, insightObj);

		// Get the Insight, grab its ID
		// set the user id into the insight
		insightObj.setUser(this.insight.getUser());
		Map<String, List<Object>> params = getParamMap();
		if (!insightObj.isOldInsight()) {
			throw new IllegalArgumentException("This is a legacy pixel that should only be used for old insights");
		}
		((OldInsight) insightObj).setParamHash(params);
		// store in insight store
		InsightStore.getInstance().put(insightObj);
		InsightStore.getInstance().addToSessionHash(getSessionId(), insightObj.getInsightId());

		// TODO: why did we allow the FE to still require this when
		// we already pass a boolean that says this is not pkql....
		// wtf...

		Map<String, Object> insightMap = new HashMap<String, Object>();
		Map<String, Object> stuipdFEInsightGarabage = new HashMap<String, Object>();
		stuipdFEInsightGarabage.put("clear", false);
		stuipdFEInsightGarabage.put("closedPanels", new Object[0]);
		stuipdFEInsightGarabage.put("dataID", 0);
		stuipdFEInsightGarabage.put("feData", new HashMap());
		stuipdFEInsightGarabage.put("insightID", insightObj.getInsightId());
		stuipdFEInsightGarabage.put("newColumns", new HashMap());
		stuipdFEInsightGarabage.put("newInsights", new Object[0]);
		stuipdFEInsightGarabage.put("pkqlData", new Object[0]);
		insightMap.put("insights", new Object[] { stuipdFEInsightGarabage });

		// we have some old legacy stuff...
		// just run and return the object
		OldInsightProcessor processor = new OldInsightProcessor((OldInsight) insightObj);
		Map<String, Object> obj = processor.runWeb();
		obj.put("isPkqlRunnable", false);
		obj.put("recipe", new Object[0]);
		obj.put("pkqlOutput", insightMap);

		SecurityInsightUtils.updateExecutionCountAsync(projectId, insightId);

		return new NounMetadata(obj, PixelDataType.MAP, PixelOperationType.OLD_INSIGHT);
	}

	/**
	 * Get the params for the method
	 * 
	 * @return
	 */
	private Map<String, List<Object>> getParamMap() {
		GenRowStruct mapGrs = this.store.getGenRowStruct(this.keysToGet[2]);
		if (mapGrs != null && !mapGrs.isEmpty()) {
			return (Map<String, List<Object>>) mapGrs.get(0);
		}
		if (!curRow.isEmpty()) {
			return (Map<String, List<Object>>) curRow.get(1);
		}

		return new Hashtable<String, List<Object>>();
	}

}
