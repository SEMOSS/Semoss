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
package prerna.reactor.insights.save;

import java.util.HashMap;
import java.util.Map;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CheckInsightNameExistsReactor extends AbstractReactor {

	public CheckInsightNameExistsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.INSIGHT_NAME.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		String insightName = this.keyValue.get(this.keysToGet[1]);

		// will just return false
		if (projectId == null || projectId.isEmpty()) {
			throw new IllegalArgumentException("Must provide a project id");
		}

		if (insightName == null || (insightName = insightName.trim()).isEmpty()) {
			Map<String, Object> retMap = new HashMap<>();
			retMap.put("exists", false);
			return new NounMetadata(retMap, PixelDataType.MAP);
		}

		String existingInsightId = SecurityInsightUtils.insightNameExists(projectId, insightName);
		Map<String, Object> retMap = new HashMap<>();
		if (existingInsightId != null) {
			retMap.put("exists", true);
			retMap.put("projectId", projectId);
			retMap.put("insightId", existingInsightId);
			boolean canEdit = SecurityInsightUtils.userCanEditInsight(this.insight.getUser(), projectId,
					existingInsightId);
			retMap.put("userCanEdit", canEdit);
		} else {
			retMap.put("exists", false);
		}
		return new NounMetadata(retMap, PixelDataType.MAP);
	}
}
