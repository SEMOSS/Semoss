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
package prerna.reactor.panel.ornaments;

import java.util.HashMap;
import java.util.Map;
import prerna.om.InsightPanel;
import prerna.reactor.panel.AbstractInsightPanelReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RetrievePanelOrnamentsReactor extends AbstractInsightPanelReactor {

	public RetrievePanelOrnamentsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.TRAVERSAL.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// get the insight panel
		InsightPanel insightPanel = getInsightPanel();
		String traversal = getTraversalLiteralInput();
		Map<String, Object> ornamentData = new HashMap<String, Object>();
		// need to add the panel id so the FE knows which panel this is for
		ornamentData.put("panelId", insightPanel.getPanelId());
		if (traversal == null) {
			ornamentData.put("ornaments", insightPanel.getOrnaments());
		} else {
			ornamentData.put("path", traversal);
			Object ornamentMap = insightPanel.getMapInput(insightPanel.getOrnaments(), traversal);
			if (ornamentMap == null) {
				ornamentMap = new HashMap<>();
			}
			ornamentData.put("ornaments", ornamentMap);
		}
		return new NounMetadata(ornamentData, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.PANEL_ORNAMENT);
	}
}
