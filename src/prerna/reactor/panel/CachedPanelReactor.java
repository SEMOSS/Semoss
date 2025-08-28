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
package prerna.reactor.panel;

import prerna.om.InsightPanel;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CachedPanelReactor extends AbstractReactor {

	/**
	 * This code is the same as the Panel Reactor But it has a different op type
	 *
	 * <p>
	 * It is only intended to be used to simplify the cached insight recipe into a
	 * single call to get the panel state instead of multiple calls for each portion
	 * of the insight
	 */
	public CachedPanelReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// first input is the name of the panel
		organizeKeys();
		String panelId = this.keyValue.get(this.keysToGet[0]);
		InsightPanel insightPanel = this.insight.getInsightPanel(panelId);
		if (insightPanel == null) {
			throw new NullPointerException("Panel Id " + panelId + " does not exist");
		}
		NounMetadata noun = new NounMetadata(insightPanel, PixelDataType.PANEL, PixelOperationType.CACHED_PANEL);
		return noun;
	}
}
