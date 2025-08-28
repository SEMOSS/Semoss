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
package prerna.reactor.sheet;

import java.util.List;
import java.util.Map;
import prerna.om.InsightSheet;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetSheetConfigReactor extends AbstractSheetReactor {

	public SetSheetConfigReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.SHEET.getKey(), ReactorKeysEnum.SHEET_CONFIG_KEY.getKey()};
	}

	@Override
	public NounMetadata execute() {
		InsightSheet insightSheet = getInsightSheet();
		// get the label
		Map<String, Object> sheetConfig = getSheetConfig();
		// merge the map options
		if (sheetConfig.containsKey("sheetLabel")) {
			insightSheet.setSheetLabel((String) sheetConfig.get("sheetLabel"));
		}
		if (sheetConfig.containsKey("backgroundColor")) {
			insightSheet.setBackgroundColor((String) sheetConfig.get("backgroundColor"));
		}
		if (sheetConfig.containsKey("hideHeaders")) {
			insightSheet.setHideHeaders((Boolean) sheetConfig.get("hideHeaders"));
		}
		if (sheetConfig.containsKey("hideBorders")) {
			insightSheet.setHideBorders((Boolean) sheetConfig.get("hideBorders"));
		}
		if (sheetConfig.containsKey("borderSize")) {
			insightSheet.setBorderSize(((Number) sheetConfig.get("borderSize")).intValue());
		}
		if (sheetConfig.containsKey("height")) {
			insightSheet.setHeight((String) sheetConfig.get("height"));
		}
		if (sheetConfig.containsKey("width")) {
			insightSheet.setWidth((String) sheetConfig.get("width"));
		}

		return new NounMetadata(insightSheet, PixelDataType.SHEET, PixelOperationType.SHEET_CONFIG);
	}

	private Map<String, Object> getSheetConfig() {
		// see if it was passed directly in with the lower case key ornaments
		GenRowStruct genericReactorGrs = this.store.getNoun(keysToGet[1]);
		if (genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
			return (Map<String, Object>) genericReactorGrs.get(0);
		}

		// see if it is in the curRow
		// if it was passed directly in as a variable
		List<NounMetadata> strNouns = this.curRow.getNounsOfType(PixelDataType.MAP);
		if (strNouns != null && !strNouns.isEmpty()) {
			return (Map<String, Object>) strNouns.get(0).getValue();
		}

		return null;
	}
}
