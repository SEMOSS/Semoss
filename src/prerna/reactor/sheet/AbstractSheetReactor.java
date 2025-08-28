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
import prerna.om.InsightSheet;
import prerna.reactor.AbstractReactor;
// import prerna.om.InsightPanel;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public abstract class AbstractSheetReactor extends AbstractReactor {

	protected InsightSheet getInsightSheet() {
		// passed in directly as sheet
		GenRowStruct genericReactorGrs = this.store.getNoun(ReactorKeysEnum.SHEET.getKey());
		if (genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
			NounMetadata noun = genericReactorGrs.getNoun(0);
			PixelDataType nounType = noun.getNounType();
			if (nounType == PixelDataType.SHEET) {
				return (InsightSheet) noun.getValue();
			} else if (nounType == PixelDataType.COLUMN || nounType == PixelDataType.CONST_STRING) {
				String sheetId = noun.getValue().toString();
				return this.insight.getInsightSheet(sheetId);
			}
		}

		// see if it is in the curRow
		// if it was passed directly in as a variable
		List<NounMetadata> panelNouns = this.curRow.getNounsOfType(PixelDataType.SHEET);
		if (panelNouns != null && !panelNouns.isEmpty()) {
			return (InsightSheet) panelNouns.get(0).getValue();
		}

		// see if string or column passed in
		List<String> strInputs = this.curRow.getAllStrValues();
		if (strInputs != null && !strInputs.isEmpty()) {
			for (String sheetId : strInputs) {
				InsightSheet sheet = this.insight.getInsightSheet(sheetId);
				if (sheet != null) {
					return sheet;
				}
			}
		}

		List<NounMetadata> strNouns = this.curRow.getNounsOfType(PixelDataType.CONST_INT);
		if (strNouns != null && !strNouns.isEmpty()) {
			return this.insight.getInsightSheet(strNouns.get(0).getValue().toString());
		}

		// well, you are out of luck
		return null;
	}
}
