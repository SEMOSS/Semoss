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

import java.util.Map;
import prerna.om.InsightPanel;
import prerna.om.InsightSheet;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CloseSheetReactor extends AbstractReactor {

	public CloseSheetReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.SHEET.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// first input is the name of the sheet
		String sheetId = this.curRow.get(0).toString();
		Map<String, InsightPanel> panels = this.insight.getInsightPanels();
		for (String key : panels.keySet()) {
			if (panels.get(key).getSheetId().equals(sheetId)) {
				throw new IllegalArgumentException("Cannot close sheet while it has open panels");
			}
		}
		// actually remove it once we know there are no panels
		InsightSheet sheetToDelete = this.insight.getInsightSheets().remove(sheetId);
		if (sheetToDelete == null) {
			throw new IllegalArgumentException("Could not find sheet with id = " + sheetId + " to close.");
		}
		return new NounMetadata(sheetId, PixelDataType.CONST_STRING, PixelOperationType.SHEET_CLOSE);
	}
}
