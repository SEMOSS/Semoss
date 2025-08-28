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

import java.util.Random;
import java.util.Set;
import prerna.om.InsightSheet;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AddSheetReactor extends AbstractReactor {

	private static Random rand = new Random();

	public AddSheetReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.SHEET.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// first input is the id of the sheet
		String sheetId = null;
		organizeKeys();
		if (this.keyValue.isEmpty()) {
			// need to make sure the panel doesn't already exist
			sheetId = rand.nextInt(5000) + "";
			Set<String> existingPanelIds = this.insight.getInsightPanels().keySet();
			while (existingPanelIds.contains(sheetId)) {
				sheetId = rand.nextInt(5000) + "";
			}
		} else {
			sheetId = this.keyValue.get(this.keysToGet[0]);
		}
		InsightSheet newSheet = new InsightSheet(sheetId);
		this.insight.addNewInsightSheet(newSheet);
		NounMetadata noun = new NounMetadata(newSheet, PixelDataType.SHEET, PixelOperationType.SHEET_OPEN);
		return noun;
	}
}
