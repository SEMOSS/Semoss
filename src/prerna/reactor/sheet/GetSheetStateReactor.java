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

import prerna.om.InsightSheet;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.insight.InsightUtility;

public class GetSheetStateReactor extends AbstractSheetReactor {

	/*
	 * This class is complimentary to SetSheetStateReactor
	 */

	public GetSheetStateReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.SHEET.getKey(), InsightUtility.OUTPUT_TYPE};
	}

	@Override
	public NounMetadata execute() {
		InsightSheet sheet = getInsightSheet();
		if (sheet == null) {
			throw new NullPointerException("No sheet was passed in to get the state");
		}
		String outputType = getOutput();

		// we will just serialize the insight sheet
		return InsightUtility.getSheetState(sheet, outputType);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(InsightUtility.OUTPUT_TYPE)) {
			return "The value to return - as a 'string' or 'map'";
		}
		return super.getDescriptionForKey(key);
	}

	private String getOutput() {
		GenRowStruct grs = this.store.getNoun(InsightUtility.OUTPUT_TYPE);
		if (grs != null && !grs.isEmpty()) {
			String input = grs.get(0).toString();
			if (input.equalsIgnoreCase(InsightUtility.STRING_OUTPUT)) {
				return InsightUtility.STRING_OUTPUT;
			} else {
				return InsightUtility.MAP_OUTPUT;
			}
		}

		if (!this.curRow.isEmpty()) {
			if (this.curRow.toString().equalsIgnoreCase(InsightUtility.STRING_OUTPUT)) {
				return InsightUtility.STRING_OUTPUT;
			} else {
				return InsightUtility.MAP_OUTPUT;
			}
		}

		return InsightUtility.MAP_OUTPUT;
	}
}
