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
package prerna.reactor;

import java.util.List;
import java.util.Map;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.insight.InsightUtility;

public class CalcVarReactor extends AbstractReactor {

	public String[] keysToGet = new String[]{ReactorKeysEnum.VARIABLE.getKey()};
	// which of these are optional : 1 means required, 0 means optional
	public int[] keyRequired = new int[]{0}; // if nothing is given calculate everything

	// sample - String [] formulas = new String[]{"x=1", "age_sum =
	// frame_d['age'].astype(int).sum()",
	// "msg = 'Total Age now is {}'.format(age_sum)"};

	@Override
	public NounMetadata execute() {
		organizeKeys();
		// in case variable or variableName is passed
		List dynamicVarNames = null;
		if (this.getNounStore().getNoun(keysToGet[0]) != null) {
			dynamicVarNames = this.getNounStore().getNoun(this.keysToGet[0]).getAllValues();
		} else if (!this.curRow.isEmpty()) {
			dynamicVarNames = this.curRow.getAllValues();
		} else {
			dynamicVarNames = insight.getAllVars();
		}

		// moved the existing logic to InsightUtility to easily use as a static utility
		// method
		Map<String, Object> varValue = InsightUtility.calculateDynamicVars(this.insight, dynamicVarNames);
		return new NounMetadata(varValue, PixelDataType.MAP);
	}
}
