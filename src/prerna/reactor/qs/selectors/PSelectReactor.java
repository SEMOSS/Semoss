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
package prerna.reactor.qs.selectors;

import java.util.List;
import java.util.Vector;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class PSelectReactor extends AbstractQueryStructReactor {

	/**
	 * This class is meant to be used for getting a query column selector When we
	 * have a path that we are trying to get Example : JSON parsing
	 */
	public PSelectReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMN.getKey()};
	}

	protected AbstractQueryStruct createQueryStruct() {
		GenRowStruct qsInputs = this.getCurRow();
		if (qsInputs != null && !qsInputs.isEmpty()) {
			List<IQuerySelector> selectors = new Vector<IQuerySelector>();
			for (int selectIndex = 0; selectIndex < qsInputs.size(); selectIndex++) {
				NounMetadata input = qsInputs.getNoun(selectIndex);
				IQuerySelector selector = getSelector(input);
				if (selector != null) {
					selectors.add(selector);
				}
			}
			setAlias(selectors, this.selectorAlias, 0);
			qs.mergeSelectors(selectors);
		}
		return qs;
	}

	private IQuerySelector getSelector(NounMetadata input) {
		QueryColumnSelector c = new QueryColumnSelector();
		c.setTable("PATH");
		c.setColumn(input.getValue().toString());
		return c;
	}
}
