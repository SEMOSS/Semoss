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
package prerna.reactor.qs.filter;

import java.util.List;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.filters.BetweenQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.ReactorKeysEnum;

public class BetweenReactor extends FilterReactor {

	public BetweenReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.START.getKey(),
				ReactorKeysEnum.END.getKey()};
	}

	protected AbstractQueryStruct createQueryStruct() {
		// for now we can only handle simple values
		List<Object> filters = this.curRow.getAllValues();
		// there should be three values
		// first one is the column
		// second is the start
		// third is the end
		BetweenQueryFilter bqf = new BetweenQueryFilter();

		Object oColumn = filters.get(0);
		if (oColumn instanceof QueryColumnSelector)
			bqf.setColumn((QueryColumnSelector) oColumn);

		if (filters.size() >= 1)
			bqf.setStart(filters.get(1));

		if (filters.size() >= 2)
			bqf.setEnd(filters.get(2));

		qs.addExplicitFilter(bqf);

		return qs;
	}
}
