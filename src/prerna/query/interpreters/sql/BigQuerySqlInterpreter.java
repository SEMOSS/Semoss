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
package prerna.query.interpreters.sql;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IDatabaseEngine;
import prerna.query.querystruct.selectors.IQuerySelector;

public class BigQuerySqlInterpreter extends SqlInterpreter {

	public BigQuerySqlInterpreter() {
	}

	public BigQuerySqlInterpreter(IDatabaseEngine engine) {
		super(engine);
	}

	public BigQuerySqlInterpreter(ITableDataFrame frame) {
		super(frame);
	}

	@Override
	public void addSelector(IQuerySelector selector) {
		String alias = selector.getAlias();
		String newSelector = processSelector(selector, true) + " AS " + alias;
		if (selectors.length() == 0) {
			selectors = newSelector;
		} else {
			selectors += " , " + newSelector;
		}
		selectorList.add(newSelector);
		selectorAliases.add(alias);
	}

	@Override
	protected void addOrderBySelector() {
		int counter = 0;
		for (StringBuilder orderBySelector : this.orderBySelectors) {
			String alias = "o" + counter++;
			String newSelector = "(" + orderBySelector + ") AS " + alias;
			if (selectors.length() == 0) {
				selectors = newSelector;
			} else {
				selectors += " , " + newSelector;
			}
			selectorList.add(newSelector);
			selectorAliases.add(alias);
		}
	}
}
