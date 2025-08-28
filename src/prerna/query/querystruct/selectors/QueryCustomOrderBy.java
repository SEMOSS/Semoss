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
package prerna.query.querystruct.selectors;

import java.util.List;
import java.util.Vector;

public class QueryCustomOrderBy implements IQuerySort {

	private List<Object> customOrder = new Vector<Object>();
	private QueryColumnSelector columnToSort = null;

	public QueryCustomOrderBy() {
	}

	public void setCustomOrder(List<Object> customOrder) {
		this.customOrder = customOrder;
	}

	public List<Object> getCustomOrder() {
		return this.customOrder;
	}

	public QueryColumnSelector getColumnToSort() {
		return columnToSort;
	}

	public void setColumnToSort(QueryColumnSelector columnToSort) {
		this.columnToSort = columnToSort;
	}

	@Override
	public QUERY_SORT_TYPE getQuerySortType() {
		return QUERY_SORT_TYPE.CUSTOM;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof QueryCustomOrderBy) {
			QueryCustomOrderBy orderBy = (QueryCustomOrderBy) obj;
			// see if the 2 lists are the same
			return this.columnToSort.equals(orderBy.columnToSort) && this.customOrder.equals(orderBy.customOrder);
		}
		return false;
	}
}
