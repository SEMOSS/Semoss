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

public class QueryColumnOrderBySelector extends QueryColumnSelector implements IQuerySort {

	public enum ORDER_BY_DIRECTION {
		ASC, DESC
	};

	private String sortDir = "";

	public QueryColumnOrderBySelector() {
		super();
	}

	public QueryColumnOrderBySelector(String qsValue) {
		super(qsValue);
	}

	public QueryColumnOrderBySelector(String qsValue, String sortDir) {
		super(qsValue);
		setSortDir(sortDir);
	}

	public QueryColumnOrderBySelector(String qsValue, ORDER_BY_DIRECTION sortDir) {
		super(qsValue);
		this.sortDir = sortDir.toString();
	}

	public void setSortDir(String sortDir) {
		this.sortDir = sortDir.toUpperCase();
	}

	public String getSortDirString() {
		return this.sortDir;
	}

	public ORDER_BY_DIRECTION getSortDir() {
		// if empty, assume ascending
		if (this.sortDir.isEmpty()) {
			return ORDER_BY_DIRECTION.ASC;
		}

		/*
		 * Accounting for: ascending increasing up
		 */
		if (this.sortDir.contains("ASC") || this.sortDir.contains("INC") || this.sortDir.contains("UP")) {
			return ORDER_BY_DIRECTION.ASC;
		}

		return ORDER_BY_DIRECTION.DESC;
	}

	@Override
	public QUERY_SORT_TYPE getQuerySortType() {
		return QUERY_SORT_TYPE.COLUMN;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof QueryColumnOrderBySelector) {
			QueryColumnOrderBySelector selector = (QueryColumnOrderBySelector) obj;
			if (super.equals(selector) && this.getSortDir() == selector.getSortDir()) {
				return true;
			}
		}
		return false;
	}
}
