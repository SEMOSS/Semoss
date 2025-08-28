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

import com.google.gson.TypeAdapter;
import prerna.util.gson.QueryColumnOrderBySelectorAdapter;
import prerna.util.gson.QueryCustomOrderByAdapter;

public interface IQuerySort {

	public enum QUERY_SORT_TYPE {
		COLUMN, CUSTOM
	};

	QUERY_SORT_TYPE getQuerySortType();

	/**
	 * Get the adapter for the sort type
	 *
	 * @param type
	 * @return
	 */
	static TypeAdapter getAdapterForSort(QUERY_SORT_TYPE type) {
		if (type == QUERY_SORT_TYPE.COLUMN) {
			return new QueryColumnOrderBySelectorAdapter();
		} else if (type == QUERY_SORT_TYPE.CUSTOM) {
			return new QueryCustomOrderByAdapter();
		}

		return null;
	}

	/**
	 * Convert string to SELECTOR_TYPE
	 *
	 * @param s
	 * @return
	 */
	static QUERY_SORT_TYPE convertStringToSortType(String s) {
		if (s.equals(QUERY_SORT_TYPE.COLUMN.toString())) {
			return QUERY_SORT_TYPE.COLUMN;
		} else if (s.equals(QUERY_SORT_TYPE.CUSTOM.toString())) {
			return QUERY_SORT_TYPE.CUSTOM;
		}

		return null;
	}
}
