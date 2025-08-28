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

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import prerna.om.HeadersException;

public class QueryOpaqueSelector extends AbstractQuerySelector {

	private static final IQuerySelector.SELECTOR_TYPE SELECTOR_TYPE = IQuerySelector.SELECTOR_TYPE.OPAQUE;

	private String querySelectorSyntax;
	private String table;

	public QueryOpaqueSelector() {
	}

	public QueryOpaqueSelector(String querySelectorSyntax) {
		this.querySelectorSyntax = querySelectorSyntax;
	}

	public QueryOpaqueSelector(String querySelectorSyntax, String alias) {
		this.querySelectorSyntax = querySelectorSyntax;
		this.alias = alias;
	}

	@Override
	public SELECTOR_TYPE getSelectorType() {
		return SELECTOR_TYPE;
	}

	@Override
	public String getAlias() {
		if (this.alias == null || this.alias.equals("")) {
			String cleanSelectorSyntax = HeadersException.getInstance().recursivelyFixHeaders(this.querySelectorSyntax,
					new ArrayList<String>());
			return cleanSelectorSyntax;
		}
		return this.alias;
	}

	public String getQuerySelectorSyntax() {
		return this.querySelectorSyntax;
	}

	public void setQuerySelectorSyntax(String querySelectorSyntax) {
		this.querySelectorSyntax = querySelectorSyntax;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public String getTable() {
		return this.table;
	}

	@Override
	public boolean isDerived() {
		return true;
	}

	@Override
	public String getQueryStructName() {
		return getAlias();
	}

	@Override
	public String getDataType() {
		return null;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof QueryOpaqueSelector) {
			QueryOpaqueSelector selector = (QueryOpaqueSelector) obj;
			if (this.querySelectorSyntax.equals(selector.querySelectorSyntax) && this.alias.equals(selector.alias)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public int hashCode() {
		String allString = querySelectorSyntax + ":::" + alias;
		return allString.hashCode();
	}

	@Override
	public List<QueryColumnSelector> getAllQueryColumns() {
		// return empty list
		List<QueryColumnSelector> usedCols = new Vector<QueryColumnSelector>();
		return usedCols;
	}
}
