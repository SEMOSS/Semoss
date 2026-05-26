/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.query.querystruct.selectors;

import java.util.ArrayList;
import java.util.List;

public class QueryFunctionSelector extends AbstractQuerySelector {

	private static final IQuerySelector.SELECTOR_TYPE SELECTOR_TYPE = IQuerySelector.SELECTOR_TYPE.FUNCTION;

	private List<IQuerySelector> innerSelectors;
	private String functionName;
	private boolean isDistinct;
	private String colCast;
	private List<Object[]> additionalFunctionParams;
	private String dataType = null;
	private String separator = null;

	public QueryFunctionSelector() {
		this.innerSelectors = new ArrayList<>();
		this.functionName = null;
		this.isDistinct = false;
		this.colCast = "";
		this.additionalFunctionParams = new ArrayList<>();
	}

	@Override
	public SELECTOR_TYPE getSelectorType() {
		return SELECTOR_TYPE;
	}

	@Override
	public String getAlias() {
		if (this.alias == null || this.alias.equals("")) {
			StringBuilder qsConcat = new StringBuilder();
			for (int i = 0; i < this.innerSelectors.size(); i++) {
				qsConcat.append(this.innerSelectors.get(i).getAlias());
			}
			return QueryFunctionHelper.getPrettyName(this.functionName) + "_" + qsConcat;
		}
		return this.alias;
	}

	@Override
	public boolean isDerived() {
		return true;
	}

	@Override
	public String getDataType() {
		if (dataType == null) {
			dataType = QueryFunctionHelper.determineTypeOfFunction(functionName);
		}
		return dataType;
	}

	@Override
	public String getQueryStructName() {
		StringBuilder qsConcat = new StringBuilder();
		for (int i = 0; i < this.innerSelectors.size(); i++) {
			qsConcat.append(this.innerSelectors.get(i).getQueryStructName());
		}
		return QueryFunctionHelper.getPrettyName(this.functionName) + "(" + qsConcat + ")";
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	public List<IQuerySelector> getInnerSelector() {
		return innerSelectors;
	}

	public void addInnerSelector(IQuerySelector innerSelector) {
		this.innerSelectors.add(innerSelector);
	}

	public void setInnerSelector(List<IQuerySelector> innerSelectors) {
		this.innerSelectors = innerSelectors;
	}

	public String getFunction() {
		return functionName;
	}

	public void setFunction(String functionName) {
		this.functionName = functionName;
	}

	public boolean isDistinct() {
		return isDistinct;
	}

	public void setDistinct(boolean isDistinct) {
		this.isDistinct = isDistinct;
	}

	public void setColCast(String colCast) {
		this.colCast = colCast;
	}

	public String getColCast() {
		return this.colCast;
	}

	public void addAdditionalParam(Object[] param) {
		this.additionalFunctionParams.add(param);
	}

	public List<Object[]> getAdditionalFunctionParams() {
		return additionalFunctionParams;
	}

	public void setAdditionalFunctionParams(List<Object[]> additionalFunctionParams) {
		this.additionalFunctionParams = additionalFunctionParams;
	}

	public String getSeparator() {
		return separator;
	}

	public void setSeparator(String separator) {
		this.separator = separator;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof QueryFunctionSelector) {
			QueryFunctionSelector selector = (QueryFunctionSelector) obj;
			if (this.innerSelectors.equals(selector.innerSelectors) && this.alias.equals(selector.alias)
					&& this.functionName.equals(selector.functionName) && this.isDistinct == selector.isDistinct) {
				return true;
			}
		}
		return false;
	}

	@Override
	public int hashCode() {
		String allString = innerSelectors + ":::" + alias + ":::" + functionName + ":::" + isDistinct;
		return allString.hashCode();
	}

	@Override
	public List<QueryColumnSelector> getAllQueryColumns() {
		// grab all the columns from the inner selector
		List<QueryColumnSelector> usedCols = new ArrayList<>();
		for (int i = 0; i < this.innerSelectors.size(); i++) {
			usedCols.addAll(this.innerSelectors.get(i).getAllQueryColumns());
		}
		return usedCols;
	}

	/**
	 * Helper method to generate a function selector on a column
	 * 
	 * @param function
	 * @param qsName
	 * @param alias
	 * @return
	 */
	public static QueryFunctionSelector makeFunctionSelector(String function, String qsName, String alias) {
		return makeFunctionSelector(function, new QueryColumnSelector(qsName), alias);
	}

	/**
	 * Helper method to generate a function selector on a selector
	 * 
	 * @param function
	 * @param selector
	 * @param alias
	 * @return
	 */
	public static QueryFunctionSelector makeFunctionSelector(String function, IQuerySelector selector, String alias) {
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction(function);
		fun.addInnerSelector(selector);
		fun.setAlias(alias);
		return fun;
	}

	/**
	 * Make coalesce selector between 2 columns
	 * 
	 * @param qsName1
	 * @param qsName2
	 * @param alias
	 * @return
	 */
	public static QueryFunctionSelector makeCol2ColCoalesceSelector(String qsName1, String qsName2, String alias) {
		return makeCoalesceSelector(new QueryColumnSelector(qsName1), new QueryColumnSelector(qsName2), alias);
	}

	/**
	 * Make coalesce selector
	 * 
	 * @param qsName1
	 * @param value
	 * @param alias
	 * @return
	 */
	public static QueryFunctionSelector makeCol2ValCoalesceSelector(String qsName1, Object value, String alias) {
		return makeCoalesceSelector(new QueryColumnSelector(qsName1), new QueryConstantSelector(value), alias);
	}

	/**
	 * Make concat function
	 * 
	 * @param qsName1
	 * @param qsName2
	 * @param alias
	 * @return
	 */
	public static QueryFunctionSelector makeConcat2ColumnsFunction(String qsName1, String qsName2, String alias) {
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction(QueryFunctionHelper.CONCAT);
		fun.addInnerSelector(new QueryColumnSelector(qsName1));
		fun.addInnerSelector(new QueryColumnSelector(qsName2));
		fun.setAlias(alias);
		return fun;
	}

	/**
	 * Make coalesce selector
	 * 
	 * @param selector1
	 * @param selector2
	 * @param alias
	 * @return
	 */
	public static QueryFunctionSelector makeCoalesceSelector(IQuerySelector selector1, IQuerySelector selector2,
			String alias) {
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction(QueryFunctionHelper.COALESCE);
		fun.addInnerSelector(selector1);
		fun.addInnerSelector(selector2);
		fun.setAlias(alias);
		return fun;
	}

	/**
	 * Helper method to generate a date diff function selector
	 * 
	 * @param timeUnit
	 * @param selector1
	 * @param selector2
	 * @param alias
	 * @return
	 */
	public static QueryFunctionSelector makeDateDiffFunctionSelector(String timeUnit, IQuerySelector selector1,
			IQuerySelector selector2, String alias) {
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction(QueryFunctionHelper.DATEDIFF);
		fun.addAdditionalParam(new Object[] { timeUnit });
		fun.addInnerSelector(selector1);
		fun.addInnerSelector(selector2);
		fun.setAlias(alias);
		return fun;
	}

	/**
	 * Helper method to generate a date add function selector
	 * 
	 * @param timeUnit
	 * @param valueToAdd
	 * @param selector1
	 * @param alias
	 * @return
	 */
	public static QueryFunctionSelector makeDateAddFunctionSelector(String timeUnit, int valueToAdd,
			IQuerySelector selector1, String alias) {
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.setFunction(QueryFunctionHelper.DATE_ADD);
		fun.addAdditionalParam(new Object[] { timeUnit, valueToAdd });
		fun.addInnerSelector(selector1);
		fun.setAlias(alias);
		return fun;
	}

}