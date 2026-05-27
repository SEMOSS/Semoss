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

public class QueryArithmeticSelector extends AbstractQuerySelector {

	private static final IQuerySelector.SELECTOR_TYPE SELECTOR_TYPE = IQuerySelector.SELECTOR_TYPE.ARITHMETIC;

	private IQuerySelector leftSelector;
	private String mathExpr;
	private IQuerySelector rightSelector;
	boolean encapsulated = false;

	public QueryArithmeticSelector() {
		this.mathExpr = "";
	}

	@Override
	public SELECTOR_TYPE getSelectorType() {
		return SELECTOR_TYPE;
	}

	@Override
	public String getAlias() {
		if (this.alias == null || this.alias.equals("")) {
			return this.leftSelector.getAlias() + "_" + getEnglishForMath() + "_" + this.rightSelector.getAlias();
		}
		return this.alias;
	}

	@Override
	public boolean isDerived() {
		return true;
	}

	@Override
	public String getQueryStructName() {
		String ret = "";
		if (this.leftSelector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.ARITHMETIC) {
			ret += "(" + this.leftSelector.getQueryStructName() + ")";
		} else {
			ret += this.leftSelector.getQueryStructName();
		}
		ret += this.mathExpr;
		if (this.rightSelector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.ARITHMETIC) {
			ret += "(" + this.rightSelector.getQueryStructName() + ")";
		} else {
			ret += this.rightSelector.getQueryStructName();
		}
		return ret;
	}

	@Override
	public String getDataType() {
		return "NUMBER";
	}

	public boolean isEncapsulated() {
		return this.encapsulated;
	}

	public void setEncapsulated(boolean encapsulated) {
		this.encapsulated = encapsulated;
	}

	public IQuerySelector getLeftSelector() {
		return leftSelector;
	}

	public void setLeftSelector(IQuerySelector leftSelector) {
		this.leftSelector = leftSelector;
	}

	public String getMathExpr() {
		return mathExpr;
	}

	public void setMathExpr(String mathExpr) {
		this.mathExpr = mathExpr;
	}

	public IQuerySelector getRightSelector() {
		return rightSelector;
	}

	public void setRightSelector(IQuerySelector rightSelector) {
		this.rightSelector = rightSelector;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof QueryArithmeticSelector) {
			QueryArithmeticSelector selector = (QueryArithmeticSelector) obj;
			if (this.leftSelector.equals(selector.leftSelector) && this.rightSelector.equals(selector.rightSelector)
					&& this.mathExpr.equals(selector.mathExpr) && this.alias.equals(selector.alias)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public int hashCode() {
		String allString = leftSelector + ":::" + this.mathExpr + ":::" + this.rightSelector + ":::" + alias;
		return allString.hashCode();
	}

	/**
	 * Used for the default alias since most languages will not support the string
	 * version of the math expression (for obvious reasons)
	 * 
	 * @return
	 */
	private String getEnglishForMath() {
		if (this.mathExpr.equals("+")) {
			return "Plus";
		} else if (this.mathExpr.equals("-")) {
			return "Minus";
		} else if (this.mathExpr.equals("*")) {
			return "MultipiedBy";
		} else if (this.mathExpr.equals("/")) {
			return "DividedBy";
		}
		return "";
	}

	@Override
	public List<QueryColumnSelector> getAllQueryColumns() {
		// grab all the columns from the left selector and the right selector
		List<QueryColumnSelector> usedCols = new ArrayList<QueryColumnSelector>();
		usedCols.addAll(this.leftSelector.getAllQueryColumns());
		usedCols.addAll(this.rightSelector.getAllQueryColumns());
		return usedCols;
	}

	/**
	 * 
	 * @param leftColQs
	 * @param rightColQs
	 * @param mathExpr
	 * @param alias
	 * @return
	 */
	public static QueryArithmeticSelector makeCol2ColSelector(String leftColQs, String rightColQs, String mathExpr,
			String alias) {
		return makeCol2ColSelector(new QueryColumnSelector(leftColQs), new QueryColumnSelector(rightColQs), mathExpr,
				alias);
	}

	/**
	 * 
	 * @param leftSelector
	 * @param rightSelector
	 * @param mathExpr
	 * @param alias
	 * @return
	 */
	public static QueryArithmeticSelector makeCol2ColSelector(IQuerySelector leftSelector, IQuerySelector rightSelector,
			String mathExpr, String alias) {
		QueryArithmeticSelector math = new QueryArithmeticSelector();
		math.setLeftSelector(leftSelector);
		math.setRightSelector(rightSelector);
		math.setMathExpr(mathExpr);
		math.setAlias(alias);
		return math;
	}
}
