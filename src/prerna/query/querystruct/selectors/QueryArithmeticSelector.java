package prerna.query.querystruct.selectors;

import java.util.List;
import java.util.Vector;

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
		if(this.alias == null || this.alias.equals("")) {
			return this.leftSelector.getAlias()+ "_" + getEnglishForMath() + "_" + this.rightSelector.getAlias();
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
		if(this.leftSelector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.ARITHMETIC) {
			ret += "(" + this.leftSelector.getQueryStructName() + ")";
		} else {
			ret += this.leftSelector.getQueryStructName();
		}
		ret += this.mathExpr;
		if(this.rightSelector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.ARITHMETIC) {
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
		if(obj instanceof QueryArithmeticSelector) {
			QueryArithmeticSelector selector = (QueryArithmeticSelector)obj;
			if(this.leftSelector.equals(selector.leftSelector) &&
					this.rightSelector.equals(selector.rightSelector) &&
					this.mathExpr.equals(selector.mathExpr) &&
					this.alias.equals(selector.alias)) {
					return true;
			}
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		String allString = leftSelector+":::"+this.mathExpr+":::"+this.rightSelector+":::"+alias;
		return allString.hashCode();
	}

	/**
	 * Used for the default alias since most languages will not support
	 * the string version of the math expression (for obvious reasons)
	 * @return
	 */
	private String getEnglishForMath() {
		if(this.mathExpr.equals("+")) {
			return "Plus";
		} else if(this.mathExpr.equals("-")) {
			return "Minus";
		} else if(this.mathExpr.equals("*")) {
			return "MultipiedBy";
		} else if(this.mathExpr.equals("/")) {
			return "DividedBy";
		}
		return "";
	}

	@Override
	public List<QueryColumnSelector> getAllQueryColumns() {
		// grab all the columns from the left selector and the right selector
		List<QueryColumnSelector> usedCols = new Vector<QueryColumnSelector>();
		usedCols.addAll(this.leftSelector.getAllQueryColumns());
		usedCols.addAll(this.rightSelector.getAllQueryColumns());
		return usedCols;
	}
}
