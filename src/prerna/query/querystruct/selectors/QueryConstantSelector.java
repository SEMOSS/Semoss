package prerna.query.querystruct.selectors;

import java.util.List;
import java.util.Vector;

import prerna.date.SemossDate;
import prerna.reactor.qs.SubQueryExpression;
import prerna.util.Utility;

public class QueryConstantSelector extends AbstractQuerySelector {

	private static final IQuerySelector.SELECTOR_TYPE SELECTOR_TYPE = IQuerySelector.SELECTOR_TYPE.CONSTANT;

	private Object constant;
	
	public QueryConstantSelector() {
		this.constant = "";
	}
	
	public QueryConstantSelector(Object constant) {
		this.constant = constant;
	}
	
	public QueryConstantSelector(Object constant, String alias) {
		this.constant = constant;
		setAlias(alias);
	}

	@Override
	public SELECTOR_TYPE getSelectorType() {
		return SELECTOR_TYPE;
	}

	@Override
	public String getAlias() {
		if(this.alias == null || this.alias.equals("")) {
			return "CONSTANT_" + Utility.makeAlphaNumeric(constant.toString());
		}
		return this.alias;
	}
	
	@Override
	public boolean isDerived() {
		return true;
	}

	@Override
	public String getQueryStructName() {
		if(this.constant instanceof SubQueryExpression) {
			return ((SubQueryExpression) this.constant).getQueryStructName();
		}
		return this.constant.toString();
	}

	@Override
	public String getDataType() {
		if(constant instanceof Number) {
			return "NUMBER";
		} else if(constant instanceof SemossDate) {
			if(((SemossDate) constant).hasTime()) {
				return "TIMESTAMP";
			} else {
				return "DATE";
			}
		} else {
			return "STRING";
		}
	}
	
	public void setConstant(Object constant) {
		this.constant = constant;
	}
	
	public Object getConstant() {
		return this.constant;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof QueryConstantSelector) {
			QueryConstantSelector selector = (QueryConstantSelector)obj;
			if(this.constant.equals(selector.constant) &&
					this.alias.equals(selector.alias)) {
					return true;
			}
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		String allString = constant+":::"+alias;
		return allString.hashCode();
	}

	@Override
	public List<QueryColumnSelector> getAllQueryColumns() {
		// its a constant... obviously this is empty
		List<QueryColumnSelector> usedCols = new Vector<QueryColumnSelector>();
		return usedCols;
	}
	
}
