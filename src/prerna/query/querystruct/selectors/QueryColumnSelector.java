package prerna.query.querystruct.selectors;

import java.util.List;
import java.util.Vector;

import prerna.query.querystruct.QueryStruct2;

public class QueryColumnSelector extends AbstractQuerySelector {

	private String column;
	private String table;
	
	public QueryColumnSelector() {
		this.column = "";
		this.table = "";
	}
	
	public QueryColumnSelector(String qsValue) {
		if(qsValue.contains("__")) {
			String[] split = qsValue.split("__");
			this.table = split[0];
			this.column = split[1];
		} else {
			this.table = qsValue;
			this.column = QueryStruct2.PRIM_KEY_PLACEHOLDER;
		}
	}

	@Override
	public SELECTOR_TYPE getSelectorType() {
		return SELECTOR_TYPE.COLUMN;
	}

	@Override
	public String getAlias() {
		if(this.alias.equals("")) {
			if(isPrimKeyColumn()) {
				return this.table;
			} else {
				return this.column;
			}
		}
		return this.alias;
	}
	
	@Override
	public boolean isDerived() {
		return false;
	}

	@Override
	public String getQueryStructName() {
		if(this.column == null || QueryStruct2.PRIM_KEY_PLACEHOLDER.equals(this.column) || this.column.trim().isEmpty()) {
			return this.table;
		} else {
			return this.table + "__" + this.column;
		}
	}

	@Override
	public String getDataType() {
		return null;
	}

	public void setColumn(String column) {
		this.column = column;
	}
	
	public String getColumn() {
		return this.column;
	}
	
	public void setTable(String table) {
		this.table = table;
	}
	
	public String getTable() {
		return this.table;
	}
	
	public boolean isPrimKeyColumn() {
		return PRIM_KEY_PLACEHOLDER.equals(column);
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof QueryColumnSelector) {
			QueryColumnSelector selector = (QueryColumnSelector)obj;
			if(this.column.equals(selector.column) &&
					this.alias.equals(selector.alias) &&
					this.table.equals(selector.table)) {
					return true;
			}
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		String allString = column+":::"+alias+":::"+table;
		return allString.hashCode();
	}

	@Override
	public List<QueryColumnSelector> getAllQueryColumns() {
		// this is a column
		// return it in a list
		List<QueryColumnSelector> usedCols = new Vector<QueryColumnSelector>();
		usedCols.add(this);
		return usedCols;
	}
}
