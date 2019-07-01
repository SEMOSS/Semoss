package prerna.query.querystruct.selectors;

import java.util.List;
import java.util.Vector;

import prerna.query.querystruct.SelectQueryStruct;

public class QueryColumnSelector extends AbstractQuerySelector {

	private static final IQuerySelector.SELECTOR_TYPE SELECTOR_TYPE = IQuerySelector.SELECTOR_TYPE.COLUMN;
	
	private String column;
	private String table;
	private String tableAlias;
	
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
			this.column = SelectQueryStruct.PRIM_KEY_PLACEHOLDER;
		}
	}
	
	public QueryColumnSelector(String qsValue, String alias) {
		this(qsValue);
		this.alias = alias;
	}

	@Override
	public SELECTOR_TYPE getSelectorType() {
		return SELECTOR_TYPE;
	}

	@Override
	public String getAlias() {
		if(this.alias == null || this.alias.equals("")) {
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
		if(this.column == null || SelectQueryStruct.PRIM_KEY_PLACEHOLDER.equals(this.column) || this.column.trim().isEmpty()) {
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
	
	public void setTableAlias(String tableAlias) {
		this.tableAlias = tableAlias;
	}
	
	public String getTableAlias() {
		return this.tableAlias;
	}
	
	public boolean isPrimKeyColumn() {
		return (column == null || column.isEmpty() || PRIM_KEY_PLACEHOLDER.equals(column));
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
