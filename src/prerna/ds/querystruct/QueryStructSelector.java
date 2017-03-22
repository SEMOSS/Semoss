package prerna.ds.querystruct;

public class QueryStructSelector {

	private String column;
	private String table;
	private String math;
	private String alias;
	
	public static String PRIM_KEY_PLACEHOLDER = "PRIM_KEY_PLACEHOLDER";
	
	public QueryStructSelector() {
		//default these so that equals/hashcode will work correctly
		column = "";
		table = "";
		math = "";
		alias = "";
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
	
	public void setMath(String math) {
		this.math = math;
	}
	
	public String getMath() {
		return this.math;
	}
	
	public void setAlias(String alias) {
		this.alias = alias;
	}
	
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
	
	public boolean isPrimKeyColumn() {
		return PRIM_KEY_PLACEHOLDER.equals(column);
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof QueryStructSelector) {
			QueryStructSelector selector = (QueryStructSelector)obj;
			return this.column.equals(selector.column) &&
					this.alias.equals(selector.alias) &&
					this.math.equals(selector.math) &&
					this.table.equals(selector.table);
			
		}
		return false;
	}
	
	public int hashCode() {
		String allString = column+":::"+alias+":::"+math+":::"+table;
		return allString.hashCode();
	}
}
