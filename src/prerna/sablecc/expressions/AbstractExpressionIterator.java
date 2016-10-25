package prerna.sablecc.expressions;

public abstract class AbstractExpressionIterator implements IExpressionIterator{

	protected int numCols = 0;
	
	protected String expression;
	protected String newColumnName;
	
	protected String[] headers;
	protected String[] joinCols;
	protected String[] groupColumns;

	@Override
	public void setNewColumnName(String newColumnName) {
		this.newColumnName = newColumnName;
	}
	
	@Override
	public String getNewColumnName() {
		return this.newColumnName;
	}
	
	public void setJoinCols(String[] joinCols) {
		this.joinCols = joinCols;
	}

	@Override
	public String[] getJoinColumns() {
		return this.joinCols;
	}
	
	public void setGroupColumns(String[] groupColumns) {
		this.groupColumns = groupColumns;
	}

	@Override
	public String[] getGroupColumns() {
		return this.groupColumns;
	}

	@Override
	public String toString() {
		return this.expression;
	}
	
	@Override
	public String[] getHeaders() {
		return this.headers;
	}
	
	@Override
	public void setHeaders(String[] headers) {
		this.headers = headers;
	}
	
	@Override
	public void setExpression(String expression) {
		this.expression = expression;
	}
	
	@Override
	public String getExpression() {
		return this.expression;
	}
	

}
