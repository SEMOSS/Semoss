package prerna.sablecc.expressions;

import java.util.List;
import java.util.Vector;

public abstract class AbstractExpressionIterator implements IExpressionIterator{

	protected int numCols = 0;
	
	protected List<String> expression = new Vector<String>();
	protected List<String> newColumnName = new Vector<String>();
	
	protected String[] headers;
	protected String[] joinCols;
	protected String[] groupColumns;

	@Override
	public void setNewColumnName(List<String> newColumnName) {
		this.newColumnName = newColumnName;
	}
	
	@Override
	public List<String> getNewColumnName() {
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
		StringBuilder exprBuilder = new StringBuilder();
		int size = this.expression.size();
		if(this.expression.size() > 0) {
			exprBuilder.append(this.expression.get(0));
			for(int i = 1; i < size; i++) {
				exprBuilder.append(" , ").append(this.expression.get(i));
			}
		}
		return exprBuilder.toString();
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
	public void setExpression(List<String> expression) {
		this.expression = expression;
	}
	
	@Override
	public List<String> getExpression() {
		return this.expression;
	}
	

}
