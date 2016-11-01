package prerna.sablecc.expressions.sql.builder;

import java.util.List;
import java.util.Vector;

import prerna.sablecc.expressions.IExpressionSelector;

public class SqlGroupBy {

	/*
	 * This class will hold the group bys
	 */
	
	protected List<IExpressionSelector> groupByList = new Vector<IExpressionSelector>();

	protected void addGroupBy(IExpressionSelector groupBySelector) {
		if( !(groupBySelector instanceof SqlColumnSelector) ){
			throw new IllegalArgumentException("Can only group by a column, not an expression");
		}
		groupByList.add(groupBySelector);
	}
	
	protected List<String> getGroupByCols() {
		List<String> groupBys = new Vector<String>();
		for(IExpressionSelector groupBySelector : groupByList) {
			groupBys.add(groupBySelector.toString());
		}
		return groupBys;
	}
	
	public List<IExpressionSelector> getGroupBySelectors() {
		return this.groupByList;
	}
	
	@Override
	public String toString() {
		if(groupByList.isEmpty()) {
			return "";
		}
		
		StringBuilder builder = new StringBuilder(" GROUP BY ");
		int counter = 0;
		for(IExpressionSelector groupBySelector : groupByList) {
			if(counter == 0) {
				builder.append(groupBySelector);
			} else {
				builder.append(" , ").append(groupBySelector);
			}
			counter++;
		}
		
		return builder.toString();
	}
	
}
