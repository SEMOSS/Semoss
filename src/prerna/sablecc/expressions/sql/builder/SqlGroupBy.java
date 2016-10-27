package prerna.sablecc.expressions.sql.builder;

import java.util.List;
import java.util.Vector;

public class SqlGroupBy {

	/*
	 * This class will hold the group bys
	 */
	
	protected List<SqlColumnSelector> groupByList = new Vector<SqlColumnSelector>();

	protected void addGroupBy(SqlColumnSelector groupBySelector) {
		groupByList.add(groupBySelector);
	}
	
	protected List<String> getGroupByCols() {
		List<String> groupBys = new Vector<String>();
		for(SqlColumnSelector groupBySelector : groupByList) {
			groupBys.add(groupBySelector.toString());
		}
		return groupBys;
	}
	
	public List<SqlColumnSelector> getGroupBySelectors() {
		return this.groupByList;
	}
	
	@Override
	public String toString() {
		if(groupByList.isEmpty()) {
			return "";
		}
		
		StringBuilder builder = new StringBuilder(" GROUP BY ");
		int counter = 0;
		for(SqlColumnSelector groupBySelector : groupByList) {
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
