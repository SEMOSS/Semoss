package prerna.sablecc.expressions;

import java.util.List;
import java.util.Vector;

public abstract class AbstractExpressionGroupBy {

	/*
	 * This class will hold the list of group by values for the expression
	 * Note: the only method that needs to be overridden is the toString()
	 * method which actual generates the script in the appropriate format
	 * for proper compilation
	 */
	
	protected List<IExpressionSelector> groupByList = new Vector<IExpressionSelector>();

	@Override
	public abstract String toString();

	/**
	 * Add a new group by
	 * @param groupBySelector
	 */
	public void addGroupBy(IExpressionSelector groupBySelector) {
		groupByList.add(groupBySelector);
	}
	
	/**
	 * Get the list of the group by columns names
	 * @return
	 */
	public List<String> getGroupByCols() {
		List<String> groupBys = new Vector<String>();
		for(IExpressionSelector groupBySelector : groupByList) {
			groupBys.add(groupBySelector.toString());
		}
		return groupBys;
	}
	
	/**
	 * Get the list of the group by objects
	 * @return
	 */
	public List<IExpressionSelector> getGroupBySelectors() {
		return this.groupByList;
	}
}
