package prerna.sablecc2.reactor.imports;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.Insight;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.sablecc2.om.Join;
import prerna.util.ArrayUtilityMethods;

public abstract class AbstractImporter implements IImporter {

	protected Insight in = null;
	
	@Override
	public void setInsight(Insight in) {
		this.in = in;
	}
	
	/**
	 * Perform to see if the string v is contained within the set taking into consideration an ignore set
	 * @param v
	 * @param set
	 * @param ignoreSet
	 * @return
	 */
	protected String setIgnoreCaseMatch(String v, Set<String> set, Set<String> ignoreSet) {
		for(String s : set) {
			if(ignoreSet.contains(s)) {
				continue;
			}
			if(s.equalsIgnoreCase(v)) {
				return s;
			}
		}
		return null;
	}
	
	/**
	 * Get the set of join columns we are using on the new table that is generated
	 * @param joins
	 * @return
	 */
	protected Set<String> getRightJoinColumns(List<Join> joins) {
		Set<String> rightTableJoinCols = new HashSet<String>();
		int numJoins = joins.size();
		for(int jIdx = 0; jIdx < numJoins; jIdx++) {
			Join j = joins.get(jIdx);
			String rightTableJoinCol = j.getQualifier();
			if(rightTableJoinCol.contains("__")) {
				rightTableJoinCol = rightTableJoinCol.split("__")[1];
			}
			
			// keep track of join columns on the right table
			rightTableJoinCols.add(rightTableJoinCol);
		}
		return rightTableJoinCols;
	}
	
	/**
	 * Update the frame meta based on a 
	 * @param dataframe
	 * @param qs
	 * @param it
	 * @param joins
	 * @param rightTableAlias
	 */
	protected void updateMetaWithAlias(ITableDataFrame dataframe, SelectQueryStruct qs, Iterator<IHeadersDataRow> it, List<Join> joins, Map<String, String> rightTableAlias) {
		for(String rightCol : rightTableAlias.keySet()) {
			List<IQuerySelector> selectors = qs.getSelectors();
			int numSelectors = selectors.size();
			for(int i = 0; i < numSelectors; i++) {
				IQuerySelector selector = selectors.get(i);
				String alias =  selector.getAlias();
				if(alias.equals(rightCol)) {
					selector.setAlias(rightTableAlias.get(rightCol));
				}
			}
		}
		ImportUtility.parseQueryStructToFlatTableWithJoin(dataframe, qs, dataframe.getTableName(), it, joins);
		dataframe.syncHeaders();
	}
	
	/**
	 * Determine if all the headers are taken into consideration within the iterator
	 * This helps to determine if we need to perform an insert vs. an update query to fill the frame
	 * @param headers1				The original set of headers in the frame
	 * @param headers2				The new set of headers from the iterator
	 * @param joins					Needs to take into consideration the joins since we can join on 
	 * 								columns that do not have the same names
	 * @return
	 */
	protected boolean allHeadersAccounted(String[] startHeaders, String[] newHeaders, List<Join> joins) {
		int newHeadersSize = newHeaders.length;
		for(int i = 0; i <  newHeadersSize; i++) {
			// need each of the new headers to be included in the start headers
			if(!ArrayUtilityMethods.arrayContainsValue(startHeaders, newHeaders[i])) {
				// need to account for join 
				for(Join join : joins) {
					String startNode = join.getSelector();
					if(startNode.contains("__")) {
						startNode = startNode.split("__")[1];
					}
					String endNode = join.getQualifier();
					if(newHeaders[i].equalsIgnoreCase(endNode)) {
						continue;
					} else {
						return false;
					}
				}
			}
		}
		// we were able to iterate through all the new headers
		// and each one exists in the starting headers
		// so all of them are taking into consideration
		return true;
	}

}
