package prerna.algorithm.impl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPMismatchException;

import prerna.ds.R.RDataTable;
import prerna.sablecc.AbstractReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;

public abstract class RBaseReducer extends AbstractReactor {

	public RBaseReducer() {
		String[] thisReacts = { PKQLEnum.EXPR_TERM, PKQLEnum.DECIMAL, PKQLEnum.NUMBER, PKQLEnum.GROUP_BY,
				PKQLEnum.COL_DEF, PKQLEnum.MATH_PARAM};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.MATH_FUN;
	}
	
	/**
	 * This will generate the process script for the routine
	 * @param tableName			The name of the R datatable
	 * @param column			The column in the R datatable
	 * @return					The R-Script to execute
	 */
	public abstract String process(String tableName, String column);
	
	/**
	 * This will generate the process script when group bys are present for the routine
	 * @param tableName			The name of the R datatable
	 * @param column			The column in the R datatable	 
	 * @param groupByCols		The columns to group by in the R datatable
	 * @return					The R-Script to execute
	 */
	public abstract String processGroupBy(String tableName, String column, List<String> groupByCols);

	
	@Override
	public Iterator process() {
		modExpression();
		String nodeStr = myStore.get(whoAmI).toString();
		
		RDataTable rFrame = (RDataTable)myStore.get("G");
		String varRFrame = rFrame.getTableVarName();
		
		Vector<String> columns = (Vector <String>)myStore.get(PKQLEnum.COL_DEF);
		Vector<String> groupBys = (Vector <String>)myStore.get(PKQLEnum.COL_CSV);
		
		if(groupBys != null && !groupBys.isEmpty()){
			String sumScript = processGroupBy(varRFrame, columns.get(0), groupBys);
			REXP math = rFrame.executeRScript(sumScript);
			
			try {
				// note: the derived column comes back with header "V1"
				Map<String, Object> groups = (Map<String, Object>) math.asNativeJavaObject();
				double[] results = (double[]) groups.get("V1");

				// this is only here because this is what viz reactor expects
				// TODO: when we get job ids, will be very happy to get rid of this
				// annoying object
				HashMap<HashMap<Object,Object>,Object> groupByHash = new HashMap<HashMap<Object,Object>,Object>();
				
				// loop through and get everything
				for(int i = 0; i < results.length; i++) {
					HashMap<Object, Object> groupMap = new HashMap<Object, Object>();
					for(String groupByName : groupBys) {
						// all the values match by instance
						// grab the i-th index for each of the lists
						Object groupByValue = ((Object[]) groups.get(groupByName))[i];
						// store in the inner map
						groupMap.put(groupByName, groupByValue);
					}
					// now grab the i-th index of the results
					// and put it as the value in the final hash
					groupByHash.put(groupMap, results[i]);
				}
				
				myStore.put(nodeStr, groupByHash);
				myStore.put("STATUS",STATUS.SUCCESS);

			} catch (REXPMismatchException e) {
				e.printStackTrace();
			}
		} else {
			String sumScript = process(varRFrame, columns.get(0));
			REXP rSum = rFrame.executeRScript(sumScript);
			double sum = 0;
			try {
				sum = rSum.asDouble();
			} catch (REXPMismatchException e) {
				e.printStackTrace();
			}
			
			myStore.put(nodeStr, sum);
			myStore.put("STATUS",STATUS.SUCCESS);
		}
		
		return null;
	}
}
