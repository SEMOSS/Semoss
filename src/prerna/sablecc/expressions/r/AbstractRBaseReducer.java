package prerna.sablecc.expressions.r;

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

public abstract class AbstractRBaseReducer extends AbstractReactor {

	//TODO:
	//TODO:
	//TODO: need to add in the filters on the RFrame
	
	public AbstractRBaseReducer() {
		String[] thisReacts = { PKQLEnum.EXPR_TERM, PKQLEnum.DECIMAL, PKQLEnum.NUMBER, PKQLEnum.GROUP_BY, PKQLEnum.COL_DEF, PKQLEnum.MATH_PARAM};
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
		String nodeStr = myStore.get(whoAmI).toString();

		// modify the expression to get the sql syntax
		modExpression();
		
		String script = myStore.get("MOD_" + whoAmI).toString();
		script = script.replace("[", "").replace("]", "");
		
		RDataTable rFrame = (RDataTable)myStore.get("G");
		String varRFrame = rFrame.getTableVarName();
		
		Vector<String> groupBys = (Vector <String>)myStore.get(PKQLEnum.COL_CSV);

		if(groupBys != null && !groupBys.isEmpty()){
			String rScript = processGroupBy(varRFrame, script, groupBys);
			REXP math = rFrame.executeRScript(rScript);
			
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
						
						Object groupByValue = null;
						Object group = groups.get(groupByName);
						// ugh... need to do this casting
						if(group instanceof Object[]) {
							groupByValue = ((Object[]) groups.get(groupByName))[i];
						} else if(group instanceof double[]) {
							groupByValue = ((double[]) groups.get(groupByName))[i];
						} else if(group instanceof int[]) {
							groupByValue = ((int[]) groups.get(groupByName))[i];
						}
						
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
			String rScript = process(varRFrame, script);
			REXP math = rFrame.executeRScript(rScript);
			double result = 0;
			try {
				result = math.asDouble();
			} catch (REXPMismatchException e) {
				e.printStackTrace();
			}
			
			myStore.put(nodeStr, result);
			myStore.put("STATUS",STATUS.SUCCESS);
		}
		
		return null;
	}
}
