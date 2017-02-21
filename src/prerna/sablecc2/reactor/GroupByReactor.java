package prerna.sablecc2.reactor;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import cern.colt.Arrays;
import prerna.ds.h2.H2Frame;
import prerna.engine.api.IScriptReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.expressions.IExpressionSelector;
import prerna.sablecc.expressions.sql.H2SqlExpressionIterator;
import prerna.sablecc.expressions.sql.builder.SqlColumnSelector;
import prerna.sablecc.expressions.sql.builder.SqlExpressionBuilder;
import prerna.sablecc2.om.GenRowStruct;

public class GroupByReactor extends AbstractReactor {

	
	@Override
	public void In() {
		this.defaultOutputAlias = new String[] {"EXPR"};
		curNoun("all");
	}

	@Override
	public Object Out() {
		IScriptReactor expression = addGroupBy();
		String key = (String)expression.getValue(PKQLEnum.MATH_FUN);
		expression.process();
		SqlExpressionBuilder builder = (SqlExpressionBuilder)expression.getValue(key);
		printOutput(builder, (H2Frame)this.planner.getFrame());
		if(parentReactor != null) {
			
		}
		return null;
	}

	@Override
	protected void mergeUp() {
		if(parentReactor != null) {
			
		}
	}

	@Override
	protected void updatePlan() {
		addOutputLinksToPlanner();
		storeOutputsToPlanner(addGroupBy());
	}
		
	//upload the outputs to the planner
	private void storeOutputsToPlanner(Object exprOutput) {
		Hashtable<String, Object> outputStore = new Hashtable<>();
		outputStore.put("expr", exprOutput);
		this.planner.addProperty(asName[0], "STORE", outputStore);
	}
	
	//Todo make this generic such that it will work with other non sql frames
	private IScriptReactor addGroupBy() {
		
		//get the alias name
		GenRowStruct operation = store.getNoun("op");
		String aliasName = (String)operation.get(0); //we only expect one operation here
		
		//get the object associated with the alias from the planner
		Map<String, Object> map = (Map<String, Object>)this.planner.getProperty(aliasName, "STORE");
		
		
		//get the group by columns
		GenRowStruct groupBy = store.getNoun("groupBy");
		List<String> groupByColumns = (Vector<String>)groupBy.getAllColumns();
		
		
		Object expression = map.get("expr"); //expr should be an input
		if(expression instanceof IScriptReactor) {
			((IScriptReactor)expression).put(PKQLEnum.COL_CSV, groupByColumns);
		}
		
		return (IScriptReactor)expression;
	}
	
	private void printOutput(SqlExpressionBuilder builder, H2Frame frame) {
		
		// need to have a bifurcation when the expression is just a single scalar value
		if(builder.isScalar()) {
			System.out.println("result: "+builder.getScalarValue());
		} else {
			// we add some selectors to make sure this output is more intuitive
			List<String> groups = builder.getGroupByColumns();
			if(groups == null || groups.isEmpty()) {
				// if no groups
				// use the existing columns to join on
				List<String> joinCols = builder.getAllTableColumnsUsed();
				for(String joinCol : joinCols) {
					SqlColumnSelector selector = new SqlColumnSelector(frame, joinCol);
					builder.addSelector(selector);
				}
			} else {
				// use the group columns to join on
				for(String group : groups) {
					SqlColumnSelector selector = new SqlColumnSelector(frame, group);
					builder.addSelector(selector);
				}
			}
			
			StringBuilder retStringBuilder = new StringBuilder();
			H2SqlExpressionIterator it = new H2SqlExpressionIterator(builder);
			List<Object[]> first500 = new Vector<Object[]>();
			first500.add(builder.getSelectorNames().toArray());
			int counter = 0;
			while(it.hasNext() && counter < 500) {
				first500.add(it.next());
				counter++;
			}
			// if we only show a subset
			if(counter == 500) {
				retStringBuilder.append("Only showing first 500 rows\n");
				it.close();
			}
			
			String retResponse = getStringFromList( first500, retStringBuilder);
			System.out.println(retResponse);
		}
	}
	
	private String getStringFromList(List objList, StringBuilder builder) {
		for(int i = 0; i < objList.size(); i++) {
			Object obj = objList.get(i);
			if(obj instanceof Object[]) {
				getStringFromObjectArray( (Object[]) obj, builder);
			} else if(obj instanceof double[]) {
				builder.append(Arrays.toString((double[]) obj));
			} else if(obj instanceof int[]) {
				builder.append(Arrays.toString((int[]) obj));
			} else {
				builder.append(obj);
			}
			builder.append("\n");
		}
		
		return builder.toString();
	}
	
	private String getStringFromObjectArray(Object[] objArray, StringBuilder builder) {
		builder.append("[");
		for(int i = 0; i < objArray.length; i++) {
			Object obj = objArray[i];
			if(obj instanceof Object[]) {
				getStringFromObjectArray((Object[]) obj, builder);
			} else {
				if(i == objArray.length-1) {
					if(obj instanceof double[]) {
						builder.append(Arrays.toString((double[]) obj));
					} else if(obj instanceof int[]) {
						builder.append(Arrays.toString((int[]) obj));
					} else {
						builder.append(obj);
					}
				} else {
					// since primitive arrays are stupid in java
					if(obj instanceof double[]) {
						builder.append(Arrays.toString((double[]) obj)).append(", ");
						builder.append("\n");
					} else if(obj instanceof int[]) {
						builder.append(Arrays.toString((int[]) obj)).append(", ");
						builder.append("\n");
					} else {
						builder.append(obj).append(", ");
					}
				}
			}
		}
		builder.append("]");
		
		return builder.toString();
	}
}
