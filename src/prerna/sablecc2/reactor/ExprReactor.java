package prerna.sablecc2.reactor;

import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

import cern.colt.Arrays;
import prerna.ds.h2.H2Frame;
import prerna.engine.api.IScriptReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.expressions.sql.H2SqlExpressionIterator;
import prerna.sablecc.expressions.sql.builder.SqlColumnSelector;
import prerna.sablecc.expressions.sql.builder.SqlExpressionBuilder;
import prerna.sablecc2.om.GenRowStruct;

public class ExprReactor extends AbstractReactor {

	public void In()
	{
		//why do we do this?
//		if(this.parentReactor != null && this.parentReactor.getName().equalsIgnoreCase("OPERATION_FORMULA"))
//		{
//			this.store = this.parentReactor.getNounStore();
//			curRow = store.getNoun("all");
//		}
//		else
		this.defaultOutputAlias = new String[]{"EXPR"};
		curNoun("all");
	}
	
//	public Object Out()
//	{
//		System.out.println("Calling the out of" + operationName);
//		System.out.println("Calling the out of " + reactorName);
//		// if the operation is fully sql-able etc
//		// and it is a map operation.. in this place it should merge with the parent
//		// there are 2 cases here
//		// a. I can assimilate with the parent and let it continue to rip
//		// b. I have to finish processing this before I give it off to parent
//		
//		// additionally.. if I do see a parent reactor.. I should add this as the input to the parent
//		// so that we know what order to execute it
//		
//		if(this.parentReactor != null && !this.parentReactor.getName().equalsIgnoreCase("OPERATION_FORMULA"))
//		{
//			// align to plan
//			// not sure I need this right now to add something to itself ?
//			// updatePlan();
//		}
//		if(this.type != IReactor.TYPE.REDUCE && this.store.isSQL() && this.parentReactor != null && !this.parentReactor.getName().equalsIgnoreCase("OPERATION_FORMULA")) 
//		{
//			// 2 more scenarios here
//			// if parent reactor is not null
//			// merge
//			// if not execute it
//			// if the whole thing is done through SQL, then just add the expression
//			if(this.parentReactor != null)
//			{
//				mergeUp();
//				return parentReactor;
//			}
//			// else assimilated with the other execute
///*			else
//			{
//				// execute it
//			}
//*/		
//		}
//		else if(parentReactor == null)
//		{
//			// execute it
//			mergeUp();
//		}
//		else if(parentReactor != null) {
//			mergeUp();
//			return parentReactor;
//		}
//		// else all the merging has already happened
//		return null;
//	}
	
	public Object Out() {
		//if we have a parent reactor...execute and push up
		//if we do not have a parent
		mergeUp();
		if(parentReactor != null) return parentReactor;
		return null;
	}

	@Override
	protected void mergeUp() {
		if(parentReactor != null ) {
			SqlExpressionBuilder builder = buildSQLExpression(buildReactor());
			parentReactor.setProp(this.signature, builder);
		} else {
			updatePlan(); //well no parent so just execute and push to the planner
		}
		
		printOutput(buildSQLExpression(buildReactor()), (H2Frame)planner.getFrame());
	}

	@Override
	protected void updatePlan() {
		addOutputLinksToPlanner();
		storeOutputsToPlanner(buildReactor());
	}
	
	//upload the outputs to the planner
	private void storeOutputsToPlanner(Object exprOutput) {
		Hashtable<String, Object> outputStore = new Hashtable<>();
		outputStore.put("expr", exprOutput);
		this.planner.addProperty(asName[0], "STORE", outputStore);
	}
	
	//TODO : put some comments
	private IScriptReactor buildReactor() {
		List<String> columns = this.getCurRow().getAllColumns();
		String reactorName = (String)this.getProp("REACTOR_NAME");
		
		try {
			IScriptReactor reactor = (IScriptReactor)Class.forName(reactorName).newInstance();
			
			reactor.put("G", planner.getFrame());
			if(this.getProp(this.signature) != null && this.getProp(this.signature) instanceof SqlExpressionBuilder) {
				reactor.put("TERM", this.getProp(this.signature));
			} else {
				reactor.put(PKQLEnum.COL_DEF, columns);
			}
			reactor.put(PKQLEnum.MATH_FUN, this.signature);
			return reactor;
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	private SqlExpressionBuilder buildSQLExpression(IScriptReactor reactor) {
		reactor.process();
		SqlExpressionBuilder builder = (SqlExpressionBuilder)reactor.getValue(this.signature);
		return builder;
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
