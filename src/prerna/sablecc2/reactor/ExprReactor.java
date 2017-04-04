package prerna.sablecc2.reactor;

import java.util.List;
import java.util.Vector;

import prerna.engine.api.IScriptReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.expressions.sql.builder.SqlExpressionBuilder;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public class ExprReactor extends AbstractReactor {

	NounMetadata output;
	
	public void In()
	{
		this.defaultOutputAlias = new String[]{"EXPR"};
		curNoun("all");
	}
	
	public Object Out() {
		return parentReactor;
	}
	
	public Object execute() {
		SqlExpressionBuilder builder = buildSQLExpression();
		NounMetadata noun;
		if(builder.isScalar()) {
			noun = new NounMetadata(builder.getScalarValue(), PkslDataTypes.CONST_DECIMAL);
		} else {
			noun = new NounMetadata(builder, PkslDataTypes.SQLE);
		}
		output = noun;
		return noun;
	}

	private SqlExpressionBuilder buildSQLExpression() {
		List<String> columns = this.getCurRow().getAllColumns();
		String reactorName = (String)this.getProp("REACTOR_NAME");
		
		IScriptReactor reactor = null;
		try {
			reactor = (IScriptReactor)Class.forName(reactorName).newInstance();
			
			reactor.put("G", planner.getFrame());
			if(this.getProp(this.signature) != null && this.getProp(this.signature) instanceof SqlExpressionBuilder) {
				reactor.put("TERM", this.getProp(this.signature));
			} else {
				reactor.put(PKQLEnum.COL_DEF, columns);
			}
			reactor.put(PKQLEnum.MATH_FUN, this.signature);
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		reactor.process();
		SqlExpressionBuilder builder = (SqlExpressionBuilder)reactor.getValue(this.signature);
		return builder;
	}
	
	@Override
	public List<NounMetadata> getOutputs() {
		List<NounMetadata> outputs = super.getOutputs();
		if(outputs != null) return outputs;
		
		outputs = new Vector<>();
		outputs.add(output);
		return outputs;
	}

	
	
	// PKS LEGACY CODE... NOT SURE IF NEEDED/WHAT TO DO WITH IT
	
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
}
