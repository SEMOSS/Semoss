package prerna.sablecc2.reactor.expression;

import java.util.List;
import java.util.Vector;

import prerna.engine.api.IScriptReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.expressions.sql.builder.SqlExpressionBuilder;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.AbstractReactor;

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
	
	public NounMetadata execute() {
		Object expressionObj = buildExpression();
		if(expressionObj instanceof SqlExpressionBuilder) {
			SqlExpressionBuilder builder = (SqlExpressionBuilder) expressionObj;
			NounMetadata noun;
			if(builder.isScalar()) {
				noun = new NounMetadata(builder.getScalarValue(), PkslDataTypes.CONST_DECIMAL);
			} else {
				noun = new NounMetadata(builder, PkslDataTypes.SQLE);
			}
			this.output = noun;
			return noun;
		} else if(expressionObj instanceof NounMetadata){
			this.output = (NounMetadata) expressionObj;
			return (NounMetadata) expressionObj;
		} else {
			return null;
		}
	}
	
	private Object buildExpression() {
		// if we have columns
		// then this has to be some kind of expression on
		// and existing engine or a frame
		// if there are no columns
		// then it is a math operation to execute
		// outside of the engine/frame data structur
		List<String> columns = this.getCurRow().getAllColumns();
		if(columns != null && !columns.isEmpty()) {
			// TODO: stop assuming this is sql...
			return buildSQLExpression();
		} else {
			// okay, we have some kind of math routine to execute
			// that is some kind of function working as a 
			// operational formula
			System.out.println("this should be the max");
			OpReactor reactor = OpFactory.getOp(this.operationName, this.curRow);
			Object testExecution = reactor.execute();
			NounMetadata retNoun = new NounMetadata(reactor, PkslDataTypes.LAMBDA);
			return retNoun;
		}
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
		if(outputs != null) {
			return outputs;
		}
		
		outputs = new Vector<>();
		outputs.add(output);
		return outputs;
	}

}
