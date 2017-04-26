package prerna.sablecc2.reactor.expression;

import java.util.List;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.h2.H2Frame;
import prerna.sablecc.expressions.sql.H2SqlExpressionIterator;
import prerna.sablecc.expressions.sql.builder.SqlColumnSelector;
import prerna.sablecc.expressions.sql.builder.SqlExpressionBuilder;
import prerna.sablecc.expressions.sql.builder.SqlMathSelector;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.util.ArrayUtilityMethods;

public abstract class OpBasic extends OpReactor {

	protected String operation;
	protected boolean allIntValue = true;
	
	/*
	 * This class is to be extended for basic math operations
	 * To deal with string inputs that need to be evaluated
	 * on the frame
	 * 
	 * TODO: stop casting everything to h2frame
	 * 		make generic expression class that uses
	 * 		existing classes
	 */
	
	@Override
	public NounMetadata execute() {
		NounMetadata[] nouns = getValues();
		Object[] values = evaluateNouns(nouns);
		NounMetadata result = evaluate(values);
		return result;
	}
	
	protected abstract NounMetadata evaluate(Object[] values);
	
	protected Object[] evaluateNouns(NounMetadata[] nouns) {
		Object[] evaluatedNouns = new Object[nouns.length];
		for(int i = 0; i < nouns.length; i++) {
			NounMetadata val = nouns[i];
			evaluatedNouns[i] = evaluateNoun(val);
		}
		return evaluatedNouns;
	}
	
	protected Object evaluateNoun(NounMetadata val) {
		Object obj;
		PkslDataTypes valType = val.getNounName();
		if(valType == PkslDataTypes.CONST_DECIMAL) {
			this.allIntValue = false;
			obj = ((Number) val.getValue()).doubleValue();
		} else if(valType == PkslDataTypes.CONST_INT) {
			obj = ((Number) val.getValue()).intValue(); 
		} else if(valType == PkslDataTypes.VECTOR) {
			List<NounMetadata> nouns = (List<NounMetadata>)val.getValue();
			Object[] objArray = new Object[nouns.size()];
			for(int i = 0; i < nouns.size(); i++) {
				objArray[i] = evaluateNoun(nouns.get(i));
			}
			obj = objArray;
		} else {
			obj = val.getValue();
		}
		
		return obj;
	}
}
