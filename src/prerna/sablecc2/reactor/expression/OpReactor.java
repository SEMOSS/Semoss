package prerna.sablecc2.reactor.expression;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.IReactor;

public abstract class OpReactor extends AbstractReactor {

	/*
	 * The specific operation would only need to override
	 * what the execute function does
	 */
	
	@Override
	public void In() {
		curNoun("all");
	}

	@Override
	public Object Out() {
		return parentReactor;
	}

	/**
	 * Merge the curRow from the expr reactor
	 * which generated this operation
	 * @param grs
	 */
	public void mergeCurRow(GenRowStruct grs) {
		this.curRow.merge(grs);
	}
	
	/**
	 * Flush out the gen row into the values
	 * Takes into consideration the lambdas that still need to be executed
	 * @return
	 */
	public Object[] getValues() {
		int numVals = curRow.size();
		Object[] retValues = new Object[numVals];
		
		for(int cIndex = 0; cIndex < numVals; cIndex++) {
			Object value = curRow.get(cIndex);
			if(value instanceof IReactor) {
				NounMetadata nounOutput = ((IReactor) value).execute();
				retValues[cIndex] = nounOutput.getValue();
			}
			// CURRENTLY, THE SQL EXPRESSION WOULD HAVE ALREADY BEEN
			// EVALUATED SO THIS IS NOT NECESSARY
//			else if(value instanceof SqlExpressionBuilder) {
//				SqlExpressionBuilder builder = (SqlExpressionBuilder) value;
//				if(builder.isScalar()) {
//					retValues[cIndex] = builder.getScalarValue();
//				} else {
//					// what to do in this case....
//					retValues[cIndex] = null;
//				}
//			}
			else {
				retValues[cIndex] = value;
			}
		}
		
		return retValues;
	}
}
