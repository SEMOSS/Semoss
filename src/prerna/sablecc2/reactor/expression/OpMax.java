package prerna.sablecc2.reactor.expression;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public class OpMax extends OpBasicMath {	
	
	public OpMax() {
		this.operation = "max";
	}
	
	@Override
	protected NounMetadata evaluate(Object[] values) {
		double max = -1.0 * Double.MAX_VALUE;
		
		for(Object val : values) {
			Double nextDouble = ((Number) val).doubleValue();
			max = max < nextDouble ? nextDouble : max;
		}
		NounMetadata maxNoun = new NounMetadata(max, PkslDataTypes.CONST_DECIMAL);
		return maxNoun;
	}	
}
