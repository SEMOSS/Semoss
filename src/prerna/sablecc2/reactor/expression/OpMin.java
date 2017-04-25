package prerna.sablecc2.reactor.expression;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public class OpMin extends OpBasicMath {

	public OpMin() {
		this.operation = "min";
	}
	
	@Override
	protected NounMetadata evaluate(Object[] values) {
		double min = Double.MAX_VALUE;
		
		for(Object val : values) {
			Double nextNumber = ((Number) val).doubleValue();
			min = min > nextNumber ? nextNumber : min;
		}
		
		NounMetadata minNoun = null;
		if(this.allIntValue) {
			minNoun = new NounMetadata((int) min, PkslDataTypes.CONST_INT);
		} else {
			minNoun = new NounMetadata(min, PkslDataTypes.CONST_DECIMAL);
		}
		return minNoun;
	}	
}
