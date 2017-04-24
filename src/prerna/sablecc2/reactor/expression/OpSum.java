package prerna.sablecc2.reactor.expression;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public class OpSum extends OpBasicMath {

	public OpSum() {
		this.operation = "sum";
	}
	
	@Override
	protected NounMetadata evaluate(Object[] values) {
		double sum = 0;
		
		for(Object val : values) {
			sum += ((Number) val).doubleValue();
		}
		NounMetadata sumNoun = new NounMetadata(sum, PkslDataTypes.CONST_DECIMAL);
		return sumNoun;
	}
}
