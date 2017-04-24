package prerna.sablecc2.reactor.expression;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public class OpMean extends OpBasicMath {

	public OpMean() {
		this.operation = "avg";
	}
	
	protected NounMetadata evaluate(Object[] values) {
		double sum = 0;
		for(Object val : values) {
			sum += ((Number) val).doubleValue();
		}
		double mean = sum/values.length;
		NounMetadata maxNoun = new NounMetadata(mean, PkslDataTypes.CONST_DECIMAL);
		return maxNoun;
	}
}