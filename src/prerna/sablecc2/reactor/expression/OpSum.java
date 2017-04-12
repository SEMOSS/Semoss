package prerna.sablecc2.reactor.expression;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public class OpSum extends OpReactor {

	@Override
	public NounMetadata execute() {
		double sum = 0;
		
		// get the values
		// this evaluated any lambda that 
		// was stored in currow
		Object[] values = getValues();
		for(int i = 0; i < values.length; i++) {
			Object val = values[i];
			if(val instanceof Number) {
				sum += ((Number) val).doubleValue(); 
			} else {
				throw new IllegalArgumentException("Invalid input for Sum. Require all values to be numeric");
			}
		}
		NounMetadata maxNoun = new NounMetadata(sum, PkslDataTypes.CONST_DECIMAL);
		return maxNoun;
	}
}
