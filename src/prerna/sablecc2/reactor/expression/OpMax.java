package prerna.sablecc2.reactor.expression;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public class OpMax extends OpReactor {

	@Override
	public NounMetadata execute() {
		double maxValue = Double.MIN_VALUE;
		
		// get the values
		// this evaluated any lambda that 
		// was stored in currow
		Object[] values = getValues();
		for(int i = 0; i < values.length; i++) {
			Object val = values[i];
			if(val instanceof Number) {
				maxValue = performComp(maxValue, ((Number) val).doubleValue()); 
			} else {
				throw new IllegalArgumentException("Invalid input for Max. Require all values to be numeric");
			}
		}
		NounMetadata maxNoun = new NounMetadata(maxValue, PkslDataTypes.CONST_DECIMAL);
		return maxNoun;
	}
	
	/**
	 * Do a basic math comparison
	 * @param curMax
	 * @param newMax
	 * @return
	 */
	public double performComp(double curMax, double newMax) {
		if(curMax > newMax) {
			return curMax;
		} else {
			return newMax;
		}
	}
	
}
