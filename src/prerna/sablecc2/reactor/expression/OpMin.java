package prerna.sablecc2.reactor.expression;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public class OpMin extends OpReactor{

	@Override
	public NounMetadata execute() {
		double minValue = Double.MAX_VALUE;
		
		// get the values
		// this evaluated any lambda that 
		// was stored in currow
		Object[] values = getValues();
		for(int i = 0; i < values.length; i++) {
			Object val = values[i];
			if(val instanceof Number) {
				minValue = performComp(minValue, ((Number) val).doubleValue()); 
			} else {
				throw new IllegalArgumentException("Invalid input for Max. Require all values to be numeric");
			}
		}
		NounMetadata maxNoun = new NounMetadata(minValue, PkslDataTypes.CONST_DECIMAL);
		return maxNoun;
	}
	
	/**
	 * Do a basic math comparison
	 * @param curMax
	 * @param newMax
	 * @return
	 */
	public double performComp(double curMin, double newMin) {
		if(curMin < newMin) {
			return curMin;
		} else {
			return newMin;
		}
	}
	
}
