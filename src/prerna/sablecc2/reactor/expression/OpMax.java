package prerna.sablecc2.reactor.expression;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public class OpMax extends OpBasicMath {

	@Override
	public NounMetadata execute() {
		double maxValue = -1.0 * Double.MAX_VALUE;
		
		// get the values
		// this evaluated any lambda that 
		// was stored in currow
		NounMetadata[] values = getValues();
		for(int i = 0; i < values.length; i++) {
			NounMetadata val = values[i];
			PkslDataTypes valType = val.getNounName();
			if(valType == PkslDataTypes.CONST_DECIMAL) {
				maxValue = performComp(maxValue, ((Number) val.getValue()).doubleValue()); 
			} else if(valType == PkslDataTypes.COLUMN) {
				// at this point, we have already checked if this is a 
				// variable, so it better exist on the frame
				// also, you can only have one of these
				maxValue = performComp(maxValue, evaluateString("max", val)); 
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
		if(curMax < newMax) {
			return newMax;
		} else {
			return curMax;
		}
	}
	
}
