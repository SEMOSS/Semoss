package prerna.sablecc2.reactor.expression;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public class OpMin extends OpBasicMath {

	@Override
	public NounMetadata execute() {
		double minValue = Double.MAX_VALUE;
		
		// get the values
		// this evaluated any lambda that 
		// was stored in currow
		NounMetadata[] values = getValues();
		for(int i = 0; i < values.length; i++) {
			NounMetadata val = values[i];
			PkslDataTypes valType = val.getNounName();
			if(valType == PkslDataTypes.CONST_DECIMAL) {
				minValue = performComp(minValue, ((Number) val.getValue()).doubleValue()); 
			} else if(valType == PkslDataTypes.COLUMN) {
				// at this point, we have already checked if this is a 
				// variable, so it better exist on the frame
				// also, you can only have one of these
				minValue = performComp(minValue, evaluateString("min", val)); 
			} else {
				throw new IllegalArgumentException("Invalid input for Min. Require all values to be numeric or column names");
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
