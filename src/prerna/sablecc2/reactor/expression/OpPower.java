package prerna.sablecc2.reactor.expression;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public class OpPower extends OpBasicMath {

	@Override
	public NounMetadata execute() {
		// get the values
		// this evaluated any lambda that 
		// was stored in currow
		NounMetadata[] values = getValues();

		if(values.length != 2) {
			throw new IllegalArgumentException("Invalid input for Power. Must have exactly two inputs: first input is the number and the second input is the power to raise that number.");
		}
		
		// we need to calculate all of the values
		// and then order them
		// and then find the middle one
		double[] evals = new double[values.length];

		for(int i = 0; i < values.length; i++) {
			NounMetadata val = values[i];
			PkslDataTypes valType = val.getNounName();
			if(valType == PkslDataTypes.CONST_DECIMAL) {
				evals[i] = ((Number) val.getValue()).doubleValue(); 
			} else if(valType == PkslDataTypes.COLUMN) {
				// TODO: need to figure out what this would do
				// should only enter here if we have a column
				// which is from the frame
				// at which point, should i return an iterator?
				
				throw new IllegalArgumentException("Invalid input for Power. Cannot handle frame columns at this time.");
			} else {
				throw new IllegalArgumentException("Invalid input for Power. Require all values to be numeric at this time.");
			}
		}
		
		double powerVal = Math.pow(evals[0], evals[1]);
		NounMetadata medianNoun = new NounMetadata(powerVal, PkslDataTypes.CONST_DECIMAL);
		return medianNoun;
	}
}
