package prerna.sablecc2.reactor.expression;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public class OpSum extends OpBasicMath {

	@Override
	public NounMetadata execute() {
		double sum = 0;
		
		// get the values
		// this evaluated any lambda that 
		// was stored in currow
		NounMetadata[] values = getValues();
		for(int i = 0; i < values.length; i++) {
			NounMetadata val = values[i];
			PkslDataTypes valType = val.getNounName();
			if(valType == PkslDataTypes.CONST_DECIMAL) {
				sum += ((Number) val.getValue()).doubleValue(); 
			} else if(valType == PkslDataTypes.COLUMN) {
				// at this point, we have already checked if this is a 
				// variable, so it better exist on the frame
				// also, you can only have one of these
				sum += evaluateString("sum", val);
			} else {
				throw new IllegalArgumentException("Invalid input for Sum. Require all values to be numeric or column names");
			}
		}
		NounMetadata maxNoun = new NounMetadata(sum, PkslDataTypes.CONST_DECIMAL);
		return maxNoun;
	}
}
