package prerna.sablecc2.reactor.expression;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;

public class OpNotEmpty extends OpBasic {

	@Override
	protected NounMetadata evaluate(Object[] values) {
		boolean notEmpty = false;
		if(values != null && values.length > 0) {
			notEmpty = true;
		}
		return new NounMetadata(notEmpty, PixelDataType.BOOLEAN);
	}
	
	@Override
	public String getReturnType() {
		return "boolean";
	}

}
