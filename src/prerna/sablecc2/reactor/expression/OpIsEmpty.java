package prerna.sablecc2.reactor.expression;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;

public class OpIsEmpty extends OpBasic {

	@Override
	protected NounMetadata evaluate(Object[] values) {
		boolean isEmpty = true;
		if(values != null && values.length > 0) {
			isEmpty = false;
		}
		return new NounMetadata(isEmpty, PixelDataType.BOOLEAN);
	}
	
	@Override
	public String getReturnType() {
		return "boolean";
	}

}