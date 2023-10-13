package prerna.reactor.expression;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class OpIsEmpty extends OpBasic {
	
	public OpIsEmpty() {
		this.keysToGet = new String[]{ReactorKeysEnum.VALUES.getKey()};
	}

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