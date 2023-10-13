package prerna.reactor.expression;

import java.util.Collection;
import java.util.Map;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class OpIsObjectEmpty extends OpBasic {
	
	public OpIsObjectEmpty() {
		this.keysToGet = new String[]{ReactorKeysEnum.VALUES.getKey()};
	}

	@Override
	protected NounMetadata evaluate(Object[] values) {
		boolean isEmpty = true;
		if(values != null && values.length > 0) {
			Object input = values[0];
			if(input instanceof Collection) {
				isEmpty = ((Collection) input).isEmpty();
			} else if(input instanceof Map) {
				isEmpty = ((Map) input).isEmpty();
			}
		}
		return new NounMetadata(isEmpty, PixelDataType.BOOLEAN);
	}
	
	@Override
	public String getReturnType() {
		return "boolean";
	}

}