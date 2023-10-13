package prerna.reactor.expression;


import java.util.List;
import java.util.Vector;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class OpList extends OpBasic {

	public OpList() {
		this.operation="list";
		this.keysToGet = new String[]{ReactorKeysEnum.ARRAY.getKey()};
	}

	@Override
	protected NounMetadata evaluate(Object[] values) {
		List<Object> list = new Vector<>(values.length);
		for(Object v : values) {
			list.add(v);
		}
		NounMetadata noun = new NounMetadata(list, PixelDataType.VECTOR);
		return noun;
	}

	@Override
	public String getReturnType() {
		return "List";
	}
	
}
