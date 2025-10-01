package prerna.reactor;

import com.github.f4b6a3.uuid.alt.GUID;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class UUIDReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		return new NounMetadata(GUID.v7().toUUID().toString(), PixelDataType.CONST_STRING);
	}

}
