package prerna.reactor;

import java.util.UUID;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class UUIDReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		return new NounMetadata(UUID.randomUUID().toString(), PixelDataType.CONST_STRING);
	}

}
