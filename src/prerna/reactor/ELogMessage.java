package prerna.reactor;

import org.apache.logging.log4j.Logger;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class ELogMessage extends AbstractReactor{

	private static final String CLASS_NAME = ELogMessage.class.getName();
	
	public ELogMessage() {
		this.keysToGet = new String[] {ReactorKeysEnum.MESSAGE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String message = this.keyValue.get(this.keysToGet[0]);
		message = Utility.decodeURIComponent(message);
		Logger logger = getLogger(CLASS_NAME);
		logger.info(message);
		return new NounMetadata(message, PixelDataType.CONST_STRING);
	}

}
