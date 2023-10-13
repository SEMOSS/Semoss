package prerna.reactor;

import org.apache.logging.log4j.Logger;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class LogMessage extends AbstractReactor{

	private static final String CLASS_NAME = LogMessage.class.getName();
	
	public LogMessage() {
		this.keysToGet = new String[] {ReactorKeysEnum.MESSAGE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String message = this.keyValue.get(this.keysToGet[0]);
		Logger logger = getLogger(CLASS_NAME);
		logger.info(message);
		return new NounMetadata(message, PixelDataType.CONST_STRING);
	}

}
