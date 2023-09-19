package prerna.sablecc2.reactor.export;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.upload.UploadInputUtility;

public class WriteObjectToFileReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(WriteObjectToFileReactor.class);
	
	public WriteObjectToFileReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.VALUE.getKey(), ReactorKeysEnum.ENCODED.getKey(), ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		String filePath = UploadInputUtility.getFilePath(this.store, this.insight);
		Object value = getObject();
		if(isEncoded()) {
			value = Utility.decodeURIComponent(value+"");
		}
		File f = new File(filePath);
		try {
			FileUtils.writeStringToFile(f, value.toString(), "UTF-8");
			return new NounMetadata(f.getName(), PixelDataType.CONST_STRING);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Unable to write object to file");
		}
	}
	
	/**
	 * 
	 * @return
	 */
	private Object getObject() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.VALUE.getKey());
		if(grs != null && !grs.isEmpty()) {
			return grs.get(0);
		}
		
		if(!this.curRow.isEmpty()) {
			return this.curRow.get(0);
		}
		
		throw new NullPointerException("Must define the object to write to file");
	}

	/**
	 * 
	 * @return
	 */
	private boolean isEncoded() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.ENCODED.getKey());
		if(grs != null && !grs.isEmpty()) {
			return (Boolean) grs.get(0);
		}
		
		return false;
	}
	
}
