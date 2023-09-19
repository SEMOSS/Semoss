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
import prerna.util.upload.UploadInputUtility;

public class LoadPyFromFileReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(LoadPyFromFileReactor.class);
	
	public LoadPyFromFileReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.ALIAS.getKey(), ReactorKeysEnum.SPACE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String filePath = UploadInputUtility.getFilePath(this.store, this.insight);
		String alias = keyValue.get(keysToGet[1]);
		
		File f = new File(filePath);
		try {
			String script = alias + " = smssutil.load_module_from_file(module_name='" + alias + "', file_path='" + filePath +"')";
			this.insight.getPyTranslator().runScript(script);
			return new NounMetadata("Variable set " + alias, PixelDataType.CONST_STRING);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Unable to write object to file");
		}
	}
}
