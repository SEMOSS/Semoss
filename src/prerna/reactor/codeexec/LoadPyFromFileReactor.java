package prerna.reactor.codeexec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
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
		// this also validates
		String filePath = UploadInputUtility.getFilePath(this.store, this.insight);
		String alias = keyValue.get(keysToGet[1]);

		try {
			String script = alias + " = smssutil.load_module_from_file(module_name='" + alias + "', file_path='" + filePath +"')";
			this.insight.getPyTranslator().runScript(script);
			return new NounMetadata("Variable set " + alias, PixelDataType.CONST_STRING);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Unable to load python file as module");
		}
	}
}
