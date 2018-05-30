package prerna.sablecc2.reactor.utils;

import java.io.File;
import java.util.UUID;

import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.ZipDatabase;

public class ExportAppReactor extends AbstractReactor {

	private static final String CLASS_NAME = ExportAppReactor.class.getName();
	
	public ExportAppReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.APP.getKey() };
	}
	
	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		engineId = MasterDatabaseUtility.testEngineIdIfAlias(engineId);
		File zip = null;
		try {
			logger.info("Exporting Database Now... ");
			logger.info("Stopping the engine ... ");
			// remove the app
			IEngine engine = Utility.getEngine(engineId);
			engine.closeDB();
			zip = ZipDatabase.zipEngine(engineId, engine.getEngineName());			
			DIHelper.getInstance().removeLocalProperty(engineId);
			logger.info("Synchronize Database Complete");
		} finally {
			// open it back up
			logger.info("Opening the engine again ... ");
			Utility.getEngine(engineId);
		}
		
		// store it in the insight so the FE can download it
		// only from the given insight
		String randomKey = UUID.randomUUID().toString();
		this.insight.addExportFile(randomKey, zip.getAbsolutePath());
		return new NounMetadata(randomKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
	}

}
