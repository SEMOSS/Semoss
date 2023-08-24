package prerna.sablecc2.reactor.masterdatabase;

import java.util.List;

import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.EngineSyncUtility;

public class GetDatabaseTableStructureReactor extends AbstractReactor {
	
	/*
	 * PAYLOAD MUST MATCH THAT OF 
	 * {@link prerna.sablecc2.reactor.frame.GetFrameTableStructureReactor}
	 */
	
	private static final String CLASS_NAME = GetDatabaseTableStructureReactor.class.getName();
	
	public GetDatabaseTableStructureReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if(engineId == null) {
			throw new IllegalArgumentException("Need to define the database to get the structure from from");
		}
		engineId = MasterDatabaseUtility.testDatabaseIdIfAlias(engineId);
		
		// account for security
		// TODO: THIS WILL NEED TO ACCOUNT FOR COLUMNS AS WELL!!!
		if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Database does not exist or user does not have access to database");
		}

		Logger logger = getLogger(CLASS_NAME);
		logger.info("Pulling database structure for database " + engineId);
		// if cache exists, return from there
		List<Object[]> data = EngineSyncUtility.getDatabaseStructureCache(engineId);
		if(data == null) {
			data = MasterDatabaseUtility.getAllTablesAndColumns(engineId);
			// store the cache for the database structure
			EngineSyncUtility.setDatabaseStructureCache(engineId, data);
		}
		return new NounMetadata(data, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_TABLE_STRUCTURE);
	}
}
