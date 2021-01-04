package prerna.sablecc2.reactor.frame;

import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetFrameMetamodelReactor extends AbstractFrameReactor {
	
	/*
	 * PAYLOAD MUST MATCH THAT OF 
	 * {@link prerna.sablecc2.reactor.masterdatabase.GetDatabaseTableStructureReactor}
	 */
	
	private static final String CLASS_NAME = GetFrameMetamodelReactor.class.getName();
	
	public GetFrameMetamodelReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FRAME.getKey()};
	}

	@Override
	public NounMetadata execute() {
		ITableDataFrame frame = getFrame();
		Map<String, Object> metamodelObject = frame.getMetaData().getMetamodel();
		return new NounMetadata(metamodelObject, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.FRAME_METAMODEL);
 
//		this.organizeKeys();
//		String engineId = this.keyValue.get(this.keysToGet[0]);
//		if(engineId == null) {
//			throw new IllegalArgumentException("Need to define the database to get the structure from from");
//		}
//		engineId = MasterDatabaseUtility.testEngineIdIfAlias(engineId);
//		
//		// account for security
//		// TODO: THIS WILL NEED TO ACCOUNT FOR COLUMNS AS WELL!!!
//		if(AbstractSecurityUtils.securityEnabled()) {
//			if(!SecurityAppUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
//				throw new IllegalArgumentException("Database does not exist or user does not have access to database");
//			}
//		}
//
//		Logger logger = getLogger(CLASS_NAME);
//		logger.info("Pulling database structure for app " + engineId);
//		// if cache exists, return from there
//		List<Object[]> data = EngineSyncUtility.getDatabaseStructureCache(engineId);
//		if(data == null) {
//			data = MasterDatabaseUtility.getAllTablesAndColumns(engineId);
//			// store the cache for the database structure
//			EngineSyncUtility.setDatabaseStructureCache(engineId, data);
//		}
//		return new NounMetadata(data, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_TABLE_STRUCTURE);
	}
}
