package prerna.reactor.database.physicaleditor;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.impl.util.Owler;
import prerna.reactor.AbstractReactor;
import prerna.reactor.masterdatabase.SyncDatabaseWithLocalMasterReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.EngineSyncUtility;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.DatabaseUpdateMetadata;

public class AddDatabaseStructureReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AddDatabaseStructureReactor.class);
	private static final String CLASS_NAME = AddDatabaseStructureReactor.class.getName();

	public AddDatabaseStructureReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.METAMODEL_ADDITIONS.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		Logger logger = getLogger(CLASS_NAME);

		String databaseId = this.keyValue.get(this.keysToGet[0]);
		databaseId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), databaseId);
		if(!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), databaseId)) {
			throw new IllegalArgumentException("Database" + databaseId + " does not exist or user does not have access to database");
		}
		
		// table > column > type
		Map<String, Map<String, String>> updates = getAdditions();
		
		IDatabaseEngine engine = Utility.getDatabase(databaseId);
		if(!(engine instanceof IRDBMSEngine)) {
			throw new IllegalArgumentException("This operation only works on relational databases");
		}
		ClusterUtil.pullOwl(databaseId);

		DatabaseUpdateMetadata dbUpdateMeta = AbstractSqlQueryUtil.performDatabaseAdditions((IRDBMSEngine) engine, updates, logger);
		Owler owler = dbUpdateMeta.getOwler();
		String errorMessages = dbUpdateMeta.getCombinedErrors();
		
		// now push the OWL and sync
		try {
			owler.export();
			SyncDatabaseWithLocalMasterReactor syncWithLocal = new SyncDatabaseWithLocalMasterReactor();
			syncWithLocal.setInsight(this.insight);
			syncWithLocal.setNounStore(this.store);
			syncWithLocal.In();
			syncWithLocal.execute();
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			NounMetadata noun = new NounMetadata(dbUpdateMeta, PixelDataType.BOOLEAN);
			noun.addAdditionalReturn(getError("Error occurred savig the metadata file with the executed changes"));
			if(!errorMessages.isEmpty()) {
				noun.addAdditionalReturn(getError(errorMessages));
			}
			return noun;
		}
		EngineSyncUtility.clearEngineCache(databaseId);
		ClusterUtil.pushOwl(databaseId);
		
		NounMetadata noun = new NounMetadata(dbUpdateMeta, PixelDataType.CUSTOM_DATA_STRUCTURE);
		if(errorMessages.length() > 0) {
			noun.addAdditionalReturn(getError(errorMessages.toString()));
		}
		
		return noun;
	}

	private Map<String, Map<String, String>> getAdditions() {
		GenRowStruct mapGrs = this.store.getNoun(ReactorKeysEnum.METAMODEL_ADDITIONS.getKey());
		if(mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if(mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, Map<String, String>>) mapInputs.get(0).getValue();
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if(mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, Map<String, String>>) mapInputs.get(0).getValue();
		}

		throw new IllegalArgumentException("Must define the map containing {tablename1:{columnname1:datatype, columnname2:datatype}} for the additions");
	}
	
}