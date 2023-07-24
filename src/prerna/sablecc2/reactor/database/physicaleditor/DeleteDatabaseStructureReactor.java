package prerna.sablecc2.reactor.database.physicaleditor;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IDatabase;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.impl.util.Owler;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.masterdatabase.SyncDatabaseWithLocalMasterReactor;
import prerna.util.Constants;
import prerna.util.EngineSyncUtility;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.DatabaseUpdateMetadata;

public class DeleteDatabaseStructureReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(DeleteDatabaseStructureReactor.class);
	private static final String CLASS_NAME = DeleteDatabaseStructureReactor.class.getName();

	public DeleteDatabaseStructureReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.METAMODEL_DELETIONS.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		Logger logger = getLogger(CLASS_NAME);
		
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		if(AbstractSecurityUtils.securityEnabled()) {
			databaseId = SecurityQueryUtils.testUserDatabaseIdForAlias(this.insight.getUser(), databaseId);
			if(!SecurityEngineUtils.userCanEditDatabase(this.insight.getUser(), databaseId)) {
				throw new IllegalArgumentException("Database" + databaseId + " does not exist or user does not have access to database");
			}
		} else {
			databaseId = MasterDatabaseUtility.testDatabaseIdIfAlias(databaseId);
			if(!MasterDatabaseUtility.getAllDatabaseIds().contains(databaseId)) {
				throw new IllegalArgumentException("Database " + databaseId + " does not exist");
			}
		}
		
		// table > [column1,column2,...]
		Map<String, List<String>> updates = getDeletions();
		
		IDatabase engine = Utility.getDatabase(databaseId);
		if(!(engine instanceof IRDBMSEngine)) {
			throw new IllegalArgumentException("This operation only works on relational databases");
		}
		ClusterUtil.reactorPullOwl(databaseId);
		
		DatabaseUpdateMetadata dbUpdateMeta = AbstractSqlQueryUtil.performDatabaseDeletions((IRDBMSEngine) engine, updates, logger);
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
			noun.addAdditionalReturn(getError("Error occurred saving the metadata file with the executed changes"));
			if(!errorMessages.isEmpty()) {
				noun.addAdditionalReturn(getError(errorMessages));
			}
			return noun;
		}
		EngineSyncUtility.clearEngineCache(databaseId);
		ClusterUtil.reactorPushOwl(databaseId);
		
		NounMetadata noun = new NounMetadata(dbUpdateMeta, PixelDataType.CUSTOM_DATA_STRUCTURE);
		if(errorMessages.length() > 0) {
			noun.addAdditionalReturn(getError(errorMessages.toString()));
		}
		
		return noun;
	}
	
	private Map<String, List<String>> getDeletions() {
		GenRowStruct mapGrs = this.store.getNoun(ReactorKeysEnum.METAMODEL_DELETIONS.getKey());
		if(mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if(mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, List<String>>) mapInputs.get(0).getValue();
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if(mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, List<String>>) mapInputs.get(0).getValue();
		}

		throw new IllegalArgumentException("Must define the map containing {tablename:[column1name, column2name, ...]} for the deletions");
	}
}
