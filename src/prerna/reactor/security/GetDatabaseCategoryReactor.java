package prerna.reactor.security;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.sql.DatabaseCategoryEnum;

/**
 * Reactor that determines the category (SQL or NoSQL) of a database engine
 * Takes an engine ID and returns the database category
 */
public class GetDatabaseCategoryReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetDatabaseCategoryReactor.class);

	public GetDatabaseCategoryReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		DatabaseCategoryEnum category = DatabaseCategoryEnum.UNKNOWN;

		GetEngineMetadataReactor metadataReactor = new GetEngineMetadataReactor();
		metadataReactor.setInsight(this.insight);
		metadataReactor.setNounStore(this.store);
		NounMetadata engineMetadataResult = metadataReactor.execute();
		if (engineMetadataResult != null && engineMetadataResult.getValue() instanceof Map) {
			Map<String, Object> engineMetadata = (Map<String, Object>) engineMetadataResult.getValue();
			Object rdbmsType = engineMetadata.get("database_subtype");
			classLogger.info("rdbms type: {}", rdbmsType);
			if (rdbmsType != null) {
				category = DatabaseCategoryEnum.getCategoryFromRdbmsType(rdbmsType.toString());
			}
		}

		return new NounMetadata(category.getCategoryName(), PixelDataType.CONST_STRING, PixelOperationType.ENGINE_INFO);
	}

}