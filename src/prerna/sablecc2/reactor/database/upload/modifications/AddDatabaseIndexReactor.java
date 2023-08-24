package prerna.sablecc2.reactor.database.upload.modifications;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IEngineModifier;
import prerna.engine.impl.modifications.EngineModificationFactory;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.database.metaeditor.properties.RemoveOwlPropertyReactor;
import prerna.util.Utility;

public class AddDatabaseIndexReactor extends AbstractReactor {

	private static final String FORCE_INDEX = "force";
	
	public AddDatabaseIndexReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(),
				ReactorKeysEnum.CONCEPT.getKey(),
				ReactorKeysEnum.COLUMN.getKey(),
				ReactorKeysEnum.CREATE_INDEX.getKey(),
				FORCE_INDEX
		};
		this.keyRequired = new int[]{1, 1, 1, 0};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		// we may have the alias
		databaseId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), databaseId);
		if(!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), databaseId)) {
			throw new IllegalArgumentException("Database" + databaseId + " does not exist or user does not have access to database");
		}
		
		String table = this.keyValue.get(this.keysToGet[1]);
		String column = this.keyValue.get(this.keysToGet[2]);
		String indexName = this.keyValue.get(this.keysToGet[3]);
		if(indexName == null) {
			indexName = table.toUpperCase() + "_" + column.toUpperCase() + "_INDEX";
		}
		boolean forceIndex = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[4]));
		
		IDatabaseEngine database = Utility.getDatabase(databaseId);
		IEngineModifier modifier = EngineModificationFactory.getEngineModifier(database);
		if(modifier == null) {
			throw new IllegalArgumentException("This type of data modification has not been implemented for this database type");
		}
		try {
			modifier.addIndex(table, column, indexName, forceIndex);
		} catch (Exception e) {
			// an error occurred here, so we need to delete from the OWL
			try {
				RemoveOwlPropertyReactor owlRemover = new RemoveOwlPropertyReactor();
				owlRemover.setInsight(this.insight);
				owlRemover.setNounStore(this.store);
				owlRemover.execute();
			} catch(Exception e2) {
				e2.printStackTrace();
			}
			
			throw new IllegalArgumentException("Error occurred to alter the table. Error returned from driver: " + e.getMessage(), e);
		}
		
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully added index"));
		return noun;
	}
	
}
