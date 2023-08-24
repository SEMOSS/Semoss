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
import prerna.sablecc2.reactor.database.metaeditor.properties.AddOwlPropertyReactor;
import prerna.sablecc2.reactor.database.metaeditor.properties.RemoveOwlPropertyReactor;
import prerna.util.Utility;

public class RemoveDatabaseColumnReactor extends AbstractReactor {

	public RemoveDatabaseColumnReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(),
				ReactorKeysEnum.CONCEPT.getKey(),
				ReactorKeysEnum.COLUMN.getKey(), 
				ReactorKeysEnum.DATA_TYPE.getKey()
		};
		this.keyRequired = new int[]{1, 1, 1, 1};
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		String databaseId = this.keyValue.get(this.keysToGet[0]);
		// we may have the alias
		databaseId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), databaseId);
		if(!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), databaseId)) {
			throw new IllegalArgumentException("Database " + databaseId + " does not exist or user does not have access to database");
		}

		String table = this.keyValue.get(this.keysToGet[1]);
		String column = this.keyValue.get(this.keysToGet[2]);

		// we need to store the values existing in the OWL in case 
		// something goes wrong
		IDatabaseEngine database = Utility.getDatabase(databaseId);
		
		
		// update the owl for any database
		// we will just update the 
		RemoveOwlPropertyReactor owlUpdater = new RemoveOwlPropertyReactor();
		owlUpdater.setInsight(this.insight);
		owlUpdater.setNounStore(this.store);
		owlUpdater.execute();

		IEngineModifier modifier = EngineModificationFactory.getEngineModifier(database);
		if(modifier == null) {
			throw new IllegalArgumentException("This type of data modification has not been implemented for this database type");
		}
		try {
			modifier.removeProperty(table, column);
		} catch (Exception e) {
			// an error occurred here, so we need to revert our change from the OWL
			try {
				AddOwlPropertyReactor owlAdder = new AddOwlPropertyReactor();
				owlAdder.setInsight(this.insight);
				owlAdder.setNounStore(this.store);
				owlAdder.execute();
			} catch(Exception e2) {
				e2.printStackTrace();
			}
						
			// an error occurred here, so we need to delete from the OWL
			throw new IllegalArgumentException("Error occurred to alter the table. Error returned from driver: " + e.getMessage(), e);
		}

		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully removed property"));
		return noun;
	}
}
