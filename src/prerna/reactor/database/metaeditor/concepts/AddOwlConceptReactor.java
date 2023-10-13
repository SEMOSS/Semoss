package prerna.reactor.database.metaeditor.concepts;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IDatabaseEngine.DATABASE_TYPE;
import prerna.engine.api.impl.util.Owler;
import prerna.reactor.database.metaeditor.AbstractMetaEditorReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineSyncUtility;
import prerna.util.Utility;

public class AddOwlConceptReactor extends AbstractMetaEditorReactor {

	public AddOwlConceptReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.CONCEPT.getKey(),
				ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.DATA_TYPE.getKey(),
				ReactorKeysEnum.ADDITIONAL_DATA_TYPE.getKey(), CONCEPTUAL_NAME, ReactorKeysEnum.DESCRIPTION.getKey(),
				ReactorKeysEnum.LOGICAL_NAME.getKey() };
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();

		String databaseId = this.keyValue.get(this.keysToGet[0]);
		// perform translation if alias is passed
		// and perform security check
		databaseId = testDatabaseId(databaseId, true);

		String concept = this.keyValue.get(this.keysToGet[1]);
		if (concept == null || concept.isEmpty()) {
			throw new IllegalArgumentException("Must define the concept being added to the database metadata");
		}

		// if RDBMS, we need to know the prim key of the column
		IDatabaseEngine database = Utility.getDatabase(databaseId);
		ClusterUtil.pullOwl(databaseId);
		String column = this.keyValue.get(this.keysToGet[2]);
		if ((column == null || column.isEmpty()) && database.getDatabaseType() == DATABASE_TYPE.RDBMS) {
			throw new IllegalArgumentException("Must define the column for the concept being added to the database metadata");
		}
		String dataType = this.keyValue.get(this.keysToGet[3]);
		if (dataType == null || dataType.isEmpty()) {
			throw new IllegalArgumentException("Must define the data type for the concept being added to the database metadata");
		}
		
		// TODO: need to account for this on concepts!!!!
		String additionalDataType = this.keyValue.get(this.keysToGet[4]);
		String conceptual = this.keyValue.get(this.keysToGet[5]);
		if (conceptual != null) {
			conceptual = conceptual.trim();
			if (!conceptual.matches("^[a-zA-Z0-9-_]+$")) {
				throw new IllegalArgumentException("Conceptual name must contain only letters, numbers, and underscores");
			}
			conceptual = conceptual.replaceAll("_{2,}", "_");
		}

		List<String> concepts = database.getPhysicalConcepts();
		for (String conceptUri : concepts) {
			String table = Utility.getInstanceName(conceptUri);
			if (table.equalsIgnoreCase(concept)) {
				throw new IllegalArgumentException("A concept already exists with this name. "
						+ "Add a new unique concept or edit the existing concept");
			}
		}

		Owler owler = getOWLER(databaseId);
//		String physicalUri = owler.addConcept(concept, column, dataType, conceptual);
//
//		// now add the description (checks done in method)
//		String description = this.keyValue.get(this.keysToGet[6]);
//		owler.addDescription(physicalUri, description);
//		// add the logical names (additional checks done in method)
//		List<String> logicalNames = getLogicalNames();
//		if(!logicalNames.isEmpty()) {
//			owler.addLogicalNames(physicalUri, logicalNames.toArray(new String[]{}));
//		}
		
		try {
			owler.export();
		} catch (IOException e) {
			e.printStackTrace();
			NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
			noun.addAdditionalReturn(new NounMetadata("An error occurred attempting to add the desired concept",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			return noun;
		}
		EngineSyncUtility.clearEngineCache(databaseId);
		ClusterUtil.pushOwl(databaseId);
		
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		noun.addAdditionalReturn(new NounMetadata("Successfully added new concept", PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}
	
	/**
	 * Get the logical names
	 * 
	 * @return
	 */
	private List<String> getLogicalNames() {
		Vector<String> logicalNames = new Vector<String>();
		GenRowStruct instanceGrs = this.store.getNoun(keysToGet[7]);
		if (instanceGrs != null && !instanceGrs.isEmpty()) {
			for (int i = 0; i < instanceGrs.size(); i++) {
				String name = (String) instanceGrs.get(i);
				if (name.length() > 0) {
					logicalNames.add(name);
				}
			}
		}
		return logicalNames;
	}
}
