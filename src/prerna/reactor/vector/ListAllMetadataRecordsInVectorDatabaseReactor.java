package prerna.reactor.vector;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.impl.vector.FaissDatabaseEngine;
import prerna.engine.impl.vector.metadata.VectorDatabaseMetadataCSVTable;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class ListAllMetadataRecordsInVectorDatabaseReactor extends AbstractReactor {
	
	public ListAllMetadataRecordsInVectorDatabaseReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.ENGINE.getKey() };
		this.keyRequired = new int[] {1};
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Vector database " + engineId + " does not exist or user does not have access to this vector database");
		}
		
		IVectorDatabaseEngine engine = Utility.getVectorDatabase(engineId);
		if (engine == null) {
			throw new SemossPixelException("Unable to find engine");
		}
		
		FaissDatabaseEngine faissEngine = (FaissDatabaseEngine) engine;
		return new NounMetadata(faissEngine.listAllMetadataRecords(), PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	@Override
	public String getReactorDescription() {
		StringBuilder headerBuilder = new StringBuilder();
		headerBuilder.append("'").append(VectorDatabaseMetadataCSVTable.SOURCE).append("', ")
			.append("'").append(VectorDatabaseMetadataCSVTable.ATTRIBUTE).append("', ")
			.append("'").append(VectorDatabaseMetadataCSVTable.STR_VALUE).append("', ")
			.append("'").append(VectorDatabaseMetadataCSVTable.INT_VALUE).append("', ")
			.append("'").append(VectorDatabaseMetadataCSVTable.NUM_VALUE).append("', ")
			.append("'").append(VectorDatabaseMetadataCSVTable.BOOL_VALUE).append("', ")
			.append("'").append(VectorDatabaseMetadataCSVTable.DATE_VAL).append("'")
			.append("'").append(VectorDatabaseMetadataCSVTable.TIMESTAMP_VAL).append("'")
			;
		
		return "Get the list of all the metadata records stored in FAISS vector database. "
				+ "This will include the following fields: " + headerBuilder.toString()
				;
	}
}
