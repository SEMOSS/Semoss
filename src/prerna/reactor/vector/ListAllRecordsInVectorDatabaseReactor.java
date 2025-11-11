package prerna.reactor.vector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.impl.vector.VectorDatabaseCSVTable;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class ListAllRecordsInVectorDatabaseReactor extends AbstractReactor{

	public ListAllRecordsInVectorDatabaseReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.ENGINE.getKey(),
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey()
		};
		this.keyRequired = new int[] {1};
	}
	
	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if(!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Vector database " + engineId + " does not exist or user does not have access to this vector database");
		}
		
		Map<String, Object> paramMap = getMap();
		if(paramMap == null) {
			paramMap = new HashMap<String, Object>();
		}
		
		IVectorDatabaseEngine engine = Utility.getVectorDatabase(engineId);
		if (engine == null) {
			throw new SemossPixelException("Unable to find engine");
		}
				
		return new NounMetadata(engine.listAllRecords(paramMap), PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}
	
	private Map<String, Object> getMap() {
        GenRowStruct mapGrs = this.store.getGenRowStruct(keysToGet[1]);
        if(mapGrs != null && !mapGrs.isEmpty()) {
            List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
            if(mapInputs != null && !mapInputs.isEmpty()) {
                return (Map<String, Object>) mapInputs.get(0).getValue();
            }
        }
        List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
        if(mapInputs != null && !mapInputs.isEmpty()) {
            return (Map<String, Object>) mapInputs.get(0).getValue();
        }
        return null;
    }
	
	
	@Override
	public String getReactorDescription() {
		StringBuilder headerBuilder = new StringBuilder();
		headerBuilder.append("'").append(VectorDatabaseCSVTable.SOURCE).append("', ")
			.append("'").append(VectorDatabaseCSVTable.SOURCE).append("', ")
			.append("'").append(VectorDatabaseCSVTable.MODALITY).append("', ")
			.append("'").append(VectorDatabaseCSVTable.DIVIDER).append("', ")
			.append("'").append(VectorDatabaseCSVTable.PART).append("', ")
			.append("'").append(VectorDatabaseCSVTable.TOKENS).append("', ")
			.append("'").append(VectorDatabaseCSVTable.CONTENT).append("'")
			;
		
		return "Get the list of all the chunks that have been uploaded into this vector database. "
				+ "This will include the following fields: " + headerBuilder.toString()
				;
	}
}
