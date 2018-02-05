package prerna.sablecc2.reactor.masterdatabase;

import java.util.List;
import java.util.Map;
import java.util.Vector;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class ConnectedConceptsReactor extends AbstractReactor {
	
	public ConnectedConceptsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.CONCEPTS.getKey()};
	}

	@Override
	public NounMetadata execute() {
		GenRowStruct conceptNamesGrs = this.store.getNoun(keysToGet[0]);
		if(conceptNamesGrs == null) {
			throw new IllegalArgumentException("Need to define the concepts to find relations");
		}
		
		List<String> conceptLogicals = new Vector<String>();
		int size = conceptNamesGrs.size();
		for(int i = 0; i < size; i++) {
			conceptLogicals.add(conceptNamesGrs.get(i).toString());
		}
		
		Map connectedConceptsData = MasterDatabaseUtility.getConnectedConceptsRDBMS(conceptLogicals);
		return new NounMetadata(connectedConceptsData, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.CONNECTED_CONCEPTS);
	}

}
