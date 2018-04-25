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

public class DatabaseConnectionsReactor extends AbstractReactor {
	
	public DatabaseConnectionsReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMNS.getKey()};
	}

	@Override
	public NounMetadata execute() {
		List<String> conceptualNames = getColumns();
		List<String> logicalNames = MasterDatabaseUtility.getAllLogicalNamesFromConceptualRDBMS(conceptualNames);
		 List<Map<String, Object>> data = MasterDatabaseUtility.getDatabaseConnections(logicalNames);
		return new NounMetadata(data, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_TRAVERSE_OPTIONS);
	}
	
	/**
	 * Getter for the list
	 * @return
	 */
	private List<String> getColumns() {
		// is it defined within store
		GenRowStruct cGrs = this.store.getNoun(this.keysToGet[0]);
		if(cGrs != null && !cGrs.isEmpty()) {
			List<String> columns = new Vector<String>();
			for(int i = 0; i < cGrs.size(); i++) {
				columns.add(cGrs.get(0).toString());
			}
			return columns;
		}

		// is it inline w/ currow
		List<String> columns = new Vector<String>();
		for(int i = 0; i < this.curRow.size(); i++) {
			columns.add(this.curRow.get(i).toString());
		}
		return columns;
	}
}
