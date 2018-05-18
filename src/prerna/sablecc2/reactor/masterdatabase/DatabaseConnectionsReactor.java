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
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMNS.getKey(), ReactorKeysEnum.APP.getKey()};
	}

	@Override
	public NounMetadata execute() {
		String dbFilter = getApp();
		List<String> conceptualNames = getColumns();
		List<String> logicalNames = MasterDatabaseUtility.getAllLogicalNamesFromConceptualRDBMS(conceptualNames, null);
		List<Map<String, Object>> data = MasterDatabaseUtility.getDatabaseConnections(logicalNames, dbFilter);
		return new NounMetadata(data, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_TRAVERSE_OPTIONS);
	}
	
	/**
	 * Getter for the list
	 * @return
	 */
	private List<String> getColumns() {
		// is it defined within store
		{
			GenRowStruct cGrs = this.store.getNoun(this.keysToGet[0]);
			if(cGrs != null && !cGrs.isEmpty()) {
				List<String> columns = new Vector<String>();
				for(int i = 0; i < cGrs.size(); i++) {
					String value = cGrs.get(0).toString();
					if(value.contains("__")) {
						columns.add(value.split("__")[1]);
					} else {
						columns.add(value);
					}
				}
				return columns;
			}
		}
		
		// is it inline w/ currow
		List<String> columns = new Vector<String>();
		for(int i = 0; i < this.curRow.size(); i++) {
			String value = this.curRow.get(i).toString();
			if(value.contains("__")) {
				columns.add(value.split("__")[1]);
			} else {
				columns.add(value);
			}
		}
		return columns;
	}
	
	private String getApp() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[1]);
		if(grs != null && !grs.isEmpty()) {
			return grs.get(0).toString();
		}
		return null;
	}
}
