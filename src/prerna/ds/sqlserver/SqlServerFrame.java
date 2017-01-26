package prerna.ds.sqlserver;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import prerna.ds.TinkerMetaHelper;
import prerna.ds.h2.H2Frame;

public class SqlServerFrame extends H2Frame {

	public SqlServerFrame() {
		this.builder = new SqlServerBuilder();
	}
	
	protected void setSchema() {
		if (this.builder == null) {
			this.builder = new SqlServerBuilder();
		}
		this.builder.setSchema(this.userId);
	}
	
	@Override
	public String getDataMakerName() {
		return "SqlServerFrame";
	}
	
	@Override
	public void dropTable() {
		//DO NOTHING!!!
		System.err.println(" >>>>>>>>>>>>>>>>>>> FOR DEBUGGING PURPOSES NOT DROPPING TABLE" );
		System.err.println(" >>>>>>>>>>>>>>>>>>> FOR DEBUGGING PURPOSES NOT DROPPING TABLE" );
		System.err.println(" >>>>>>>>>>>>>>>>>>> FOR DEBUGGING PURPOSES NOT DROPPING TABLE" );
		System.err.println(" >>>>>>>>>>>>>>>>>>> FOR DEBUGGING PURPOSES NOT DROPPING TABLE" );
	}
	
	@Override
	public void dropOnDiskTemporalSchema() {
		this.builder.closeConnection();
	}

	public void connectToExistingTable(String tableName) {
		LinkedHashMap<String, String> dataTypeMap = this.builder.connectToExistingTable(tableName);
		
		String[] headers = new String[dataTypeMap.keySet().size()];
		int counter = 0;
		for(String header : dataTypeMap.keySet()) {
			headers[counter] = header;
			counter++;
		}
		
		Map<String, Set<String>> edgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(headers);
		this.mergeEdgeHash(edgeHash, dataTypeMap);
	}
}
