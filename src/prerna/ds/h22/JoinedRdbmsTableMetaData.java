package prerna.ds.h22;

import java.sql.Connection;
import java.util.Hashtable;

import prerna.ds.RdbmsTableMetaData;

public class JoinedRdbmsTableMetaData {

	RdbmsTableMetaData coreMetaData;
	String viewTableName;
	Hashtable<String, String> joinColumnTranslation = new Hashtable<>();
	
	public JoinedRdbmsTableMetaData(RdbmsTableMetaData metaData) {
		this.coreMetaData = metaData;
	}
	
	public String getTableName() {
		return coreMetaData.getTableName();
	}
	
	public String getViewTableName() {
		return viewTableName;
	}
	
	public void setViewTable(String viewTable) {
		this.viewTableName = viewTable;
	}
	
	public void setConnection(Connection conn) {
		coreMetaData.setConnection(conn);
	}
	
	public boolean isJoined() {
		return true;
	}
}
