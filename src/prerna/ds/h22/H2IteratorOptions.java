package prerna.ds.h22;

import java.sql.Connection;

import prerna.ds.IteratorOptions;
import prerna.ds.RdbmsTableMetaData;
import prerna.ds.util.H2FilterHash;

public class H2IteratorOptions extends IteratorOptions {

	RdbmsTableMetaData tableMeta;
	private boolean useSingleColumn;
	
	public H2IteratorOptions() {
		super();
		useSingleColumn = false;
	}
	
	public void setTableMeta(RdbmsTableMetaData tableMeta) {this.tableMeta = tableMeta;}
	public RdbmsTableMetaData getTableMeta() {return this.tableMeta;}
	
	public String getReadTable() {
		return tableMeta.getViewTableName();
	}
	
	public Connection getConnection() {
		return tableMeta.getConnection();
	}
	
	public H2FilterHash getFilters() {
		return tableMeta.getFilters();
	}
	
	public boolean useSingleColumn() {
		return this.useSingleColumn;
	}
	
	public void withSingleColumn(boolean useSingleColumn) {
		this.useSingleColumn = useSingleColumn;
	}
}
