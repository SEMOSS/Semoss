package prerna.sablecc2.reactor.app.upload.rdbms.external;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;

import prerna.engine.impl.rdbms.RdbmsConnectionHelper;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.sql.RdbmsTypeEnum;

public class CustomTableAndViewIterator implements Iterator<String[]>, Closeable {

	private ResultSet tablesRs;
	private String[] tableKeys;
	private boolean useDirectValues = false;
	private Iterator<String> directValuesIterator;
	
	/**
	 * Generates an iterator to either use the database metadata to get a list of table or views
	 * Or iterators through a passed in list of values
	 * @param meta
	 * @param catalogFilter
	 * @param schemaFilter
	 * @param tableAndViewFilters
	 */
	public CustomTableAndViewIterator(Connection con, DatabaseMetaData meta, String catalogFilter, String schemaFilter, RdbmsTypeEnum driver, Collection<String> tableAndViewFilters) {
		if(tableAndViewFilters == null || tableAndViewFilters.isEmpty()) {
			try {
				this.tablesRs = RdbmsConnectionHelper.getTables(con, meta, catalogFilter, schemaFilter, driver);
				this.tableKeys = RdbmsConnectionHelper.getTableKeys(driver);
			} catch (SQLException e) {
				throw new SemossPixelException(new NounMetadata("Unable to get tables from database metadata", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			}
		} else {
			this.useDirectValues = true;
			this.directValuesIterator = tableAndViewFilters.iterator();	
		}
	}
	
	@Override
	public boolean hasNext() {
		if(this.useDirectValues) {
			return this.directValuesIterator.hasNext();
		} else {
			try {
				return this.tablesRs.next();
			} catch (SQLException e) {
				throw new SemossPixelException(new NounMetadata("Unable to get tables from database metadata", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			}
		}
	}

	@Override
	public String[] next() {
		String tableOrViewName = null;
		String tableType = null;
		String tableSchema = null;
		
		if(this.useDirectValues) {
			tableOrViewName = this.directValuesIterator.next();
			// since we take in only 1 list
			// we do not differentiate between a view or table
			// we pass table to check if we should check for relationships 
			tableType = "TABLE";
		} else {
			try {
				tableOrViewName = this.tablesRs.getString(this.tableKeys[0]);
				tableType = this.tablesRs.getString(this.tableKeys[1]);
				tableSchema = this.tablesRs.getString(this.tableKeys[2]);
			} catch (SQLException e) {
				throw new SemossPixelException(new NounMetadata("Unable to get tables from database metadata", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			}
		}
		
		return new String[]{tableOrViewName, tableType, tableSchema};
	}

	@Override
	public void close() throws IOException {
		if(this.tablesRs != null) {
			try {
				this.tablesRs.close();
			} catch (SQLException e) {
				// ignore
			}
		}		
	}
	
}
