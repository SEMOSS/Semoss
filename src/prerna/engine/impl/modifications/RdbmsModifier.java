package prerna.engine.impl.modifications;

import java.sql.SQLException;

import prerna.engine.api.IEngine;
import prerna.engine.api.IEngineModifier;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.util.sql.AbstractSqlQueryUtil;

public class RdbmsModifier implements IEngineModifier {

	private RDBMSNativeEngine engine = null;
	private AbstractSqlQueryUtil queryUtil = null;
	
	@Override
	public void setEngine(IEngine engine) {
		this.engine = (RDBMSNativeEngine) engine;
		this.queryUtil = this.engine.getQueryUtil();
	}

	@Override
	public void addProperty(String existingConcept, String newColumn, String dataType) throws Exception {
		if(queryUtil.allowAddColumn()) {
			String sqlQuery = queryUtil.alterTableAddColumn(existingConcept, newColumn, dataType);
			try {
				this.engine.insertData(sqlQuery);
			} catch (SQLException e) {
				throw new SQLException("Error occured to alter the table. Error returned from driver: " + e.getMessage(), e);
			}
		} else {
			// we need to make a new temp table with the new column we are adding
			// we need to insert all the old values of the existing table
			// we need to drop the existing table (we will rename it in case the last operation fails and only drop at end)
			// we need to rename the temp table to the existing table name
			
			
		}
	}

	@Override
	public void removeProperty(String existingConcept, String existingColumn) throws Exception {
		if(queryUtil.allowDropColumn()) {
			String sqlQuery = queryUtil.alterTableDropColumn(existingConcept, existingColumn);
			try {
				this.engine.insertData(sqlQuery);
			} catch (SQLException e) {
				throw new SQLException("Error occured to alter the table. Error returned from driver: " + e.getMessage(), e);
			}
		} else {
			// we need to make a new temp table without the column we are dropping
			// we need to insert all the old values of the existing table
			// we need to drop the existing table (we will rename it in case the last operation fails and only drop at end)
			// we need to rename the temp table to the existing table name
			
			
		}
	}

	
	
}
