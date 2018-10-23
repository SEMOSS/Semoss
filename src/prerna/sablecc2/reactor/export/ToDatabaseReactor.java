package prerna.sablecc2.reactor.export;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ACTION_TYPE;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.task.TaskBuilderReactor;
import prerna.util.Utility;

public class ToDatabaseReactor extends TaskBuilderReactor {
	
	private static final String TARGET_DATABASE = "targetDatabase";
	private static final String TARGET_TABLE = "targetTable";
	private static final String OVERWRITE = "overwrite";

	
	public ToDatabaseReactor() {
		this.keysToGet = new String[]{TARGET_DATABASE, TARGET_TABLE, OVERWRITE};
	}
	
	@Override
	public NounMetadata execute() {
		this.task = getTask();
		buildTask();

		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	@Override
	protected void buildTask() {
		// we need to iterate and write the headers during the first time
		organizeKeys();
		IEngine targetEngine = Utility.getEngine(this.keyValue.get(this.keysToGet[0]));
		String targetTable = this.keyValue.get(this.keysToGet[1]);
		boolean overWrite = getOverWrite();
		
		// only for RDBMS right now
		if (targetEngine instanceof RDBMSNativeEngine) {
			// create prepared statement of all inserts from task
			String[] headers = null;
			if (this.task.hasNext()) {
				IHeadersDataRow row = this.task.next();
				headers = row.getHeaders();
			}
			String[] dataTypes = new String[headers.length];
			String[] createDataTypes = new String[headers.length];

			// WARNING: work around -- makes all STRINGS
			// get actual header data types and convert to sql
			// types for create table statements
			for (int i = 0; i < headers.length; i++) {
				dataTypes[i] = "STRING";
				createDataTypes[i] = "VARCHAR(2000)";
			}

			Object[] getPreparedStatementArgs = new Object[headers.length + 1];
			getPreparedStatementArgs[0] = targetTable;
			for (int headerIndex = 0; headerIndex < headers.length; headerIndex++) {
				getPreparedStatementArgs[headerIndex + 1] = headers[headerIndex];
			}
			PreparedStatement ps = (PreparedStatement) targetEngine.doAction(ACTION_TYPE.BULK_INSERT, getPreparedStatementArgs);

			// delete table if it exists and overwrite is true
			if (overWrite) {
				String delete = "DROP TABLE IF EXISTS " + targetTable + ";";
				String create = RdbmsQueryBuilder.makeCreate(targetTable, headers, createDataTypes);
				try {
					targetEngine.insertData(delete);
					targetEngine.insertData(create);
				} catch (Exception e) {
					e.printStackTrace();
					throw new SemossPixelException(new NounMetadata("Error occured trying to delete and create the new table", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
					
				}
			}

			// keep a batch size so we dont get heapspace
			final int batchSize = 5000;
			int count = 0;

			// we loop through every row of task result
			IHeadersDataRow row = null;
			try {
				int i = 0;
				while (this.task.hasNext()) {
					row = this.task.next();
					int size = headers.length;
					Object[] dataRow = row.getValues();
					// we need to loop through every value and cast
					// appropriately
					i = 0;
					for (; i < size; i++) {
						String type = dataTypes[i];
						if (Utility.isDateType(type)) {
//							java.util.Date value = Utility.getDateAsDateObj(dataRow[i].toString());
//							System.out.println("");
//							if (value != null) {
//								ps.setDate(i + 1, new java.sql.Date(value.getTime()));
//							} else {
//								// set default as null
//								ps.setObject(i + 1, null);
//							}
						} else if (Utility.isDoubleType(type)) {
//							Double value = null;
//							String val = dataRow[i].toString().trim();
//							try {
//								// added to remove $ and , in data and then try
//								// parsing as Double
//								int mult = 1;
//								if (val.startsWith("(") || val.startsWith("-")) // this
//																				// is
//																				// a
//																				// negativenumber
//									mult = -1;
//								val = val.replaceAll("[^0-9\\.E]", "");
//								value = mult * Double.parseDouble(val.trim());
//							} catch (NumberFormatException ex) {
//								// do nothing
//							}
//
//							if (value != null) {
//								ps.setDouble(i + 1, value);
//							} else {
//								// set default as null
//								ps.setObject(i + 1, null);
//							}
						} else {
							ps.setString(i + 1, dataRow[i] + "");
						}
					}

					// add it
					ps.addBatch();

					// batch commit based on size
					if (++count % batchSize == 0) {
						ps.executeBatch();
					}
				}

				ps.executeBatch(); // insert any remaining records
				ps.close();
			} catch (SQLException e) {
				e.printStackTrace();
				throw new IllegalArgumentException("ETL job failed.");
			}
		}
	}
	
	private boolean getOverWrite() {
		GenRowStruct boolGrs = this.store.getNoun(OVERWRITE);
		if(boolGrs != null) {
			if(boolGrs.size() > 0) {
				List<Object> val = boolGrs.getValuesOfType(PixelDataType.BOOLEAN);
				return (boolean) val.get(0);
			}
		}
		return false;
	}
}