package prerna.sablecc2.reactor.app.upload.rdbms;

import java.io.IOException;
import java.util.Map;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IEngine;
import prerna.util.OWLER;

public class RdbmsUploadReactorUtility {

	private RdbmsUploadReactorUtility() {
		
	}
	
	/**
	 * Fill in the sqlHash with the types
	 */
	public static void createSQLTypes(Map<String, String> sqlHash) {
		sqlHash.put("DECIMAL", "FLOAT");
		sqlHash.put("DOUBLE", "FLOAT");
		sqlHash.put("STRING", "VARCHAR(2000)");
		sqlHash.put("TEXT", "VARCHAR(2000)");
		// TODO: the FE needs to differentiate between "dates with times" vs.
		// "dates"
		sqlHash.put("DATE", "DATE");
		sqlHash.put("SIMPLEDATE", "DATE");
		// currently only add in numbers as doubles
		sqlHash.put("NUMBER", "FLOAT");
		sqlHash.put("INTEGER", "FLOAT");
		sqlHash.put("BOOLEAN", "BOOLEAN");

		// TODO: standardized set of values
		sqlHash.put(SemossDataType.BOOLEAN.toString(), "BOOLEAN");
		sqlHash.put(SemossDataType.INT.toString(), "INT");
		sqlHash.put(SemossDataType.DOUBLE.toString(), "FLOAT");
		sqlHash.put(SemossDataType.STRING.toString(), "VARCHAR(2000)");
		sqlHash.put(SemossDataType.DATE.toString(), "DATE");
		sqlHash.put(SemossDataType.TIMESTAMP.toString(), "TIMESTAMP");

	}
	
	/**
	 * Add the metadata into the OWL
	 * @param owler
	 * @param tableName
	 * @param uniqueRowId
	 * @param headers
	 * @param sqlTypes
	 */
	public static void generateTableMetadata(OWLER owler, String tableName, String uniqueRowId, String[] headers, String[] sqlTypes, String[] additionalTypes) {
		// add the main column
		owler.addConcept(tableName, uniqueRowId, OWLER.BASE_URI, "LONG");
		// add the props
		for (int i = 0; i < headers.length; i++) {
			// NOTE ::: SQL_TYPES will have the added unique row id at index 0
			owler.addProp(tableName, uniqueRowId, headers[i], sqlTypes[i + 1], additionalTypes[i]);
		}
	}



	/**
	 * Create a new table given the table name, headers, and types
	 * Returns the sql types that were generated
	 * @param tableName
	 * @param headers
	 * @param types
	 * @throws IOException 
	 */
	public static String[] createNewTable(IEngine engine, String tableName, String uniqueRowId, String[] headers, SemossDataType[] types) throws Exception {
		// we need to add the identity column
		int size = types.length;
		String[] sqlTypes = new String[size + 1];
		String[] newHeaders = new String[size + 1];

		newHeaders[0] = uniqueRowId;
		sqlTypes[0] = "IDENTITY";
		for (int i = 0; i < size; i++) {
			newHeaders[i + 1] = headers[i];
			SemossDataType sType = types[i];
			if (sType == SemossDataType.STRING || sType == SemossDataType.FACTOR) {
				sqlTypes[i + 1] = "VARCHAR(2000)";
			} else if (sType == SemossDataType.INT) {
				sqlTypes[i + 1] = "INT";
			} else if (sType == SemossDataType.DOUBLE) {
				sqlTypes[i + 1] = "DOUBLE";
			} else if (sType == SemossDataType.DATE) {
				sqlTypes[i + 1] = "DATE";
			} else if (sType == SemossDataType.TIMESTAMP) {
				sqlTypes[i + 1] = "TIMESTAMP ";
			}
		}

		String createTable = RdbmsQueryBuilder.makeOptionalCreate(tableName, newHeaders, sqlTypes);
		engine.insertData(createTable);
		return sqlTypes;
	}

	/**
	 * 
	 * @param engine
	 * @param tableName
	 * @param columnName
	 * @throws IOException 
	 */
	public static void addIndex(IEngine engine, String tableName, String columnName) throws Exception {
		String indexName = columnName.toUpperCase() + "_INDEX";
		String indexSql = "CREATE INDEX " + indexName + " ON " + tableName + "(" + columnName.toUpperCase() + ")";
		engine.insertData(indexSql);
	}

}
