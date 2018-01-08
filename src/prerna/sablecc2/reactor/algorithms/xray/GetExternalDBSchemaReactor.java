package prerna.sablecc2.reactor.algorithms.xray;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;

import prerna.engine.impl.rdbms.RdbmsConnectionHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetExternalDBSchemaReactor extends AbstractReactor {
	public GetExternalDBSchemaReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DB_DRIVER_KEY.toString(), ReactorKeysEnum.HOST.toString(),
				ReactorKeysEnum.PORT.toString(), ReactorKeysEnum.USERNAME.toString(),
				ReactorKeysEnum.PASSWORD.toString(), ReactorKeysEnum.SCHEMA.toString() };
	}
	@Override
	public NounMetadata execute() {
		String dbDriver = getDriver();
		String host = getHost();
		String port = getPort();
		String username = getUsername();
		String password = getPassword();
		String schema = getSchema();
		
		Connection con = null;
		String schemaJSON = "";
		NounMetadata noun = null;
		try {
			con = RdbmsConnectionHelper.buildConnection(dbDriver, host, port, username, password, schema, null); 
			String url = "";
			HashMap<String, ArrayList<HashMap>> tableDetails = new HashMap<String, ArrayList<HashMap>>(); // tablename:
			// [colDetails]
			HashMap<String, ArrayList<HashMap>> relations = new HashMap<String, ArrayList<HashMap>>(); // sub_table:
			// [(obj_table,
			// fromCol,
			// toCol)]

			DatabaseMetaData meta = con.getMetaData();
			ResultSet tables = meta.getTables(null, null, null, new String[] { "TABLE" });
			while (tables.next()) {
				ArrayList<String> primaryKeys = new ArrayList<String>();
				HashMap<String, Object> colDetails = new HashMap<String, Object>(); // name:
				// ,
				// type:
				// ,
				// isPK:
				ArrayList<HashMap> allCols = new ArrayList<HashMap>();
				HashMap<String, String> fkDetails = new HashMap<String, String>();
				ArrayList<HashMap> allRels = new ArrayList<HashMap>();

				String table = tables.getString("table_name");
				System.out.println("Table: " + table);
				ResultSet keys = meta.getPrimaryKeys(null, null, table);
				while (keys.next()) {
					primaryKeys.add(keys.getString("column_name"));

					System.out.println(keys.getString("table_name") + ": " + keys.getString("column_name") + " added.");
				}

				System.out.println("COLUMNS " + primaryKeys);
				keys = meta.getColumns(null, null, table, null);
				while (keys.next()) {
					colDetails = new HashMap<String, Object>();
					colDetails.put("name", keys.getString("column_name"));
					colDetails.put("type", keys.getString("type_name"));
					if (primaryKeys.contains(keys.getString("column_name"))) {
						colDetails.put("isPK", true);
					} else {
						colDetails.put("isPK", false);
					}
					allCols.add(colDetails);

					System.out.println(
							"\t" + keys.getString("column_name") + " (" + keys.getString("type_name") + ") added.");
				}
				tableDetails.put(table, allCols);

				System.out.println("FOREIGN KEYS");
				keys = meta.getExportedKeys(null, null, table);
				while (keys.next()) {
					fkDetails = new HashMap<String, String>();
					fkDetails.put("fromCol", keys.getString("PKCOLUMN_NAME"));
					fkDetails.put("toTable", keys.getString("FKTABLE_NAME"));
					fkDetails.put("toCol", keys.getString("FKCOLUMN_NAME"));
					allRels.add(fkDetails);

					System.out.println(keys.getString("PKTABLE_NAME") + ": " + keys.getString("PKCOLUMN_NAME") + " -> "
							+ keys.getString("FKTABLE_NAME") + ": " + keys.getString("FKCOLUMN_NAME") + " added.");
				}
				relations.put(table, allRels);
			}
			HashMap<String, Object> ret = new HashMap<String, Object>();
			ret.put("databaseName", con.getCatalog());
			ret.put("tables", tableDetails);
			ret.put("relationships", relations);
			ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
			schemaJSON = ow.writeValueAsString(ret);
			noun = new NounMetadata(schemaJSON, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.CODE_EXECUTION);

			con.close();

		} catch (SQLException e) {
			e.printStackTrace();
		} catch (JsonGenerationException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (con != null) {
				try {
					con.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		return noun;
	}
	private String getSchema() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.SCHEMA.toString());
		if (grs != null && !grs.isEmpty()) {
			String sheet = grs.getNoun(0).getValue() + "";
			if (sheet.length() > 0) {
				return sheet;
			}
		}
		throw new IllegalArgumentException("Need to define the " + ReactorKeysEnum.SCHEMA.toString());
	}
	private String getPassword() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.PASSWORD.toString());
		if (grs != null && !grs.isEmpty()) {
			String sheet = grs.getNoun(0).getValue() + "";
			if (sheet.length() > 0) {
				return sheet;
			}
		}
		throw new IllegalArgumentException("Need to define the " + ReactorKeysEnum.PASSWORD.toString());
	}
	private String getUsername() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.USERNAME.toString());
		if (grs != null && !grs.isEmpty()) {
			String sheet = grs.getNoun(0).getValue() + "";
			if (sheet.length() > 0) {
				return sheet;
			}
		}
		throw new IllegalArgumentException("Need to define the " + ReactorKeysEnum.USERNAME.toString());
	}
	private String getPort() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.PORT.toString());
		if (grs != null && !grs.isEmpty()) {
			String sheet = grs.getNoun(0).getValue() + "";
			if (sheet.length() > 0) {
				return sheet;
			}
		}
		throw new IllegalArgumentException("Need to define the " + ReactorKeysEnum.PORT.toString());
	}
	private String getHost() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.HOST.toString());
		if (grs != null && !grs.isEmpty()) {
			String sheet = grs.getNoun(0).getValue() + "";
			if (sheet.length() > 0) {
				return sheet;
			}
		}
		throw new IllegalArgumentException("Need to define the " + ReactorKeysEnum.HOST.toString());
	}
	private String getDriver() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.DB_DRIVER_KEY.toString());
		if (grs != null && !grs.isEmpty()) {
			String sheet = grs.getNoun(0).getValue() + "";
			if (sheet.length() > 0) {
				return sheet;
			}
		}
		throw new IllegalArgumentException("Need to define the " + ReactorKeysEnum.DB_DRIVER_KEY.toString());
	}

}
