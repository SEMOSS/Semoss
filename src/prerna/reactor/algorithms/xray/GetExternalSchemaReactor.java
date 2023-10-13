package prerna.reactor.algorithms.xray;
//package prerna.sablecc2.reactor.algorithms.xray;
//
//import java.io.IOException;
//import java.sql.Connection;
//import java.sql.DatabaseMetaData;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.util.ArrayList;
//import java.util.HashMap;
//
//import com.fasterxml.jackson.core.JsonGenerationException;
//import com.fasterxml.jackson.databind.JsonMappingException;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.databind.ObjectWriter;
//
//import prerna.engine.impl.rdbms.RdbmsConnectionHelper;
//import prerna.sablecc2.om.PixelDataType;
//import prerna.sablecc2.om.PixelOperationType;
//import prerna.sablecc2.om.ReactorKeysEnum;
//import prerna.sablecc2.om.nounmeta.NounMetadata;
//import prerna.sablecc2.reactor.AbstractReactor;
//
//public class GetExternalSchemaReactor extends AbstractReactor {
//	
//	public GetExternalSchemaReactor() {
//		this.keysToGet = new String[] { ReactorKeysEnum.DB_DRIVER_KEY.getKey(), ReactorKeysEnum.HOST.getKey(),
//				ReactorKeysEnum.PORT.getKey(), ReactorKeysEnum.USERNAME.getKey(),
//				ReactorKeysEnum.PASSWORD.getKey(), ReactorKeysEnum.SCHEMA.getKey() };
//	}
//
//	@Override
//	public NounMetadata execute() {
//		organizeKeys();
//		String dbDriver = this.keyValue.get(this.keysToGet[0]);
//		if(dbDriver == null) {
//			throw new IllegalArgumentException("Need to define the " + ReactorKeysEnum.DB_DRIVER_KEY.getKey());
//		}
//		String host = this.keyValue.get(this.keysToGet[1]);
//		if(host == null) {
//			throw new IllegalArgumentException("Need to define the " + ReactorKeysEnum.HOST.getKey());
//		}
//		String port = this.keyValue.get(this.keysToGet[2]);
//		if(port == null) {
//			throw new IllegalArgumentException("Need to define the " + ReactorKeysEnum.PORT.getKey());
//		}
//		String username = this.keyValue.get(this.keysToGet[3]);
//		if(username == null) {
//			throw new IllegalArgumentException("Need to define the " + ReactorKeysEnum.USERNAME.getKey());
//		}
//		String password = this.keyValue.get(this.keysToGet[4]);
//		if(password == null) {
//			throw new IllegalArgumentException("Need to define the " + ReactorKeysEnum.PASSWORD.getKey());
//		}
//		String schema = this.keyValue.get(this.keysToGet[5]);
//		if(schema == null) {
//			throw new IllegalArgumentException("Need to define the " + ReactorKeysEnum.SCHEMA.getKey());
//		}
//		
//		Connection con = null;
//		String schemaJSON = "";
//		NounMetadata noun = null;
//		try {
//			con = RdbmsConnectionHelper.buildConnection(dbDriver, host, port, username, password, schema, null);
//			String url = "";
//			HashMap<String, ArrayList<HashMap>> tableDetails = new HashMap<String, ArrayList<HashMap>>(); // tablename:
//			// [colDetails]
//			HashMap<String, ArrayList<HashMap>> relations = new HashMap<String, ArrayList<HashMap>>(); // sub_table:
//			// [(obj_table, fromCol, toCol)]
//
//			DatabaseMetaData meta = con.getMetaData();
//			ResultSet tables = meta.getTables(null, null, null, new String[] { "TABLE" });
//			while (tables.next()) {
//				ArrayList<String> primaryKeys = new ArrayList<String>();
//				HashMap<String, Object> colDetails = new HashMap<String, Object>(); // name:
//
//				ArrayList<HashMap> allCols = new ArrayList<HashMap>();
//				HashMap<String, String> fkDetails = new HashMap<String, String>();
//				ArrayList<HashMap> allRels = new ArrayList<HashMap>();
//
//				String table = tables.getString("table_name");
//				ResultSet keys = meta.getPrimaryKeys(null, null, table);
//				while (keys.next()) {
//					primaryKeys.add(keys.getString("column_name"));
//				}
//				keys = meta.getColumns(null, null, table, null);
//				while (keys.next()) {
//					colDetails = new HashMap<String, Object>();
//					colDetails.put("name", keys.getString("column_name"));
//					colDetails.put("type", keys.getString("type_name"));
//					if (primaryKeys.contains(keys.getString("column_name"))) {
//						colDetails.put("isPK", true);
//					} else {
//						colDetails.put("isPK", false);
//					}
//					allCols.add(colDetails);
//				}
//				tableDetails.put(table, allCols);
//				keys = meta.getExportedKeys(null, null, table);
//				while (keys.next()) {
//					fkDetails = new HashMap<String, String>();
//					fkDetails.put("fromCol", keys.getString("PKCOLUMN_NAME"));
//					fkDetails.put("toTable", keys.getString("FKTABLE_NAME"));
//					fkDetails.put("toCol", keys.getString("FKCOLUMN_NAME"));
//					allRels.add(fkDetails);
//				}
//				relations.put(table, allRels);
//			}
//			HashMap<String, Object> ret = new HashMap<String, Object>();
//			ret.put("databaseName", con.getCatalog());
//			ret.put("tables", tableDetails);
//			ret.put("relationships", relations);
//			ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
//			schemaJSON = ow.writeValueAsString(ret);
//			noun = new NounMetadata(schemaJSON, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.CODE_EXECUTION);
//
//			con.close();
//
//		} catch (SQLException e) {
//			e.printStackTrace();
//		} catch (JsonGenerationException e) {
//			e.printStackTrace();
//		} catch (JsonMappingException e) {
//			e.printStackTrace();
//		} catch (IOException e) {
//			e.printStackTrace();
//		} finally {
//			if (con != null) {
//				try {
//					con.close();
//				} catch (SQLException e) {
//					e.printStackTrace();
//				}
//			}
//		}
//
//		return noun;
//	}
//
//}
