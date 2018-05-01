package prerna.sablecc2.reactor.utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.ds.datastax.DataStaxGraphEngine;
import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.solr.SolrUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.OWLER;
import prerna.util.Utility;
import prerna.util.sql.H2QueryUtil;

public class ExternalDSEGraphDBReactor extends AbstractReactor {
	private static final String CLASS_NAME = ExternalDSEGraphDBReactor.class.getName();

	public ExternalDSEGraphDBReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.HOST.getKey(),
				ReactorKeysEnum.PORT.getKey(), ReactorKeysEnum.GRAPH_NAME.getKey(),
				ReactorKeysEnum.GRAPH_TYPE_ID.getKey(), ReactorKeysEnum.GRAPH_METAMODEL.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		organizeKeys();
		final String BASE = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
		String databaseName = this.keyValue.get(this.keysToGet[0]);
		String host = this.keyValue.get(this.keysToGet[1]);
		String port = this.keyValue.get(this.keysToGet[2]);
		String graphName = this.keyValue.get(this.keysToGet[3]);
		String graphTypeId = this.keyValue.get(this.keysToGet[4]);

		// meta model
		List<Object> mapInput = this.curRow.getValuesOfType(PixelDataType.MAP);
		Map<String, Object> metaMap = (Map<String, Object>) mapInput.get(0);
		Map<String, Object> nodes = (Map<String, Object>) metaMap.get("nodes");
		Map<String, Object> edges = (Map<String, Object>) metaMap.get("edges");
		Set<String> concepts = nodes.keySet();
		Map<String, String> conceptTypes = new HashMap<String, String>();
		Set<String> edgeLabels = edges.keySet();

		// create db folder
		logger.info("Start generating app folder");
		String dbFolder = BASE + DIR_SEPARATOR + "db" + DIR_SEPARATOR + databaseName;
		File newF = new File(dbFolder);
		newF.mkdirs();
		logger.info("Done generating app folder");

		// create insights dbs
		logger.info("Start generating insights database");
		IEngine insightDb = generateInsightsDatabase(databaseName);
		logger.info("Done generating insights database");

		// create smss
		Map<String, String> typeMap = new HashMap<>();
		// create typeMap for smms
		for (String concept : concepts) {
			Map<String, Object> propMap = (Map) nodes.get(concept);
			for (String prop : propMap.keySet()) {
				// get concept type
				if (prop.equals(graphTypeId)) {
					conceptTypes.put(concept, propMap.get(graphTypeId).toString());
					typeMap.put(concept, graphTypeId);
					break;
				}
			}
		}

		// add to DIHelper so we dont auto load with the file watcher
		String tempSmssLocation = null;
		logger.info("Start generating temp smss");
		try {
			tempSmssLocation = generateTempSmss(databaseName, host, port, graphName, typeMap);
			DIHelper.getInstance().getCoreProp().setProperty(databaseName + "_" + Constants.STORE, tempSmssLocation);
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e.getMessage());
		}
		logger.info("Done generating temp smss");

		// load into solr
		logger.info("Start loading into solr");
		try {
			SolrUtility.addAppToSolr(databaseName);
		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException e) {
			e.printStackTrace();
		}
		logger.info("Done loading into solr");

		// create owl file
		logger.info("Start creating owl");
		String owlPath = dbFolder + DIR_SEPARATOR + databaseName + "_OWL.owl";
		OWLER owler = new OWLER(owlPath, IEngine.ENGINE_TYPE.TINKER);
		// add concepts
		for (String concept : concepts) {
			String conceptType = conceptTypes.get(concept);
			owler.addConcept(concept, conceptType);
			Map<String, Object> propMap = (Map<String, Object>) nodes.get(concept);
			// add properties
			for (String prop : propMap.keySet()) {
				if (!prop.equals(graphTypeId)) {
					String propType = propMap.get(prop).toString();
					owler.addProp(concept, prop, propType);
				}
			}
		}
		// add relationships
		for (String label : edgeLabels) {
			List<String> rels = (List<String>) edges.get(label);
			owler.addRelation(rels.get(0), rels.get(1), null);
		}
		try {
			owler.commit();
			owler.export();
		} catch (IOException e) {
			e.printStackTrace();
		}
		owler.closeOwl();
		logger.info("Done creating owl");
		// rename .temp to .smss
		logger.info("Replacing .temp smss file with .smm ");
		File tempFile = new File(tempSmssLocation);
		File smssFile = new File(tempSmssLocation.replace(".temp", ".smss"));
		try {
			FileUtils.copyFile(tempFile, smssFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
		tempFile.delete();
		DIHelper.getInstance().getCoreProp().setProperty(databaseName + "_" + Constants.STORE,
				smssFile.getAbsolutePath());
		logger.info("Done replacing .temp smss file with .smss ");
		logger.info("Finalizing adding external graph engine ");
		Utility.synchronizeEngineMetadata(databaseName);
		DataStaxGraphEngine dseEngine = new DataStaxGraphEngine();
		dseEngine.setEngineName(databaseName);
		dseEngine.setInsightDatabase(insightDb);
		dseEngine.openDB(smssFile.getAbsolutePath());
		// only at end do we add to DIHelper
		DIHelper.getInstance().setLocalProperty(databaseName, dseEngine);
		String appNames = (String) DIHelper.getInstance().getLocalProp(Constants.ENGINES);
		appNames = appNames + ";" + databaseName;
		DIHelper.getInstance().setLocalProperty(Constants.ENGINES, appNames);

		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	/**
	 * Generate the SMSS for the db
	 * 
	 * @param appName
	 * @param tinkerFilePath
	 * @param tinkerTypeMap
	 * @param tinkerDriverType
	 * @return
	 * @throws IOException
	 */
	private String generateTempSmss(String appName, String host, String port, String graphName, Map tinkerTypeMap)
			throws IOException {
		final String FILE_SEP = System.getProperty("file.separator");
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String smssFileLocation = baseFolder + FILE_SEP + "db" + FILE_SEP + appName + ".temp";

		// also write the base properties
		// ie ONTOLOGY, DREAMER, ENGINE, ENGINE CLASS
		FileWriter pw = null;
		try {
			File newFile = new File(smssFileLocation);
			pw = new FileWriter(newFile);
			// base properties
			pw.write("Base Properties \n");
			pw.write(Constants.ENGINE + "\t" + appName + "\n");
			pw.write(Constants.ENGINE_TYPE + "\tprerna.ds.datastax.DataStaxGraphEngine\n");
			pw.write(Constants.RDBMS_INSIGHTS + "\tdb" + System.getProperty("file.separator") + "@engine@"
					+ System.getProperty("file.separator") + "insights_database" + "\n");
			pw.write(Constants.SOLR_RELOAD + "\tfalse\n");
			pw.write(Constants.HIDDEN_DATABASE + "\tfalse\n");
			pw.write(Constants.OWL + "\tdb" + System.getProperty("file.separator") + "@engine@"
					+ System.getProperty("file.separator") + "@engine@_OWL.OWL" + "\n");

			// custom dse props
			pw.write("HOST" + "\t" + host + " \n");
			pw.write("PORT" + "\t" + port + " \n");
			pw.write("GRAPH_NAME" + "\t" + graphName + " \n");
			// add type map
			Gson gson = new GsonBuilder().create();
			String json = gson.toJson(tinkerTypeMap);
			pw.write("TYPE_MAP" + "\t" + json + "\n");

		} catch (IOException ex) {
			ex.printStackTrace();
			throw new IOException("Could not generate smss file");
		} finally {
			if (pw != null) {
				try {
					pw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return smssFileLocation;
	}

	/**
	 * Generate the insights database TODO: TODO: TODO: NEED TO CONSOLIDATE THIS
	 * WITH ABSTRACT ENGINE CREATOR ... TODO: LOTS OF CLEAN UP IN LOADING :(
	 * TODO:
	 * 
	 * @param appName
	 * @return
	 */
	private IEngine generateInsightsDatabase(String appName) {
		H2QueryUtil queryUtil = new H2QueryUtil();
		Properties prop = new Properties();

		final String FILE_SEP = System.getProperty("file.separator");
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String connectionURL = "jdbc:h2:" + baseFolder + FILE_SEP + "db" + FILE_SEP + appName + FILE_SEP
				+ "insights_database;query_timeout=180000;early_filter=true;query_cache_size=24;cache_size=32768";
		prop.put(Constants.CONNECTION_URL, connectionURL);
		prop.put(Constants.USERNAME, queryUtil.getDefaultDBUserName());
		prop.put(Constants.PASSWORD, queryUtil.getDefaultDBPassword());
		prop.put(Constants.DRIVER, queryUtil.getDatabaseDriverClassName());
		prop.put(Constants.RDBMS_TYPE, queryUtil.getDatabaseType().toString());
		prop.put("TEMP", "TRUE");
		RDBMSNativeEngine insightRdbmsEngine = new RDBMSNativeEngine();
		insightRdbmsEngine.setProperties(prop);
		// opening will work since we directly injected the prop map
		// this way i do not need to write it to disk and then recreate it later
		insightRdbmsEngine.openDB(null);

		// CREATE TABLE QUESTION_ID (ID VARCHAR(50), QUESTION_NAME VARCHAR(255),
		// QUESTION_PERSPECTIVE VARCHAR(225), QUESTION_LAYOUT VARCHAR(225),
		// QUESTION_ORDER INT, QUESTION_DATA_MAKER VARCHAR(225), QUESTION_MAKEUP
		// CLOB, QUESTION_PROPERTIES CLOB, QUESTION_OWL CLOB,
		// QUESTION_IS_DB_QUERY BOOLEAN, DATA_TABLE_ALIGN VARCHAR(500),
		// QUESTION_PKQL ARRAY)
		String questionTableCreate = "CREATE TABLE QUESTION_ID (" + "ID VARCHAR(50), " + "QUESTION_NAME VARCHAR(255), "
				+ "QUESTION_PERSPECTIVE VARCHAR(225), " + "QUESTION_LAYOUT VARCHAR(225), " + "QUESTION_ORDER INT, "
				+ "QUESTION_DATA_MAKER VARCHAR(225), " + "QUESTION_MAKEUP CLOB, " + "QUESTION_PROPERTIES CLOB, "
				+ "QUESTION_OWL CLOB, " + "QUESTION_IS_DB_QUERY BOOLEAN, " + "DATA_TABLE_ALIGN VARCHAR(500), "
				+ "HIDDEN_INSIGHT BOOLEAN, " + "QUESTION_PKQL ARRAY)";

		insightRdbmsEngine.insertData(questionTableCreate);

		// CREATE TABLE PARAMETER_ID (PARAMETER_ID VARCHAR(255), PARAMETER_LABEL
		// VARCHAR(255), PARAMETER_TYPE VARCHAR(225), PARAMETER_DEPENDENCY
		// VARCHAR(225), PARAMETER_QUERY VARCHAR(2000), PARAMETER_OPTIONS
		// VARCHAR(2000), PARAMETER_IS_DB_QUERY BOOLEAN, PARAMETER_MULTI_SELECT
		// BOOLEAN, PARAMETER_COMPONENT_FILTER_ID VARCHAR(255),
		// PARAMETER_VIEW_TYPE VARCHAR(255), QUESTION_ID_FK INT)
		String parameterTableCreate = "CREATE TABLE PARAMETER_ID (" + "PARAMETER_ID VARCHAR(255), "
				+ "PARAMETER_LABEL VARCHAR(255), " + "PARAMETER_TYPE VARCHAR(225), "
				+ "PARAMETER_DEPENDENCY VARCHAR(225), " + "PARAMETER_QUERY VARCHAR(2000), "
				+ "PARAMETER_OPTIONS VARCHAR(2000), " + "PARAMETER_IS_DB_QUERY BOOLEAN, "
				+ "PARAMETER_MULTI_SELECT BOOLEAN, " + "PARAMETER_COMPONENT_FILTER_ID VARCHAR(255), "
				+ "PARAMETER_VIEW_TYPE VARCHAR(255), " + "QUESTION_ID_FK INT)";

		insightRdbmsEngine.insertData(parameterTableCreate);

		String feTableCreate = "CREATE TABLE UI (" + "QUESTION_ID_FK INT, " + "UI_DATA CLOB)";

		insightRdbmsEngine.insertData(feTableCreate);

		return insightRdbmsEngine;
	}

}
