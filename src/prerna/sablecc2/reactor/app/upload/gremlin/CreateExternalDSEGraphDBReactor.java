package prerna.sablecc2.reactor.app.upload.gremlin;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.ds.datastax.DataStaxGraphEngine;
import prerna.engine.api.IEngine;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.app.upload.UploadUtilities;
import prerna.solr.SolrUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.OWLER;
import prerna.util.Utility;

public class CreateExternalDSEGraphDBReactor extends AbstractReactor {
	
	private static final String CLASS_NAME = CreateExternalDSEGraphDBReactor.class.getName();

	public CreateExternalDSEGraphDBReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.HOST.getKey(),
				ReactorKeysEnum.PORT.getKey(), ReactorKeysEnum.USERNAME.getKey(), ReactorKeysEnum.PASSWORD.getKey(),
				ReactorKeysEnum.GRAPH_NAME.getKey(), ReactorKeysEnum.GRAPH_TYPE_ID.getKey(), ReactorKeysEnum.GRAPH_NAME_ID.getKey(),
				ReactorKeysEnum.GRAPH_METAMODEL.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		organizeKeys();
		final String BASE = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
		String databaseName = this.keyValue.get(this.keysToGet[0]);
		if (databaseName == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Requires database name to save.", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		String host = this.keyValue.get(this.keysToGet[1]);
		if (host == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Requires host to save.", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		String port = this.keyValue.get(this.keysToGet[2]);
		if (port == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Requires port to save.", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		String username = this.keyValue.get(this.keysToGet[3]);
		String password = this.keyValue.get(this.keysToGet[4]);
		String graphName = this.keyValue.get(this.keysToGet[5]);
		if (graphName == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Requires graph name to save.", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		String graphTypeId = this.keyValue.get(this.keysToGet[6]);
		if (graphTypeId == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Requires graph type id to save.", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
//		String graphNameId = this.keyValue.get(this.keysToGet[7]);
//		if (graphNameId == null) {
//			SemossPixelException exception = new SemossPixelException(new NounMetadata("Requires graph name id to save.", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
//			exception.setContinueThreadOfExecution(false);
//			throw exception;
//		}

		// meta model
		GenRowStruct grs = this.store.getNoun(keysToGet[8]);
		Map<String, Object> metaMap = null;
		if(grs != null && !grs.isEmpty()) {
			metaMap = (Map<String, Object>) grs.get(0);
		}
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
		IEngine insightDb = UploadUtilities.generateInsightsDatabase(databaseName);
		logger.info("Done generating insights database");

		// create smss
		Map<String, String> typeMap = new HashMap<>();
		// create typeMap for smms
		for (String concept : concepts) {
			Map<String, Object> propMap = (Map<String, Object>) nodes.get(concept);
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
			tempSmssLocation = generateTempSmss(databaseName, host, port, username, password, graphName, typeMap);
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
	 * @param graphName2 
	 * @param password 
	 * @param tinkerFilePath
	 * @param tinkerTypeMap
	 * @param tinkerDriverType
	 * @return
	 * @throws IOException
	 */
	private String generateTempSmss(String appName, String host, String port, String username, String password, String graphName, Map tinkerTypeMap)
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
			pw.write("HOST" + "\t" + host + "\n");
			pw.write("PORT" + "\t" + port + "\n");
			if(username != null && password != null){
				pw.write("USERNAME" + "\t" + username + "\n");
				pw.write("PASSWORD" + "\t" + password + "\n");
			}
			pw.write("GRAPH_NAME" + "\t" + graphName + "\n");
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
}
