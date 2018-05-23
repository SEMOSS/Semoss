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

import prerna.engine.api.IEngine;
import prerna.engine.impl.tinker.TinkerEngine;
import prerna.poi.main.helper.ImportOptions.TINKER_DRIVER;
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

public class CreateExternalGraphDBReactor extends AbstractReactor {
	
	private static final String CLASS_NAME = CreateExternalGraphDBReactor.class.getName();

	public CreateExternalGraphDBReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.FILE_PATH.getKey(),
				ReactorKeysEnum.GRAPH_TYPE_ID.getKey(), ReactorKeysEnum.GRAPH_METAMODEL.getKey() };
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
		String fileName = this.keyValue.get(this.keysToGet[1]);
		if (fileName == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Requires file name to save.", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		String nodeType = this.keyValue.get(this.keysToGet[2]);
		if (nodeType == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Requires graph type id to save.", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		// get metamodel
		GenRowStruct grs = this.store.getNoun(keysToGet[3]);
		Map<String, Object> metaMap = null;
		if(grs != null && !grs.isEmpty()) {
			metaMap = (Map<String, Object>) grs.get(0);
		}
		Map<String, Object> nodes = (Map<String, Object>) metaMap.get("nodes");
		Map<String, Object> edges = (Map<String, Object>) metaMap.get("edges");
		Set<String> concepts = nodes.keySet();
		Map<String, String> conceptTypes = new HashMap<String, String>();
		Set<String> edgeLabels = edges.keySet();
		TINKER_DRIVER tinkerDriver = TINKER_DRIVER.NEO4J;
		if (fileName.contains(".")) {
			String fileExtension = fileName.substring(fileName.indexOf(".") + 1);
			tinkerDriver = TINKER_DRIVER.valueOf(fileExtension.toUpperCase());
		}

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
			Map<String, Object> propMap = (Map) nodes.get(concept);
			for (String prop : propMap.keySet()) {
				// get concept type
				if (prop.equals(nodeType)) {
					conceptTypes.put(concept, propMap.get(nodeType).toString());
					typeMap.put(concept, nodeType);
					break;
				}
			}
		}

		// add to DIHelper so we dont auto load with the file watcher
		String tempSmssLocation = null;
		logger.info("Start generating temp smss");
		try {
			tempSmssLocation = generateTempSmss(databaseName, fileName, typeMap, tinkerDriver);
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
				if (!prop.equals(nodeType)) {
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
		TinkerEngine tinkerEng = new TinkerEngine();
		tinkerEng.setEngineName(databaseName);
		tinkerEng.setInsightDatabase(insightDb);
		tinkerEng.openDB(smssFile.getAbsolutePath());
		// only at end do we add to DIHelper
		DIHelper.getInstance().setLocalProperty(databaseName, tinkerEng);
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
	private String generateTempSmss(String appName, String tinkerFilePath, Map tinkerTypeMap,
			TINKER_DRIVER tinkerDriverType) throws IOException {
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
			pw.write(Constants.ENGINE_TYPE + "\tprerna.engine.impl.tinker.TinkerEngine\n");
			pw.write(Constants.RDBMS_INSIGHTS + "\tdb" + System.getProperty("file.separator") + "@engine@"
					+ System.getProperty("file.separator") + "insights_database" + "\n");
			pw.write(Constants.SOLR_RELOAD + "\tfalse\n");
			pw.write(Constants.HIDDEN_DATABASE + "\tfalse\n");
			pw.write(Constants.OWL + "\tdb" + System.getProperty("file.separator") + "@engine@"
					+ System.getProperty("file.separator") + "@engine@_OWL.OWL" + "\n");

			// custom tinker props
			pw.write(Constants.TINKER_FILE + "\t" + tinkerFilePath + "\n");
			pw.write(Constants.TINKER_DRIVER + "\t" + tinkerDriverType + "\n");
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
