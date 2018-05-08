package prerna.sablecc2.reactor.algorithms.xray;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.Utility;

public class XrayMetamodelReactor extends AbstractRFrameReactor {
	private static final String CLASS_NAME = XrayMetamodelReactor.class.getName();

	public XrayMetamodelReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.CONFIG.getKey() };
	}

	@Override
	public NounMetadata execute() {
		init();
		Logger logger = getLogger(CLASS_NAME);

		// need to make sure that the textreuse package is installed
		logger.info("Checking if required R packages are installed to run X-ray...");
		this.rJavaTranslator.checkPackages(new String[] { "textreuse", "digest", "memoise", "withr", "jsonlite" });

		organizeKeys();
		// get metamodel
		System.out.println("");
		GenRowStruct grs = this.store.getNoun(keysToGet[0]);
		Map<String, Object> config = null;
		if (grs != null && !grs.isEmpty()) {
			config = (Map<String, Object>) grs.get(0);
		} else {
			throw new IllegalArgumentException("Need to define " + ReactorKeysEnum.CONFIG.getKey());
		}

		Xray xray = new Xray(this.rJavaTranslator, getBaseFolder(), logger);
		String rFrameName = xray.run(config);

		// check if we have results from xray
		String checkNull = "is.null(" + rFrameName + ")";
		boolean nullResults = this.rJavaTranslator.getBoolean(checkNull);

		// if we have results sync back SEMOSS
		if (!nullResults) {

			// format xray results into json
			String jsonR = "json" + Utility.getRandomString(5);
			String tempFrame = "temp" + Utility.getRandomString(8);
			String formatScript = tempFrame + " <- subset(" + rFrameName
					+ ", select=c(Source_Database, Source_Table, Source_Column, Source_Property, Target_Database, Target_Table, Target_Column, Target_Property));";
			formatScript += "library(jsonlite);";
			formatScript += jsonR + " <- toJSON(" + tempFrame + ", byrow = TRUE, colNames = TRUE); ";
			this.rJavaTranslator.runR(formatScript);
			String json = this.rJavaTranslator.getString(jsonR);

			List<Map> jsonMap = new ArrayList<Map>();
			if (json != null) {
				try {
					// parse json here
					jsonMap = new ObjectMapper().readValue(json, List.class);
				} catch (IOException e2) {
				}
			} else {
				throw new IllegalArgumentException("No collisions found");
			}

			// get database - concept list to get node properties for metamodel
			Set<String> conceptHash = new HashSet<String>();
			String dbConceptDelimiter = "%%%" + Utility.getRandomString(5) + "%%%";
			for (Map map : jsonMap) {
				for (Object key : map.keySet()) {
					String sourceDB = (String) map.get("Source_Database");
					String sourceTable = (String) map.get("Source_Table");
					conceptHash.add(sourceDB + dbConceptDelimiter + sourceTable);
					String targetDB = (String) map.get("Target_Database");
					String targetTable = (String) map.get("Target_Table");
					conceptHash.add(targetDB + dbConceptDelimiter + targetTable);
					String value = (String) map.get(key);
					System.out.println(key + " : " + value);

				}
				System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
			}
			// get node properties
			List<Map> nodeList = new ArrayList<>();

			for (String conceptDBKey : conceptHash) {
				Map<String, Object> nodeMap = new HashMap<>();
				String db = conceptDBKey.split(dbConceptDelimiter)[0];
				String concept = conceptDBKey.split(dbConceptDelimiter)[1];
				System.out.println("DB: " + db);
				System.out.println("Concept: " + concept);
				nodeMap.put("keySet", new ArrayList<>());
				nodeMap.put("conceptualName", concept);
				nodeMap.put("database", db);
				Map<String, List<String>> propsMap = MasterDatabaseUtility.getSpecificConceptPropertiesRDBMS(concept,
						db);
				for (String key : propsMap.keySet()) {
					List<String> props = propsMap.get(key);
					nodeMap.put("propSet", props);
				}
				nodeList.add(nodeMap);
			}
			// clean up r temp variables
			StringBuilder cleanUpScript = new StringBuilder();
			cleanUpScript.append("rm(" + rFrameName + ");");
			cleanUpScript.append("rm(" + jsonR + ");");
			cleanUpScript.append("rm(" + tempFrame + ");");
			cleanUpScript.append("gc();");
			this.rJavaTranslator.runR(cleanUpScript.toString());
			Map<String, Object> retMap = new HashMap<>();
			retMap.put("edges", jsonMap);
			retMap.put("nodes", nodeList);
			return new NounMetadata(retMap, PixelDataType.MAP);

		}
		// clean up r temp variables
		StringBuilder cleanUpScript = new StringBuilder();
		cleanUpScript.append("rm(" + rFrameName + ");");
		cleanUpScript.append("gc();");
		this.rJavaTranslator.runR(cleanUpScript.toString());
		NounMetadata noun = new NounMetadata("Unable to obtain X-ray results", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
		SemossPixelException exception = new SemossPixelException(noun);
		exception.setContinueThreadOfExecution(false);
		throw exception;
	}

}
