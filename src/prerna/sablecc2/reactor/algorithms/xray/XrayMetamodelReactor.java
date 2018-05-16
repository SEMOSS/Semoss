package prerna.sablecc2.reactor.algorithms.xray;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
		GenRowStruct grs = this.store.getNoun(keysToGet[0]);
		Map<String, Object> config = null;
		if (grs != null && !grs.isEmpty()) {
			config = (Map<String, Object>) grs.get(0);
		} else {
			throw new IllegalArgumentException("Need to define " + ReactorKeysEnum.CONFIG.getKey());
		}
		Xray xray = new Xray(this.rJavaTranslator, getBaseFolder(), logger);
		xray.setGenerateCountFrame(true);
		String rFrameName = xray.run(config);

		// check if we have results from xray
		String checkNull = "is.null(" + rFrameName + ")";
		boolean nullResults = this.rJavaTranslator.getBoolean(checkNull);

		// if we have results sync back SEMOSS
		if (!nullResults) {
			// format xray results into json
			String countFrame = xray.getCountDF();
			String jsonR = "json" + Utility.getRandomString(5);
			String tempFrame = "temp" + Utility.getRandomString(8);
			StringBuilder rsb = new StringBuilder();
			rsb.append(tempFrame + " <- subset(" + rFrameName
					+ ", select=c(Source_Database, Source_Table, Source_Column, Source_Property, Target_Database, Target_Table, Target_Column, Target_Property, Source_Instances, Target_Instances));");
			// remove bidirectional comparison
			rsb.append(tempFrame+" <- "+tempFrame+"[1:(nrow("+tempFrame+") /2),];");
			// get instance count for source
			rsb.append(tempFrame + "<- merge(" + tempFrame + ", " + countFrame
					+ ", by.x=c(\"Source_Database\", \"Source_Table\", \"Source_Column\"), by.y=c(\"engine\", \"table\",\"prop\"));");
			// rename count column for source
			rsb.append("names(" + tempFrame + ")[names(" + tempFrame + ") == 'count'] <- 'Source_Count';");
			// get instance count for target
			rsb.append(tempFrame + " <- merge(" + tempFrame + ", " + countFrame
					+ ", by.x=c(\"Target_Database\", \"Target_Table\", \"Target_Column\"), by.y=c(\"engine\", \"table\",\"prop\"));");
			// rename count column for target
			rsb.append("names(" + tempFrame + ")[names(" + tempFrame + ") == 'count'] <- 'Target_Count';");
			// add PK or FK for source
			rsb.append(tempFrame + "$Source_Key <- apply(" + tempFrame + ", 1, function(row) {ifelse(row[11] == row[9], \"PK\",\"FK\")});");
			// add PK or FK for target
			rsb.append(tempFrame + "$Target_Key <- apply(" + tempFrame + ", 1, function(row) {ifelse(row[10] == row[12], \"PK\",\"FK\")});");
			// remove FK source and FK target
			rsb.append(tempFrame + "[, c(\"Source_Key\",\"Target_Key\")][" + tempFrame + "$Source_Key == \"FK\" & " + tempFrame + "$Target_Key == \"FK\"] <- \"\";");
			// eliminate extra columns
			rsb.append(tempFrame + " <- subset(" + tempFrame + ", select=c(Source_Database, Source_Table, Source_Column, Source_Key, Target_Database, Target_Table, Target_Column, Target_Key));");
			// get json
			rsb.append("library(jsonlite);");
			rsb.append(jsonR + " <- toJSON(" + tempFrame + ", byrow = TRUE, colNames = TRUE); ");
			
			this.rJavaTranslator.runR(rsb.toString());
			System.out.println(rsb.toString() + "");
			String json = this.rJavaTranslator.getString(jsonR);
			List<Map> jsonMap = new ArrayList<Map>();
			if (json != null) {
				try {
					// parse json here
					jsonMap = new ObjectMapper().readValue(json, List.class);
				} catch (IOException e2) {
				}
			} else {
				// TODO rScript clean up 
				throw new IllegalArgumentException("No collisions found");
			}

			Map<String, Object> dbMap = new HashMap<>();
			for (Map map : jsonMap) {
				for (Object key : map.keySet()) {
					String sourceDB = (String) map.get("Source_Database");
					String sourceTable = (String) map.get("Source_Table");
					Map<String, Object> dbConcepts = null;
					if (dbMap.containsKey(sourceDB)) {
						dbConcepts = (Map<String, Object>) dbMap.get(sourceDB);
						if (!dbConcepts.containsKey(sourceTable)) {
							dbConcepts.put(sourceTable, MasterDatabaseUtility
									.getSpecificConceptPropertiesRDBMS(sourceTable, sourceDB).get(sourceDB));
						}
					} else {
						dbConcepts = new HashMap<>();
						dbConcepts.put(sourceTable, MasterDatabaseUtility
								.getSpecificConceptPropertiesRDBMS(sourceTable, sourceDB).get(sourceDB));
					}
					dbMap.put(sourceDB, dbConcepts);
					String targetDB = (String) map.get("Target_Database");
					String targetTable = (String) map.get("Target_Table");
					if (dbMap.containsKey(targetDB)) {
						dbConcepts = (Map<String, Object>) dbMap.get(targetDB);
						if (!dbConcepts.containsKey(targetTable)) {
							dbConcepts.put(targetTable, MasterDatabaseUtility.getSpecificConceptPropertiesRDBMS(targetTable, targetDB).get(targetDB));
						}
					} else {
						dbConcepts = new HashMap<>();
						dbConcepts.put(targetTable, MasterDatabaseUtility.getSpecificConceptPropertiesRDBMS(targetTable, targetDB).get(targetDB));
					}
					dbMap.put(targetDB, dbConcepts);
				}
			}
			// clean up r temp variables
			StringBuilder cleanUpScript = new StringBuilder();
			cleanUpScript.append("rm(" + rFrameName + ");");
			cleanUpScript.append("rm(" + jsonR + ");");
			cleanUpScript.append("rm(" + tempFrame + ");");
			cleanUpScript.append("rm(" + countFrame + ");");
			cleanUpScript.append("gc();");
			this.rJavaTranslator.runR(cleanUpScript.toString());
			Map<String, Object> retMap = new HashMap<>();
			retMap.put("edges", jsonMap);
			retMap.put("nodes", dbMap);
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
