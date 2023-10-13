package prerna.reactor.algorithms.xray;
//package prerna.sablecc2.reactor.algorithms.xray;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.Hashtable;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.Vector;
//
//import org.apache.logging.log4j.Logger;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//
//import prerna.ds.r.RSyntaxHelper;
//import prerna.nameserver.utility.MasterDatabaseUtility;
//import prerna.sablecc2.om.GenRowStruct;
//import prerna.sablecc2.om.PixelDataType;
//import prerna.sablecc2.om.ReactorKeysEnum;
//import prerna.sablecc2.om.nounmeta.NounMetadata;
//import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
//import prerna.util.Utility;
//
//public class XrayMetamodelReactor extends AbstractRFrameReactor {
//	private static final String CLASS_NAME = XrayMetamodelReactor.class.getName();
//
//	public XrayMetamodelReactor() {
//		this.keysToGet = new String[] { ReactorKeysEnum.CONFIG.getKey() };
//	}
//
//	@Override
//	public NounMetadata execute() {
//		init();
//		Logger logger = getLogger(CLASS_NAME);
//		// need to make sure that the textreuse package is installed
//		logger.info("Checking if required R packages are installed to run X-ray...");
//		String[] packages = new String[] { "jsonlite" };
//		this.rJavaTranslator.checkPackages(packages);
//		organizeKeys();
//		// get metamodel
//		GenRowStruct grs = this.store.getNoun(keysToGet[0]);
//		Map<String, Object> config = null;
//		if (grs != null && !grs.isEmpty()) {
//			config = (Map<String, Object>) grs.get(0);
//		} else {
//			throw new IllegalArgumentException("Need to define " + ReactorKeysEnum.CONFIG.getKey());
//		}
//		Xray xray = new Xray(this.rJavaTranslator, getBaseFolder(), logger);
//		xray.setGenerateCountFrame(true);
//		String rFrameName = xray.run(config);
//
//		// check if we have results from xray
//		String checkNull = " is.null(" + rFrameName + ")";
//		boolean nullResults = this.rJavaTranslator.getBoolean(checkNull);
//
//		Set<String> dbList = xray.getEngineList();
//		Hashtable edgesTable = new Hashtable();
//		Hashtable conceptsTable = new Hashtable();
//		for (String db : dbList) {
//			Boolean existingMetamodel = new Boolean(((Map) config.get("parameters")).get("metamodel").toString());
//			Map<String, Object> metamodelObject = MasterDatabaseUtility.getXrayExisitingMetamodelRDBMS(db);
//			Hashtable edgesList1 = (Hashtable) metamodelObject.get("edges");
//			Hashtable concepts = (Hashtable) metamodelObject.get("nodes");
//			if (existingMetamodel) {
//				edgesTable.putAll(edgesList1);
//			}
//			conceptsTable.putAll(concepts);
//		}
//
//		List<Map<String, Object>> edgesList = new Vector(edgesTable.values());
//		// this does not need to be modified maybe flush out only important concepts???????
//		List<Map<String, Object>> concepts2 = new Vector(conceptsTable.values());
//
//		// if we have results sync back SEMOSS
//		if (!nullResults) {
//			// format xray results into json
//			String countFrame = xray.getCountDF();
//			String jsonR = "json" + Utility.getRandomString(5);
//			String tempFrame = "temp" + Utility.getRandomString(8);
//			StringBuilder rsb = new StringBuilder();
//			// xray remove a-b duplicate b-a match
//			rsb.append(tempFrame + "<- " + rFrameName + ";\n");
//			String nR = "n" + Utility.getRandomString(5);
//			rsb.append(nR + "<- nrow(" + tempFrame + ");\n");
//			String vR = "v" + Utility.getRandomString(5);
//			rsb.append(vR + "<- vector();\n");
//			String iR = "i" + Utility.getRandomString(5);
//			rsb.append("for(" + iR + " in 1:" + nR + "){\n");
//
//			rsb.append("src <-paste0(" + tempFrame + "$Source_Database_Id[" + iR + "],\"%\"," + tempFrame
//					+ "$Source_Table[" + iR + "],\"%\"," + tempFrame + "$Source_Property[" + iR + "]);\n");
//			rsb.append("trg <-paste0(" + tempFrame + "$Target_Database_Id[" + iR + "],\"%\"," + tempFrame
//					+ "$Target_Table[" + iR + "],\"%\"," + tempFrame + "$Target_Property[" + iR + "]);\n");
//			rsb.append("if(src < trg){\n");
//			rsb.append(vR + "[" + iR + "]<-paste0(src,\"%\",trg);\n");
//			rsb.append("}else{\n");
//			rsb.append(vR + "[" + iR + "]<-paste0(trg,\"%\",src);\n");
//			rsb.append("}\n rm(src);\n rm(trg);\n");
//			rsb.append("}\n");
//			rsb.append(tempFrame + "$id <- " + vR + ";\n");
//			rsb.append(tempFrame + "<- " + tempFrame + "[order(" + tempFrame + "$id),];\n");
//			rsb.append(tempFrame + "<-" + tempFrame + "[!duplicated(" + tempFrame + "$id),];\n");
//			rsb.append(tempFrame + " <- subset(" + tempFrame
//					+ ", select=c(Source_Database_Id, Source_Database, Source_Table, Source_Column, Source_Property, Target_Database_Id,Target_Database, "
//					+ "Target_Table, Target_Column, Target_Property, Source_Instances, Target_Instances));");
//
//			// get instance count for source
//			rsb.append(tempFrame + "<- merge(" + tempFrame + ", " + countFrame
//					+ ", by.x=c(\"Source_Database_Id\", \"Source_Table\", \"Source_Column\"), by.y=c(\"engine\", \"table\",\"prop\"));");
//			// rename count column for source
//			rsb.append("names(" + tempFrame + ")[names(" + tempFrame + ") == 'count'] <- 'Source_Count';");
//			// get instance count for target
//			rsb.append(tempFrame + " <- merge(" + tempFrame + ", " + countFrame
//					+ ", by.x=c(\"Target_Database_Id\", \"Target_Table\", \"Target_Column\"), by.y=c(\"engine\", \"table\",\"prop\"));");
//			// rename count column for target
//			rsb.append("names(" + tempFrame + ")[names(" + tempFrame + ") == 'count'] <- 'Target_Count';");
//			// add PK or FK for source
//			rsb.append(tempFrame + "$Source_Key <- apply(" + tempFrame
//					+ ", 1, function(row) {ifelse(row[9] == row[11], \"PK\",\"FK\")});");
//			// add PK or FK for target
//			rsb.append(tempFrame + "$Target_Key <- apply(" + tempFrame
//					+ ", 1, function(row) {ifelse(row[10] == row[12], \"PK\",\"FK\")});");
//			// remove FK source and FK target
//			rsb.append(tempFrame + "[, c(\"Source_Key\",\"Target_Key\")][" + tempFrame + "$Source_Key == \"FK\" & "
//					+ tempFrame + "$Target_Key == \"FK\"] <- \"\";");
//			// eliminate extra columns
//			rsb.append(tempFrame + " <- subset(" + tempFrame
//					+ ", select=c(Source_Database_Id, Source_Database, Source_Table, Source_Column,Source_Property, Source_Key, "
//					+ "Target_Database_Id ,Target_Database, Target_Table, Target_Column,Target_Property, Target_Key));");
//			// get json
//			rsb.append(RSyntaxHelper.loadPackages(packages));
//			rsb.append(jsonR + " <-  toJSON(" + tempFrame + ", byrow = TRUE, colNames = TRUE); ");
//
//			this.rJavaTranslator.runR(rsb.toString());
//			String json = this.rJavaTranslator.getString(jsonR);
//			List<Map> jsonMap = new ArrayList<Map>();
//			if (json != null) {
//				try {
//					// parse json here
//					jsonMap = new ObjectMapper().readValue(json, List.class);
//				} catch (IOException e2) {
//				}
//			}
//
//			String delim = "-";
//			for (Map map : jsonMap) {
//				Map<String, Object> edgeMap = new HashMap<>();
//				String sourceDB = (String) map.get("Source_Database");
//				String sourceTable = (String) map.get("Source_Table");
//				// need to get property value
//				String sourceProperty = (String) map.get("Source_Property");
//				String sourceEdge = sourceDB + delim + sourceTable;
//				if (sourceProperty != null) {
//					// property
//					sourceEdge += delim + sourceProperty;
//				} else {
//					// concept
//					sourceEdge += delim + sourceTable;
//				}
//
//				String sourceKey = (String) map.get("Source_Key");
//				String targetDB = (String) map.get("Target_Database");
//				String targetTable = (String) map.get("Target_Table");
//				String targetProperty = (String) map.get("Target_Property");
//				String targetEdge = targetDB + delim + targetTable;
//				if (targetProperty != null) {
//					// property
//					targetEdge += delim + targetProperty;
//				} else {
//					// concept
//					targetEdge += delim + targetTable;
//				}
//
//				String targetKey = (String) map.get("Target_Key");
//
//				String edgeKey = sourceEdge + delim + targetEdge;
//				// check if xray edge exists
//				if (!edgesTable.contains(edgeKey)) {
//					Map<String, Object> xrayEdge = new HashMap();
//					xrayEdge.put("source", sourceEdge);
//					xrayEdge.put("sourceKey", sourceKey);
//					xrayEdge.put("target", targetEdge);
//					xrayEdge.put("targetKey", targetKey);
//					xrayEdge.put("xray", true);
//					edgesList.add(xrayEdge);
//				}
//			}
//			// clean up r temp variables
//			StringBuilder cleanUpScript = new StringBuilder();
//			cleanUpScript.append("rm(" + rFrameName + ");");
//			cleanUpScript.append("rm(" + jsonR + ");");
//			cleanUpScript.append("rm(" + tempFrame + ");");
//			cleanUpScript.append("rm(" + countFrame + ");");
//			cleanUpScript.append("gc();");
//			this.rJavaTranslator.runR(cleanUpScript.toString());
//
//		}
//		Map<String, Object> retMap = new HashMap<>();
//		retMap.put("edges", edgesList);
//		retMap.put("nodes", conceptsTable.values());
//		return new NounMetadata(retMap, PixelDataType.MAP);
//	}
//
//}
