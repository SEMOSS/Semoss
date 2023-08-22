package prerna.util;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;

import prerna.engine.impl.InsightAdministrator;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.om.MosfetFile;
import prerna.util.sql.RdbmsTypeEnum;

public class RecreateInsightsDatabaseFromMosfetFiles {

	public static void main(String[] args) throws IOException {
		String mainDirectory = null;
		String connUrl = null;
		if(args.length == 0) {
			mainDirectory = "C:/Users/mahkhalil/Desktop/review123/nogit_version_no_cache";
			connUrl = "jdbc:h2:C:\\Users\\mahkhalil\\Desktop\\review123\\insights_database";
		} else {
			mainDirectory = args[0];
			connUrl = args[1];
		}
		build(mainDirectory, connUrl);
	}

	private static void build(String mainDirectory, String connectionUrl) throws IOException {
		Properties prop = new Properties();
		prop.put(Constants.CONNECTION_URL, connectionUrl);
		prop.put(Constants.USERNAME, "sa");
		prop.put(Constants.PASSWORD, "");
		prop.put(Constants.DRIVER, RdbmsTypeEnum.H2_DB.getDriver());
		prop.put(Constants.RDBMS_TYPE, RdbmsTypeEnum.H2_DB.getLabel());
		prop.put("TEMP", "TRUE");
		RDBMSNativeEngine insightEngine = new RDBMSNativeEngine();
		insightEngine.setSmssProp(prop);
		// opening will work since we directly injected the prop map
		// this way i do not need to write it to disk and then recreate it later
		insightEngine.open(null);
		insightEngine.setBasic(true);
		SmssUtilities.runInsightCreateTableQueries(insightEngine);
		
		// main directory has insight folders inside of it
		File mainD = new File(Utility.normalizePath(mainDirectory));
		File[] mainDFiles = mainD.listFiles();
		INSIGHT_FOLDER : for(File insightFolder : mainDFiles) {
			// only care about insight folders
			if(!insightFolder.isDirectory()) {
				continue;
			}
			
			// get all the files inside the insight folder
			File[] insightFiles = insightFolder.listFiles();
			if(insightFiles.length == 0) {
				System.out.println("Insight had no mosfet = " + insightFolder.getName());
				continue INSIGHT_FOLDER;
			}
			
			boolean hasMosfet = false;
			for(File insightFile : insightFiles) {
				String fName = insightFile.getName();
				if(fName.equals(".mosfet")) {
					System.out.println("Loading mosfet file");
					hasMosfet = true;
					// we have a mosfet file to load
					MosfetFile mosfet = MosfetFile.generateFromFile(insightFile);
					
					String projectId = mosfet.getProjectId();
					String id = mosfet.getRdbmsId();
					String insightName = mosfet.getInsightName();
					String layout = mosfet.getLayout();
					List<String> recipe = mosfet.getRecipe();
					boolean global = mosfet.isGlobal();
					boolean cacheable = mosfet.isCacheable();
					int cacheMinutes = mosfet.getCacheMinutes();
					String cacheCron = mosfet.getCacheCron();
					LocalDateTime cachedOn = mosfet.getCachedOn();
					boolean cacheEncrypt = mosfet.isCacheEncrypt();
					String schemaName = mosfet.getSchemaName();
					
					InsightAdministrator admin = new InsightAdministrator(insightEngine);
					// just put the recipe into an array
					admin.addInsight(id, insightName, layout, recipe, global, cacheable, cacheMinutes, cacheCron, cachedOn, cacheEncrypt, schemaName);
				} else {
					System.out.println("Found file in insight = " + fName);
				}
			}
			
			if(!hasMosfet) {
				System.out.println("Insight had folder but no mosfet = " + insightFolder.getName());
			}
		}
	}
	
}
