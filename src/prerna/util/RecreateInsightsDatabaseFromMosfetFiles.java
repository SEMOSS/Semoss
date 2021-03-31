package prerna.util;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import prerna.engine.impl.InsightAdministrator;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.om.MosfetFile;
import prerna.sablecc2.reactor.app.upload.UploadUtilities;
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
		insightEngine.setProp(prop);
		// opening will work since we directly injected the prop map
		// this way i do not need to write it to disk and then recreate it later
		insightEngine.openDB(null);
		insightEngine.setBasic(true);
		UploadUtilities.runInsightCreateTableQueries(insightEngine);
		
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
					
					String appId = mosfet.getEngineId();
					String id = mosfet.getRdbmsId();
					String insightName = mosfet.getInsightName();
					String layout = mosfet.getLayout();
					List<String> recipe = mosfet.getRecipe();
					boolean hidden = mosfet.isHidden();

					InsightAdministrator admin = new InsightAdministrator(insightEngine);
					// just put the recipe into an array
					admin.addInsight(id, insightName, layout, recipe, hidden, true);
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
