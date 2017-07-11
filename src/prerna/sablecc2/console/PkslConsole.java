package prerna.sablecc2.console;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.test.TestUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class PkslConsole {

	private static Gson gson = new GsonBuilder().setPrettyPrinting().create();

	public static void main(String[] args){
		TestUtilityMethods.loadDIHelper();
		loadEngines();

		Insight insight = new Insight();
		InsightStore.getInstance().put(insight);
		Thread thread = new Thread(){
			public void run()
			{
				openCommandLine(insight);				
			}
		};
		thread.start();
	}

	public static void openCommandLine(Insight insight)
	{
		String end = "";
		while(!end.equalsIgnoreCase("end"))
		{
			try {
				BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
				System.out.println("Enter new pksl command ");
				String pksl = reader.readLine();   
				if(pksl != null && !pksl.isEmpty()) {
					if(!pksl.endsWith(";")) {
						pksl = pksl + ";";
					}
					long start = System.currentTimeMillis();
					Map<String, Object> returnData = run(insight, pksl);
					System.out.println(gson.toJson(returnData));
					long time2 = System.currentTimeMillis();
					System.out.println("Execution time : " + (time2 - start )+ " ms");
				} else {
					end = "end";
				}
			} catch(RuntimeException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static Map<String, Object> run(Insight insight, String pksl) {
		return insight.runPksl(pksl);
	}

	public static void loadEngines() {
		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\LocalMasterDatabase.smss";
		IEngine coreEngine = new BigDataEngine();
		coreEngine.setEngineName(Constants.LOCAL_MASTER_DB_NAME);
		coreEngine.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty(Constants.LOCAL_MASTER_DB_NAME, coreEngine);
		
//		engineProp = "C:\\workspace\\Semoss_Dev\\db\\MinInput.smss";
//		coreEngine = new RDBMSNativeEngine();
//		coreEngine.setEngineName("MinInput");
//		coreEngine.openDB(engineProp);
//		DIHelper.getInstance().setLocalProperty("MinInput", coreEngine);
//		
//		engineProp = "C:\\workspace\\Semoss_Dev\\db\\MinImpact.smss";
//		coreEngine = new RDBMSNativeEngine();
//		coreEngine.setEngineName("MinImpact");
//		coreEngine.openDB(engineProp);
//		DIHelper.getInstance().setLocalProperty("MinImpact", coreEngine);
		

		engineProp = "C:\\workspace\\Semoss_Dev\\db\\Movie_RDBMS.smss";
		coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineName("Movie_RDBMS");
		coreEngine.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty("Movie_RDBMS", coreEngine);
	}
}
