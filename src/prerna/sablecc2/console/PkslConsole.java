package prerna.sablecc2.console;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.sablecc2.PKSLRunner;
import prerna.sablecc2.om.NounMetadata;
import prerna.test.TestUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class PkslConsole {

	public static void main(String[] args){
		TestUtilityMethods.loadDIHelper();
//		loadEngines();

		PKSLRunner runner = new PKSLRunner();
		Thread thread = new Thread(){
			public void run()
			{
				openCommandLine(runner);				
			}
		};
		thread.start();
	}

	public static void openCommandLine(PKSLRunner runner)
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
					NounMetadata returnData = run(runner, pksl);
					System.out.println(">>> " + returnData.getValue());
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

	public static NounMetadata run(PKSLRunner runner, String pksl) {
		runner.runPKSL(pksl);
		return runner.getLastResult();
	}

	public static void loadEngines() {
		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\LocalMasterDatabase.smss";
		IEngine coreEngine = new BigDataEngine();
		coreEngine.setEngineName(Constants.LOCAL_MASTER_DB_NAME);
		coreEngine.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty(Constants.LOCAL_MASTER_DB_NAME, coreEngine);

		engineProp = "C:\\workspace\\Semoss_Dev\\db\\Movie_RDBMS.smss";
		coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineName("Movie_RDBMS");
		coreEngine.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty("Movie_RDBMS", coreEngine);
	}
}
