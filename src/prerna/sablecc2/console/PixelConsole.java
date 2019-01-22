package prerna.sablecc2.console;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.sablecc2.PixelRunner;
import prerna.test.TestUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.gson.GsonUtility;

public class PixelConsole {

	private static Gson gson = GsonUtility.getDefaultGson();
	
	public static void main(String[] args) throws Exception{
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
				System.out.println("Enter new pixel command ");
				String pixel = reader.readLine();   
				pixel = pixel.trim();
				if(!pixel.isEmpty()) {
//					if(!pixel.endsWith(";")) {
//						pixel = pixel + ";";
//					}
					long start = System.currentTimeMillis();
					PixelRunner returnData = run(insight, pixel);
					System.out.println(gson.toJson(returnData.getResults()));
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

	public static PixelRunner run(Insight insight, String pixel) {
		return insight.runPixel(pixel);
	}

	public static void loadEngines() throws Exception {
		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\LocalMasterDatabase.smss";
		IEngine coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineId(Constants.LOCAL_MASTER_DB_NAME);
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
		

//		engineProp = "C:\\workspace\\Semoss_Dev\\db\\Forms_TAP_Core_Data.smss";
//		coreEngine = new BigDataEngine();
//		coreEngine.setEngineName("Forms_TAP_Core_Data");
//		coreEngine.openDB(engineProp);
//		DIHelper.getInstance().setLocalProperty("Forms_TAP_Core_Data", coreEngine);

		engineProp = "C:\\workspace\\Semoss_Dev\\db\\Movie_RDBMS.smss";
		coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineId("Movie_RDBMS");
		coreEngine.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty("Movie_RDBMS", coreEngine);
		
		engineProp = "C:\\workspace\\Semoss_Dev\\db\\Movie_RDF.smss";
		coreEngine = new BigDataEngine();
		coreEngine.setEngineId("Movie_RDF");
		coreEngine.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty("Movie_RDF", coreEngine);
//		
//		engineProp = "C:\\workspace\\Semoss_Dev\\db\\TAP_Core_Data.smss";
//		coreEngine = new BigDataEngine();
//		coreEngine.setEngineName("TAP_Core_Data");
//		coreEngine.openDB(engineProp);
//		DIHelper.getInstance().setLocalProperty("TAP_Core_Data", coreEngine);
	}
}

/**
 * Generation of new NumberAdaptor to not send NaN/Infinity to the FE
 * since they are invalid JSON values
 */
class NumberAdaptor extends TypeAdapter<Double>{

	@Override 
	public Double read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}
		return in.nextDouble();
	}

	@Override 
	public void write(JsonWriter out, Double value) throws IOException {
		if (value == null) {
			out.nullValue();
			return;
		}
		double doubleValue = value.doubleValue();
		if(Double.isNaN(doubleValue) || Double.isInfinite(doubleValue)) {
			out.nullValue();
		} else {
			out.value(value);
		}
	}
}

