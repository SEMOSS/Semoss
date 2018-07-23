package prerna.om;

import java.util.Arrays;
import java.util.Set;

import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.sablecc2.om.VarStore;
import prerna.test.TestUtilityMethods;
import prerna.util.DIHelper;

public class InsightCachingTest {

	public static void main(String[] args) {
		TestUtilityMethods.loadAll("C:\\workspace\\Semoss_Dev\\RDF_Map.prop");

		String coreName = "MovieDatabase__93857bba-5aea-447b-94f4-f9d9179da4da";
		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\" + coreName + ".smss";
		IEngine coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineId(coreName.split("__")[1]);
		coreEngine.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty(coreName.split("__")[1], coreEngine);
		
		Insight in = new Insight();
		in.setEngineId("testing1");
		in.setRdbmsId("testingId2");
		InsightStore.getInstance().put(in);
		
		String[] pixel = new String[]{
				"abc = CreateFrame(grid).as([frame1]);",
				"Database(\"" + coreName.split("__")[1] + "\") | Select(Title) | Import();",
				"Frame() | QueryAll() | Collect(-1);",
				"x = 5;",
				"y = Select(Title);"
		};
		
		in.runPixel(Arrays.asList(pixel));
		
		String folderDir = "C:\\workspace\\testSave";
		InsightCacheUtility.cacheInsight(in, folderDir);
		
//		ITableDataFrame frame = (ITableDataFrame) in.getDataMaker();
//		frame.save(folderDir);
		
		Insight newIn = InsightCacheUtility.readInsightCache(folderDir);
		VarStore newVarStore = newIn.getVarStore();
		Set<String> keys = newVarStore.getKeys();
		for(String k : keys) {
			System.out.println(k + " ::: " + newVarStore.get(k));
		}
	}
	
}
