package prerna.om;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import com.google.gson.Gson;

import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.test.TestUtilityMethods;
import prerna.util.DIHelper;
import prerna.util.gson.GsonUtility;

public class InsightCachingTest {

	private static final Gson GSON = GsonUtility.getDefaultGson();
	
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
				"AddPanel(0);",
				"Panel(0)|AddPanelEvents({\"onSingleClick\":{\"Unfilter\":[{\"panel\":\"\",\"query\":\"<encode>UnfilterFrame(<SelectedColumn>);</encode>\",\"options\":{},\"refresh\":false,\"default\":true,\"disabledVisuals\":[\"Grid\",\"Sunburst\"],\"disabled\":false}]},\"onBrush\":{\"Filter\":[{\"panel\":\"\",\"query\":\"<encode>if(IsEmpty(<SelectedValues>), UnfilterFrame(<SelectedColumn>), SetFrameFilter(<SelectedColumn>==<SelectedValues>));</encode>\",\"options\":{},\"refresh\":false,\"default\":true,\"disabled\":false}]}});Panel(0)|RetrievePanelEvents();Panel(0)|SetPanelView(\"visualization\", \"<encode>{\"type\":\"echarts\"}</encode>\");Panel(0)|SetPanelView(\"federate-view\", \"<encode>{\"app_id\":\"93857bba-5aea-447b-94f4-f9d9179da4da\"}</encode>\");",
				"Panel(999)|AddPanelEvents({\"onSingleClick\":{\"Unfilter\":[{\"panel\":\"\",\"query\":\"<encode>UnfilterFrame(<SelectedColumn>);</encode>\",\"options\":{},\"refresh\":false,\"default\":true,\"disabledVisuals\":[\"Grid\",\"Sunburst\"],\"disabled\":false}]},\"onBrush\":{\"Filter\":[{\"panel\":\"\",\"query\":\"<encode>if(IsEmpty(<SelectedValues>), UnfilterFrame(<SelectedColumn>), SetFrameFilter(<SelectedColumn>==<SelectedValues>));</encode>\",\"options\":{},\"refresh\":false,\"default\":true,\"disabled\":false}]}});Panel(0)|RetrievePanelEvents();Panel(0)|SetPanelView(\"visualization\", \"<encode>{\"type\":\"echarts\"}</encode>\");Panel(0)|SetPanelView(\"federate-view\", \"<encode>{\"app_id\":\"93857bba-5aea-447b-94f4-f9d9179da4da\"}</encode>\");",
				"abc = CreateFrame(grid).as([frame1]);",
				"Database(\"" + coreName.split("__")[1] + "\") | Select(Title) | Import();",
				"Frame() | QueryAll() | Collect(-1);",
				"x = 5;",
				"y = Select(Title);"
		};
		
		in.runPixel(Arrays.asList(pixel));
		
		String folderDir = "C:\\workspace\\testSave";
		InsightCacheUtility.cacheInsight(in, folderDir);
		Insight newIn = InsightCacheUtility.readInsightCache(folderDir);
		printInsightDetails(newIn);
	}
	
	public static void printInsightDetails(Insight in) {
		VarStore newVarStore = in.getVarStore();
		Set<String> keys = newVarStore.getKeys();
		System.out.println("VarStore");
		System.out.println(">>>");
		System.out.println(">>>");
		for(String k : keys) {
			NounMetadata noun = newVarStore.get(k);
			if(noun.getNounType() == PixelDataType.FRAME) {
				System.out.println(k + " ::: " + newVarStore.get(k));
			} else if(noun.getNounType() == PixelDataType.TASK) {
				System.out.println(k + " ::: " + newVarStore.get(k));
			} else {
				System.out.println(k + " ::: " + GSON.toJson(newVarStore.get(k)));
			}
		}
		
		System.out.println("Insight Panels");
		System.out.println(">>>");
		System.out.println(">>>");
		Map<String, InsightPanel> panels = in.getInsightPanels();
		System.out.println(GSON.toJson(panels));
	}
	
}
