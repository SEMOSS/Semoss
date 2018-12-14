package prerna.om;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import com.google.gson.Gson;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
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
		in.setEngineId(coreName.split("__")[1]);
		in.setEngineName(coreName.split("__")[0]);
		in.setRdbmsId("testing");
		
		InsightStore.getInstance().put(in);
		
		String[] pixel = new String[]{
				"AddPanel ( 0 ) ;",
				"Panel ( 0 ) | AddPanelEvents ( { \"onSingleClick\" : { \"Unfilter\" : [ { \"panel\" : \"\" , \"query\" : \"<encode>UnfilterFrame(<SelectedColumn>);</encode>\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabledVisuals\" : [ \"Grid\" , \"Sunburst\" ] , \"disabled\" : false } ] } , \"onBrush\" : { \"Filter\" : [ { \"panel\" : \"\" , \"query\" : \"<encode>if(IsEmpty(<SelectedValues>), UnfilterFrame(<SelectedColumn>), SetFrameFilter(<SelectedColumn>==<SelectedValues>));</encode>\" , \"options\" : { } , \"refresh\" : false , \"default\" : true , \"disabled\" : false } ] } } ) ;",
				"Panel ( 0 ) | RetrievePanelEvents ( ) ;",
				"Panel ( 0 ) | SetPanelView ( \"visualization\" , \"<encode>{\"type\":\"echarts\"}</encode>\" ) ;",
				"Panel ( 0 ) | SetPanelView ( \"federate-view\" , \"<encode>{\"app_id\":\"93857bba-5aea-447b-94f4-f9d9179da4da\"}</encode>\" ) ;",
				"CreateFrame ( frameType = [ GRAPH ] ) .as ( [ 'FRAME228199' ] ) ;",
				"Database( database=[\"93857bba-5aea-447b-94f4-f9d9179da4da\"] ) | Select(Director, Title, Nominated, Studio, Genre).as([Director, Title, Nominated, Studio, Genre])|Join((Title, inner.join, Genre), (Title, inner.join, Nominated), (Title, inner.join, Director), (Title, inner.join, Studio)) | Import();",
				"Panel ( 0 ) | SetPanelView ( \"visualization\" ) ;",
				"Frame ( ) | QueryAll ( ) | AutoTaskOptions ( panel = [ \"0\" ] , layout = [ \"Grid\" ] ) | Collect ( 500 ) ;",
				"Select ( Director , Genre , Nominated , Studio ) .as ( [ Director , Genre , Nominated , Studio ] ) | With ( Panel ( 0 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"0\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Director\" , \"Genre\" , \"Nominated\" , \"Studio\" ] } } } ) | Collect ( 500 ) ;",
				"Select ( Director , Genre , Nominated ) .as ( [ Director , Genre , Nominated ] ) | With ( Panel ( 0 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"0\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Director\" , \"Genre\" , \"Nominated\" ] } } } ) | Collect ( 500 ) ;",
				"Select ( Director , Genre ) .as ( [ Director , Genre ] ) | With ( Panel ( 0 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"0\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Director\" , \"Genre\" ] } } } ) | Collect ( 500 ) ;",
				"Select ( Director ) .as ( [ Director ] ) | With ( Panel ( 0 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"0\" : { \"layout\" : \"Grid\" , \"alignment\" : { \"label\" : [ \"Director\" ] } } } ) | Collect ( 500 ) ;",
				"Panel ( 0 ) | SetPanelView ( \"visualization\" , \"<encode>{\"type\":\"echarts\"}</encode>\" ) ;",
				"if ( ( HasDuplicates ( Studio ) ) , ( Select ( Studio , Count ( Title ) ) .as ( [ Studio , CountofTitle ] ) | Group ( Studio ) | With ( Panel ( 0 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"0\" : { \"layout\" : \"Column\" , \"alignment\" : { \"label\" : [ \"Studio\" ] , \"value\" : [ \"CountofTitle\" ] , \"facet\" : [ ] } } } ) | Collect ( 500 ) ) , ( Select ( Studio , Count ( Title ) ) .as ( [ Studio , CountofTitle ] ) | Group ( Studio ) | With ( Panel ( 0 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"0\" : { \"layout\" : \"Column\" , \"alignment\" : { \"label\" : [ \"Studio\" ] , \"value\" : [ \"CountofTitle\" ] , \"facet\" : [ ] } } } ) | Collect ( 500 ) ) ) ;" ,
				"Panel ( 0 ) | Clone ( 1 ) ;",
				"if ( ( HasDuplicates ( Genre ) ) , ( Select ( Genre , Count ( Title ) ) .as ( [ Genre , CountofTitle ] ) | Group ( Genre ) | With ( Panel ( 1 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"1\" : { \"layout\" : \"Column\" , \"alignment\" : { \"label\" : [ \"Genre\" ] , \"value\" : [ \"CountofTitle\" ] , \"facet\" : [ ] } } } ) | Collect ( 500 ) ) , ( Select ( Genre , Count ( Title ) ) .as ( [ Genre , CountofTitle ] ) | Group ( Genre ) | With ( Panel ( 1 ) ) | Format ( type = [ 'table' ] ) | TaskOptions ( { \"1\" : { \"layout\" : \"Column\" , \"alignment\" : { \"label\" : [ \"Genre\" ] , \"value\" : [ \"CountofTitle\" ] , \"facet\" : [ ] } } } ) | Collect ( 500 ) ) ) ;",
				
				"z = Frame() | QueryAll() | Collect(-1);",
				"x = 5;",
				"y = \"abc\";"
		};
		
		// run the pixel
		in.runPixel(Arrays.asList(pixel));
		
		File insightFile;
		try {
			insightFile = InsightCacheUtility.cacheInsight(in);
			Insight newIn = InsightCacheUtility.readInsightCache(insightFile, in);
			printInsightDetails(newIn);
		} catch (IOException e) {
			e.printStackTrace();
		}
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
				ITableDataFrame frame = (ITableDataFrame) noun.getValue();
				IRawSelectWrapper it = frame.iterator();
				System.out.println(Arrays.toString(it.getHeaders()));
				int counter = 0;
				int limit = 25;
				while(it.hasNext() && (counter < limit)) {
					counter++;
					System.out.println(Arrays.toString(it.next().getValues()));
				}
				it.cleanUp();
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
