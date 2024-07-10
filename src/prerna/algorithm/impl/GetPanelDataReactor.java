//package prerna.algorithm.impl;
//
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import java.util.Vector;
//
//import com.google.gson.Gson;
//import com.google.gson.GsonBuilder;
//
//import prerna.algorithm.api.ITableDataFrame;
//import prerna.sablecc.AbstractReactor;
//import prerna.sablecc.PKQLEnum;
//
//public class GetPanelDataReactor extends BaseReducerReactor {
//	
//	public GetPanelDataReactor() {
//		setMathRoutine("PanelData");
//	}
//
//	@Override
//	public Object reduce() {
//		GsonBuilder builder = new GsonBuilder();
//		Gson gson = builder.create();
//		Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MAP_OBJ);
//		String panelName = "prerna.algorithm.impl." + (String)options.get("PanelName");
//		try {
//			Panel panel = (Panel)Class.forName(panelName).newInstance();
//			return gson.toJson(panel.getData(myStore,this));
//		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
//			classLogger.error(Constants.STACKTRACE, e);
//		}
//		return "";
//	}
//
//	@Override
//	public Map<String, Object> getColumnDataMap() {
//		// TODO Auto-generated method stub
//		return getBaseColumnDataMap();
//	}
//
//	@Override
//	public HashMap<HashMap<Object, Object>, Object> reduceGroupBy(Vector<String> groupBys,
//			Vector<String> processedColumns, String[] columnsArray, Iterator it) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//}
//
//interface Panel{
//	HashMap<String,Object> getData(HashMap<String,Object> myStore, AbstractReactor parentReactor);
//}
//
///***************************** Panels **********************************/
//
//class RandomSample implements Panel{
//
//	@Override
//	public HashMap<String, Object> getData(HashMap<String, Object> myStore, AbstractReactor parentReactor) {
//		HashMap<String,Object> panelData = new HashMap<>();
//		
//		ITableDataFrame dataFrame = (ITableDataFrame)myStore.get("G");
//		List<String> headers = Arrays.asList(dataFrame.getColumnHeaders());
//		if (headers.contains("Region")){
//			panelData.put("ColumnValuesAndCounts", dataFrame.getUniqueValuesAndCount("Region"));
//		}
//		else{
//			HashMap<String,Integer> columnValuesAndCount = new HashMap<>();
//			columnValuesAndCount.put("No Cluster", dataFrame.getNumRows());
//			panelData.put("ColumnValuesAndCounts",columnValuesAndCount);
//		}
//		return panelData;
//	}
//	
//}
//
//class Boundaries implements Panel{
//
//	@Override
//	public HashMap<String, Object> getData(HashMap<String, Object> myStore, AbstractReactor parentReactor) {
//		HashMap<String,Object> panelData = new HashMap<>();
//		ITableDataFrame dataFrame = (ITableDataFrame) myStore.get("G");
//		Vector<String> columns = (Vector <String>) myStore.get(PKQLEnum.COL_DEF);
//		int numRows = dataFrame.getNumRows();
//		Object[] xArray = dataFrame.getColumn(columns.get(0));
//		Object[] yArray = dataFrame.getColumn(columns.get(1));
//		double sigmaXY = 0, sigmaX = 0, sigmaY = 0, sigmaX2 = 0;
//		for(int i=0;i<numRows;i++){
//			double x = (double)xArray[i];
//			double y = (double)yArray[i];
//			sigmaX += x;
//			sigmaY += y;
//			sigmaX2 += x * x;
//			sigmaXY += x * y;
//		}
//		double lRegSlope = (sigmaXY - (sigmaX * sigmaY/numRows))/(sigmaX2 - (sigmaX * sigmaX/numRows));
//		double lRegIntercept = (sigmaY - (lRegSlope * sigmaX))/numRows;
//		double[] distance = new double[numRows];
//		double averageDist = 0;
//		double maxDist = Double.MIN_VALUE;
//		for(int i=0;i<numRows;i++){
//			double x = (double)xArray[i];
//			double y = (double)yArray[i];
//			distance[i] = y - (lRegSlope * x) - lRegIntercept;
//			averageDist += distance[i]/numRows;
//			maxDist = Math.max(maxDist, Math.abs(distance[i]));
//		}
//		Arrays.sort(distance);
//		double variance = 0;
//		for(int i=0;i<numRows;i++){
//			variance += Math.pow(distance[i]-averageDist, 2)/numRows;
//		}
//		double maxSliderVal = maxDist/Math.sqrt(variance);
//		panelData.put("maxSliderVal", String.format("%.02f",maxSliderVal));
//		return panelData;
//	}
//	
//}