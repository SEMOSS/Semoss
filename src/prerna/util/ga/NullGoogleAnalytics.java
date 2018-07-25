package prerna.util.ga;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.om.Insight;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.task.options.TaskOptions;

public class NullGoogleAnalytics implements IGoogleAnalytics {

	/**
	 * Constructor is protected so it can only be created by the builder
	 */
	protected NullGoogleAnalytics() {
		
	}
	
	/*
	 * This class i used when we do not want to do any tracking
	 * So no methods will be implemented
	 */
	
	@Override
	public void track(String thisExpression, String thisType) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void track(String thisExpression, String thisType, String prevExpression, String prevType) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void track(String thisExpression, String thisType, String prevExpression, String prevType, String userId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void trackAnalyticsPixel(Insight in, String routine) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void trackDataImport(Insight in, SelectQueryStruct qs) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void trackInsightExecution(Insight in, String type, String engineName, String rdbmsId, String insightName) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void trackExcelUpload(String tableName, String fileName,
			List<Map<String, Map<String, String[]>>> headerDataTypes) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void trackCsvUpload(String files, String dbName, List<Map<String, String[]>> headerDataTypes) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void trackDragAndDrop(Insight in, List<String> headers, String FileName) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void trackViz(TaskOptions taskOptions, Insight in, SelectQueryStruct qs) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void addNewLogicalNames(Map<String, Object> newLogicals, String[] columns, ITableDataFrame frame) {
		// TODO Auto-generated method stub

	}

	@Override
	public ArrayList<String> getLogicalNames(String uniqueName) {
		return null;
		// TODO Auto-generated method stub

	}

	@Override
	public void trackDescriptions(Insight in, String engineId, String engineAlias, HashMap<String,Map> descriptions) {
		// TODO Auto-generated method stub
		
	}

}
