package prerna.util.ga;

import java.util.List;
import java.util.Map;

import prerna.om.Insight;
import prerna.query.querystruct.QueryStruct2;

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
	public void trackDataImport(Insight in, QueryStruct2 qs) {
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
	public void trackViz(Map<String, Object> taskOptions, Insight in, QueryStruct2 qs) {
		// TODO Auto-generated method stub
		
	}

}
