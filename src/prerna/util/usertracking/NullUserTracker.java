package prerna.util.usertracking;

import java.util.List;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.om.Insight;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.task.options.TaskOptions;

public class NullUserTracker implements IUserTracker {

	/**
	 * Constructor is protected so it can only be created by the builder
	 */
	protected NullUserTracker() {
		
	}
	
	/*
	 * This class i used when we do not want to do any tracking
	 * So no methods will be implemented
	 */
	
	@Override
	public void trackVizWidget(Insight in, TaskOptions taskOptions, SelectQueryStruct qs) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void trackAnalyticsWidget(Insight in, ITableDataFrame frame, String routineName, Map<String, List<String>> keyValue) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void trackPixelExecution(Insight in, String pixel, boolean meta) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void trackInsightExecution(Insight in) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void trackDataImport(Insight in, SelectQueryStruct qs) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void trackQueryData(Insight in, SelectQueryStruct qs) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void trackUserWidgetMods(List<Object[]> rows) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void trackError(Insight in, String pixel, boolean invalidSyntax, Exception ex) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public boolean isActive() {
		return false;
	}

}
