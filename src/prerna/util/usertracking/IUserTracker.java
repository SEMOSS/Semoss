package prerna.util.usertracking;

import java.util.List;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.om.Insight;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.task.options.TaskOptions;

public interface IUserTracker {

	/**
	 * Determine if this is an active form of tracking
	 * @return
	 */
	boolean isActive();
	
	/**
	 * Track a visualization widget
	 * @param taskOptions
	 * @param qs
	 * @param in
	 */
	void trackVizWidget(Insight in, TaskOptions taskOptions, SelectQueryStruct qs);
	
	/**
	 * Track an analytics widget
	 * @param in
	 * @param routineName
	 * @param keyValue
	 */
	void trackAnalyticsWidget(Insight in, ITableDataFrame frame, String routineName, Map<String, List<String>> keyValue);
	
	/**
	 * Track data loading
	 * @param in
	 * @param qs
	 */
	void trackDataImport(Insight in, SelectQueryStruct qs);

	/**
	 * Track a query execution
	 * @param insight
	 * @param queryStruct
	 */
	void trackQueryData(Insight in, SelectQueryStruct qs);	
	
	/**
	 * Tracking insight execution
	 * @param in
	 */
	void trackInsightExecution(Insight in);
	
	/**
	 * Track pixel executions
	 * @param in
	 * @param pixel
	 * @param meta
	 */
	void trackPixelExecution(Insight in, String pixel, boolean meta);
	
	/**
	 * Track user changes to widgets
	 * @param values
	 */
	void trackUserWidgetMods(List<Object[]> rows);
	
	/**
	 * Track errors that occur during execution
	 * @param in
	 * @param pixel
	 * @param invalidSyntax
	 * @param ex
	 */
	void trackError(Insight in, String pixel, String reactorName, String parentReactorName, boolean meta, Exception ex);

}
