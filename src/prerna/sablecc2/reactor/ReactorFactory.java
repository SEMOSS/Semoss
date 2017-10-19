package prerna.sablecc2.reactor;

import java.util.HashMap;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.TinkerFrame;
import prerna.ds.h2.H2Frame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.reactor.algorithms.ClusteringAlgorithmReactor;
import prerna.sablecc2.reactor.algorithms.LOFAlgorithmReactor;
import prerna.sablecc2.reactor.algorithms.MatrixRegressionReactor;
import prerna.sablecc2.reactor.algorithms.MultiClusteringAlgorithmReactor;
import prerna.sablecc2.reactor.algorithms.NumericalCorrelationReactor;
import prerna.sablecc2.reactor.algorithms.OutlierAlgorithmReactor;
import prerna.sablecc2.reactor.algorithms.RatioAlgorithmReactor;
import prerna.sablecc2.reactor.algorithms.SimilarityAlgorithmReactor;
import prerna.sablecc2.reactor.algorithms.WekaAprioriReactor;
import prerna.sablecc2.reactor.algorithms.WekaClassificationReactor;
import prerna.sablecc2.reactor.export.CollectGraphReactor;
import prerna.sablecc2.reactor.export.CollectReactor;
import prerna.sablecc2.reactor.export.GrabScalarElementReactor;
import prerna.sablecc2.reactor.export.IterateReactor;
import prerna.sablecc2.reactor.expression.IfError;
import prerna.sablecc2.reactor.expression.OpAbsolute;
import prerna.sablecc2.reactor.expression.OpAnd;
import prerna.sablecc2.reactor.expression.OpAsString;
import prerna.sablecc2.reactor.expression.OpIsEmpty;
import prerna.sablecc2.reactor.expression.OpLarge;
import prerna.sablecc2.reactor.expression.OpMatch;
import prerna.sablecc2.reactor.expression.OpMax;
import prerna.sablecc2.reactor.expression.OpMean;
import prerna.sablecc2.reactor.expression.OpMedian;
import prerna.sablecc2.reactor.expression.OpMin;
import prerna.sablecc2.reactor.expression.OpNotEmpty;
import prerna.sablecc2.reactor.expression.OpOr;
import prerna.sablecc2.reactor.expression.OpPower;
import prerna.sablecc2.reactor.expression.OpRound;
import prerna.sablecc2.reactor.expression.OpSmall;
import prerna.sablecc2.reactor.expression.OpSum;
import prerna.sablecc2.reactor.expression.OpSumIf;
import prerna.sablecc2.reactor.expression.OpSumIfs;
import prerna.sablecc2.reactor.expression.OpSumProduct;
import prerna.sablecc2.reactor.frame.CreateFrameReactor;
import prerna.sablecc2.reactor.frame.CurrentFrameReactor;
import prerna.sablecc2.reactor.frame.FrameDuplicatesReactor;
import prerna.sablecc2.reactor.frame.FrameFilterModelReactor;
import prerna.sablecc2.reactor.frame.FrameTypeReactor;
import prerna.sablecc2.reactor.frame.GetFrameHeaderMetadataReactor;
import prerna.sablecc2.reactor.frame.InsightMetamodelReactor;
import prerna.sablecc2.reactor.frame.filter.AddFrameFilterReactor;
import prerna.sablecc2.reactor.frame.filter.SetFrameFilterReactor;
import prerna.sablecc2.reactor.frame.filter.UnfilterFrameReactor;
import prerna.sablecc2.reactor.imports.ImportDataReactor;
import prerna.sablecc2.reactor.imports.MergeDataReactor;
import prerna.sablecc2.reactor.insights.ClearInsightReactor;
import prerna.sablecc2.reactor.insights.DropInsightReactor;
import prerna.sablecc2.reactor.insights.GetSavedInsightRecipeReactor;
import prerna.sablecc2.reactor.insights.InsightHandleReactor;
import prerna.sablecc2.reactor.insights.OpenEmptyInsightReactor;
import prerna.sablecc2.reactor.insights.OpenInsightReactor;
import prerna.sablecc2.reactor.insights.RemoveVariableReactor;
import prerna.sablecc2.reactor.insights.RetrieveInsightOrnamentReactor;
import prerna.sablecc2.reactor.insights.SaveInsightReactor;
import prerna.sablecc2.reactor.insights.SetInsightOrnamentReactor;
import prerna.sablecc2.reactor.insights.dashboard.DashboardInsightConfigReactor;
import prerna.sablecc2.reactor.job.JobReactor;
import prerna.sablecc2.reactor.masterdatabase.ConnectedConceptsReactor;
import prerna.sablecc2.reactor.masterdatabase.DatabaseConceptPropertiesReactors;
import prerna.sablecc2.reactor.masterdatabase.DatabaseConceptsReactors;
import prerna.sablecc2.reactor.masterdatabase.DatabaseListReactor;
import prerna.sablecc2.reactor.masterdatabase.DatabaseMetamodelReactor;
import prerna.sablecc2.reactor.masterdatabase.GetTraversalOptionsReactor;
import prerna.sablecc2.reactor.panel.AddPanelIfAbsentReactor;
import prerna.sablecc2.reactor.panel.AddPanelReactor;
import prerna.sablecc2.reactor.panel.ClosePanelReactor;
import prerna.sablecc2.reactor.panel.GetInsightPanelsReactor;
import prerna.sablecc2.reactor.panel.PanelCloneReactor;
import prerna.sablecc2.reactor.panel.PanelExistsReactor;
import prerna.sablecc2.reactor.panel.PanelReactor;
import prerna.sablecc2.reactor.panel.RetrievePanelPositionReactor;
import prerna.sablecc2.reactor.panel.SetPanelLabelReactor;
import prerna.sablecc2.reactor.panel.SetPanelPositionReactor;
import prerna.sablecc2.reactor.panel.SetPanelViewReactor;
import prerna.sablecc2.reactor.panel.comments.AddPanelCommentReactor;
import prerna.sablecc2.reactor.panel.comments.RemovePanelCommentReactor;
import prerna.sablecc2.reactor.panel.comments.RetrievePanelCommentReactor;
import prerna.sablecc2.reactor.panel.comments.UpdatePanelCommentReactor;
import prerna.sablecc2.reactor.panel.events.AddPanelEventsReactor;
import prerna.sablecc2.reactor.panel.events.RemovePanelEventsReactor;
import prerna.sablecc2.reactor.panel.events.ResetPanelEventsReactor;
import prerna.sablecc2.reactor.panel.events.RetrievePanelEventsReactor;
import prerna.sablecc2.reactor.panel.external.OpenTabReactor;
import prerna.sablecc2.reactor.panel.filter.AddPanelFilterReactor;
import prerna.sablecc2.reactor.panel.filter.SetPanelFilterReactor;
import prerna.sablecc2.reactor.panel.filter.UnfilterPanelReactor;
import prerna.sablecc2.reactor.panel.ornaments.AddPanelOrnamentsReactor;
import prerna.sablecc2.reactor.panel.ornaments.RemovePanelOrnamentsReactor;
import prerna.sablecc2.reactor.panel.ornaments.ResetPanelOrnamentsReactor;
import prerna.sablecc2.reactor.panel.ornaments.RetrievePanelOrnamentsReactor;
import prerna.sablecc2.reactor.panel.sort.AddPanelSortReactor;
import prerna.sablecc2.reactor.panel.sort.RemovePanelSortReactor;
import prerna.sablecc2.reactor.panel.sort.SetPanelSortReactor;
import prerna.sablecc2.reactor.planner.GraphPlanReactor;
import prerna.sablecc2.reactor.planner.graph.ExecuteJavaGraphPlannerReactor;
import prerna.sablecc2.reactor.planner.graph.LoadGraphClient;
import prerna.sablecc2.reactor.planner.graph.UpdateGraphPlannerReactor2;
import prerna.sablecc2.reactor.qs.AverageReactor;
import prerna.sablecc2.reactor.qs.CountReactor;
import prerna.sablecc2.reactor.qs.DatabaseReactor;
import prerna.sablecc2.reactor.qs.DirectJdbcConnectionReactor;
import prerna.sablecc2.reactor.qs.FileSourceReactor;
import prerna.sablecc2.reactor.qs.FrameReactor;
import prerna.sablecc2.reactor.qs.GroupByReactor;
import prerna.sablecc2.reactor.qs.GroupConcatReactor;
import prerna.sablecc2.reactor.qs.JdbcEngineConnectorReactor;
import prerna.sablecc2.reactor.qs.JoinReactor;
import prerna.sablecc2.reactor.qs.LimitReactor;
import prerna.sablecc2.reactor.qs.MaxReactor;
import prerna.sablecc2.reactor.qs.MedianReactor;
import prerna.sablecc2.reactor.qs.MinReactor;
import prerna.sablecc2.reactor.qs.OffsetReactor;
import prerna.sablecc2.reactor.qs.OrderByReactor;
import prerna.sablecc2.reactor.qs.QueryAllReactor;
import prerna.sablecc2.reactor.qs.QueryFilterReactor;
import prerna.sablecc2.reactor.qs.QueryReactor;
import prerna.sablecc2.reactor.qs.QueryStructReactor;
import prerna.sablecc2.reactor.qs.SelectReactor;
import prerna.sablecc2.reactor.qs.StandardDeviationReactor;
import prerna.sablecc2.reactor.qs.SumReactor;
import prerna.sablecc2.reactor.qs.UniqueCountReactor;
import prerna.sablecc2.reactor.qs.UniqueGroupConcatReactor;
import prerna.sablecc2.reactor.qs.WithReactor;
import prerna.sablecc2.reactor.storage.RetrieveValue;
import prerna.sablecc2.reactor.storage.StoreValue;
import prerna.sablecc2.reactor.storage.TaxRetrieveValue2;
import prerna.sablecc2.reactor.storage.TaxSaveScenarioReactor;
import prerna.sablecc2.reactor.task.RemoveTaskReactor;
import prerna.sablecc2.reactor.task.ResetTask;
import prerna.sablecc2.reactor.task.TaskFormatReactor;
import prerna.sablecc2.reactor.task.TaskMetaCollectorReactor;
import prerna.sablecc2.reactor.task.TaskOptionsReactor;
import prerna.sablecc2.reactor.task.TaskReactor;
import prerna.sablecc2.reactor.task.modifiers.FilterLambdaTaskReactor;
import prerna.sablecc2.reactor.task.modifiers.MapLambdaTaskReactor;
import prerna.sablecc2.reactor.task.modifiers.ToNumericTypeTaskReactor;
import prerna.sablecc2.reactor.task.modifiers.ToUrlTypeTaskReactor;
import prerna.sablecc2.reactor.task.modifiers.TransposeRowTaskReactor;
import prerna.sablecc2.reactor.test.AliasMatchTestReactor;

public class ReactorFactory {

	// This holds the reactors that are frame agnostic and can be used by pixel
	private static Map<String, Class> reactorHash;

	// This holds the reactors that are expressions
	// example Sum, Max, Min
	// the reactors will handle how to execute
	// if it can be run via the frame (i.e. sql/gremlin) or needs to run
	// external
	private static Map<String, Class> expressionHash;

	// this holds that base package name for frame specific reactors
	private static Map<String, String> framePackageHash;
	private static Map<String, String> frameReactorHash;
	
	static {
		reactorHash = new HashMap<>();
		createReactorHash(reactorHash);

		expressionHash = new HashMap<>();
		populateExpressionSet(expressionHash);
		
		framePackageHash = new HashMap<>();
		frameReactorHash = new HashMap<>();
		populateFrameHash(framePackageHash, frameReactorHash);
	}

	// populates the frame agnostic reactors used by pixel
	private static void createReactorHash(Map<String, Class> reactorHash) {
		// used to generate the base Job for the pksl commands being executed
		reactorHash.put("Job", JobReactor.class); // defines the job
				
		// Import Reactors
		// takes in a query struct and imports data to a new frame
		reactorHash.put("Import", ImportDataReactor.class); 
		// takes in a query struct and merges data to an  existing frame
		reactorHash.put("Merge", MergeDataReactor.class); 
		
		// Variables
		reactorHash.put("VariableExists", VariableExistsReactor.class); 
		
		// Query Struct Reactors
		// builds the select portion of the QS
		reactorHash.put("With", WithReactor.class);
		reactorHash.put("Select", SelectReactor.class);
		reactorHash.put("Average", AverageReactor.class);
		reactorHash.put("Mean", AverageReactor.class);
		reactorHash.put("Sum", SumReactor.class);
		reactorHash.put("Max", MaxReactor.class);
		reactorHash.put("Min", MinReactor.class);
		reactorHash.put("Median", MedianReactor.class);
		reactorHash.put("StandardDeviation", StandardDeviationReactor.class);
		reactorHash.put("Count", CountReactor.class);
		reactorHash.put("UniqueCount", UniqueCountReactor.class);
		reactorHash.put("GroupConcat", GroupConcatReactor.class);
		reactorHash.put("UniqueGroupConcat", UniqueGroupConcatReactor.class);
		reactorHash.put("Group", GroupByReactor.class);
		reactorHash.put("Sort", OrderByReactor.class);
		reactorHash.put("Order", OrderByReactor.class);
		reactorHash.put("Limit", LimitReactor.class);
		reactorHash.put("Offset", OffsetReactor.class);
		reactorHash.put("Join", JoinReactor.class);
		reactorHash.put("Filter", QueryFilterReactor.class);
		reactorHash.put("Query", QueryReactor.class);
		reactorHash.put("QueryAll", QueryAllReactor.class);

		// If is in its own category
		reactorHash.put("if", IfReactor.class);
		
		// Data Source Reactors
		// specifies that our pksl operations after this point are dealing with the specified database
		reactorHash.put("Database", DatabaseReactor.class); 
		reactorHash.put("FileRead", FileSourceReactor.class); 
//		reactorHash.put("TextInput", TextInputReactor.class); 
		reactorHash.put("JdbcSource", JdbcEngineConnectorReactor.class);
		reactorHash.put("DirectJDBCConnection", DirectJdbcConnectionReactor.class);


		// specifies that our pksl operations after this point are dealing with the specified frame
		reactorHash.put("Frame", FrameReactor.class); 
		reactorHash.put("CreateFrame", CreateFrameReactor.class);
		reactorHash.put("FrameType", FrameTypeReactor.class);
		
		// Task Reactors
		reactorHash.put("Iterate", IterateReactor.class); 
		reactorHash.put("Task", TaskReactor.class); // defines the task
		reactorHash.put("ResetTask", ResetTask.class); // defines the task
		reactorHash.put("RemoveTask", RemoveTaskReactor.class);
		reactorHash.put("Collect", CollectReactor.class); // collect from task
		reactorHash.put("CollectGraph", CollectGraphReactor.class); // collect from task
		reactorHash.put("GrabScalarElement", GrabScalarElementReactor.class);
		reactorHash.put("CollectMeta", TaskMetaCollectorReactor.class); // collect meta from task
		reactorHash.put("Format", TaskFormatReactor.class); // set formats
		reactorHash.put("TaskOptions", TaskOptionsReactor.class); // set options
		// Task Operations
		reactorHash.put("Lambda", MapLambdaTaskReactor.class);
		reactorHash.put("FilterLambda", FilterLambdaTaskReactor.class);
		reactorHash.put("ToNumericType", ToNumericTypeTaskReactor.class);
		reactorHash.put("ToUrlType", ToUrlTypeTaskReactor.class); 
		reactorHash.put("TransposeRows", TransposeRowTaskReactor.class);

		// Local Master Reactors
		reactorHash.put("GetDatabaseList", DatabaseListReactor.class);
		reactorHash.put("GetDatabaseConcepts", DatabaseConceptsReactors.class);
		reactorHash.put("GetTraversalOptions", GetTraversalOptionsReactor.class);
		reactorHash.put("GetConnectedConcepts", ConnectedConceptsReactor.class);
		reactorHash.put("GetConceptProperties", DatabaseConceptPropertiesReactors.class);
		reactorHash.put("GetEngineMetamodel", DatabaseMetamodelReactor.class);
		
		// Panel Reactors
		reactorHash.put("InsightPanelIds", GetInsightPanelsReactor.class);
		reactorHash.put("Panel", PanelReactor.class);
		reactorHash.put("AddPanel", AddPanelReactor.class);
		reactorHash.put("AddPanelIfAbsent", AddPanelIfAbsentReactor.class);
		reactorHash.put("ClosePanel", ClosePanelReactor.class);
		reactorHash.put("PanelExists", PanelExistsReactor.class);
		reactorHash.put("Clone", PanelCloneReactor.class);
		reactorHash.put("SetPanelLabel", SetPanelLabelReactor.class);
		reactorHash.put("SetPanelView", SetPanelViewReactor.class);
		// panel filters
		reactorHash.put("AddPanelFilter", AddPanelFilterReactor.class);
		reactorHash.put("SetPanelFilter", SetPanelFilterReactor.class);
		reactorHash.put("UnfilterPanel", UnfilterPanelReactor.class);
		// panel sort
		reactorHash.put("AddPanelSort", AddPanelSortReactor.class);
		reactorHash.put("SetPanelSort", SetPanelSortReactor.class);
		reactorHash.put("RemovePanelSort", RemovePanelSortReactor.class);
		reactorHash.put("UnsortPanel", RemovePanelSortReactor.class);
		// panel comments
		reactorHash.put("AddPanelComment", AddPanelCommentReactor.class);
		reactorHash.put("UpdatePanelComment", UpdatePanelCommentReactor.class);
		reactorHash.put("RemovePanelComment", RemovePanelCommentReactor.class);
		reactorHash.put("RetrievePanelComment", RetrievePanelCommentReactor.class);
		// panel ornaments
		reactorHash.put("AddPanelOrnaments", AddPanelOrnamentsReactor.class);
		reactorHash.put("RemovePanelOrnaments", RemovePanelOrnamentsReactor.class);
		reactorHash.put("ResetPanelOrnaments", ResetPanelOrnamentsReactor.class);
		reactorHash.put("RetrievePanelOrnaments", RetrievePanelOrnamentsReactor.class);
		// panel events
		reactorHash.put("AddPanelEvents", AddPanelEventsReactor.class);
		reactorHash.put("RemovePanelEvents", RemovePanelEventsReactor.class);
		reactorHash.put("ResetPanelEvents", ResetPanelEventsReactor.class);
		reactorHash.put("RetrievePanelEvents", RetrievePanelEventsReactor.class);
		// panel position
		reactorHash.put("SetPanelPosition", SetPanelPositionReactor.class);
		reactorHash.put("RetrievePanelPosition", RetrievePanelPositionReactor.class);
		
		// new panel
		reactorHash.put("OpenTab", OpenTabReactor.class);
		
		// Insight Reactors
		// OpenSavedInsight (InsightRecipe to be deleted) returns the insight recipe
		reactorHash.put("OpenSavedInsight", GetSavedInsightRecipeReactor.class);
		reactorHash.put("InsightRecipe", GetSavedInsightRecipeReactor.class);
		reactorHash.put("OpenInsight", OpenInsightReactor.class);
		reactorHash.put("OpenEmptyInsight", OpenEmptyInsightReactor.class);
		reactorHash.put("DropInsight", DropInsightReactor.class);
		reactorHash.put("ClearInsight", ClearInsightReactor.class);
		reactorHash.put("InsightHandle", InsightHandleReactor.class);
		reactorHash.put("RemoveVariable", RemoveVariableReactor.class);
		reactorHash.put("SetInsightOrnament", SetInsightOrnamentReactor.class);
		reactorHash.put("RetrieveInsightOrnament", RetrieveInsightOrnamentReactor.class);

		// Save Reactors
		reactorHash.put("Save", SaveInsightReactor.class);

		// Dashboard Reactors
		reactorHash.put("DashboardInsightConfig", DashboardInsightConfigReactor.class);

		// General Frame Reactors
		reactorHash.put("FrameHeaders", GetFrameHeaderMetadataReactor.class);
		reactorHash.put("AddFrameFilter", AddFrameFilterReactor.class);
		reactorHash.put("SetFrameFilter", SetFrameFilterReactor.class);
		reactorHash.put("UnfilterFrame", UnfilterFrameReactor.class);
		reactorHash.put("InsightMetamodel", InsightMetamodelReactor.class);
		reactorHash.put("FrameFilterModel", FrameFilterModelReactor.class);
		reactorHash.put("HasDuplicates", FrameDuplicatesReactor.class);
		reactorHash.put("CurrentFrame", CurrentFrameReactor.class);
		
		// Algorithm Reactors
		reactorHash.put("RunClustering", ClusteringAlgorithmReactor.class);
		reactorHash.put("RunMultiClustering", MultiClusteringAlgorithmReactor.class);
		reactorHash.put("RunLOF", LOFAlgorithmReactor.class);
		reactorHash.put("RunSimilarity", SimilarityAlgorithmReactor.class);
		reactorHash.put("RunOutlier", OutlierAlgorithmReactor.class);
		reactorHash.put("Ratio", RatioAlgorithmReactor.class);
		
		// these algorithms return viz data to the FE
		reactorHash.put("RunNumericalCorrelation", NumericalCorrelationReactor.class);
		reactorHash.put("RunMatrixRegression", MatrixRegressionReactor.class);
		reactorHash.put("RunClassification", WekaClassificationReactor.class);
		reactorHash.put("RunAssociatedLearning", WekaAprioriReactor.class);
		
		// In mem storage of data
		reactorHash.put("StoreValue", StoreValue.class);
		reactorHash.put("RetrieveValue", RetrieveValue.class);
		reactorHash.put("GraphPlan", GraphPlanReactor.class);

		// Tax specific handles
		reactorHash.put("LoadClient", LoadGraphClient.class);
		reactorHash.put("RunPlan", ExecuteJavaGraphPlannerReactor.class);
		reactorHash.put("UpdatePlan", UpdateGraphPlannerReactor2.class);
		reactorHash.put("TaxRetrieveValue", TaxRetrieveValue2.class);
		reactorHash.put("RunAliasMatch", AliasMatchTestReactor.class);
		reactorHash.put("SaveTaxScenario", TaxSaveScenarioReactor.class);
	}
	
	private static void populateExpressionSet(Map<String, Class> expressionHash) {
		// excel like operations
		expressionHash.put("SUM", OpSum.class);
		expressionHash.put("AVERAGE", OpMean.class);
		expressionHash.put("AVG", OpMean.class);
		expressionHash.put("MEAN", OpMean.class);
		expressionHash.put("MIN", OpMin.class);
		expressionHash.put("MAX", OpMax.class);
		expressionHash.put("MEDIAN", OpMedian.class);
		expressionHash.put("POWER", OpPower.class);
		expressionHash.put("LARGE", OpLarge.class);
		expressionHash.put("SMALL", OpSmall.class);
		expressionHash.put("ROUND", OpRound.class);
		expressionHash.put("ABS", OpAbsolute.class);
		expressionHash.put("ABSOLUTE", OpAbsolute.class);
		expressionHash.put("MATCH", OpMatch.class);
		expressionHash.put("SUMIF", OpSumIf.class);
		expressionHash.put("SUMIFS", OpSumIfs.class);
		expressionHash.put("SUMPRODUCT", OpSumProduct.class);
		expressionHash.put("AND", OpAnd.class);
		expressionHash.put("OR", OpOr.class);
		expressionHash.put("IFERROR", IfError.class);
		expressionHash.put("NOTEMPTY", OpNotEmpty.class);
		expressionHash.put("ISEMPTY", OpIsEmpty.class);
		expressionHash.put("ASSTRING", OpAsString.class);
	}
	
	private static void populateFrameHash(Map<String, String> frameHash, Map<String, String> frameReactorHash) {
		// add the package directory for each frame
		frameHash.put(H2Frame.DATA_MAKER_NAME, "prerna.sablecc2.reactor.frame.rdbms.");
		frameHash.put(RDataTable.DATA_MAKER_NAME, "prerna.sablecc2.reactor.frame.r.");
		frameHash.put(NativeFrame.DATA_MAKER_NAME, "prerna.sablecc2.reactor.frame.nativeframe.");
		frameHash.put(TinkerFrame.DATA_MAKER_NAME, "prerna.sablecc2.reactor.frame.tinker.");

		// currently don't have any of these...
		frameReactorHash.put("AddColumn", "AddColumnReactor");
		frameReactorHash.put("ChangeColumnType", "ChangeColumnTypeReactor");
		frameReactorHash.put("CountIf", "CountIfReactor");
		frameReactorHash.put("DropColumn", "DropColumnReactor");
		frameReactorHash.put("DropRows", "DropRowsReactor");
		frameReactorHash.put("JoinColumns", "JoinColumnsReactor");
		frameReactorHash.put("RegexReplaceColumnValue", "RegexReplaceColumnValueReactor");
		frameReactorHash.put("RemoveDuplicateRows", "RemoveDuplicateRowsReactor");
		frameReactorHash.put("RenameColumn", "RenameColumnReactor");
		frameReactorHash.put("ReplaceColumnValue", "ReplaceColumnValueReactor");
		frameReactorHash.put("SortColumn", "SortColumnReactor");
		frameReactorHash.put("SplitColumns", "SplitColumnReactor");
		frameReactorHash.put("ToLowerCase", "ToLowerCaseReactor");
		frameReactorHash.put("ToUpperCase", "ToUpperCaseReactor");
		frameReactorHash.put("TrimColumns", "TrimReactor");
		frameReactorHash.put("Transpose", "TransposeReactor");
		frameReactorHash.put("UpdateRowValues", "UpdateRowValuesWhereColumnContainsValueReactor");
	}

	/**
	 * 
	 * @param reactorId
	 *            - reactor name
	 * @param nodeString
	 *            - pixel
	 * @param frame
	 *            - frame we will be operating on
	 * @param parentReactor
	 *            - the parent reactor
	 * @return
	 * 
	 * 		This will simply return the IReactor responsible for execution
	 *         based on the reactorId
	 * 
	 *         Special case: if we are dealing with an expression, we determine
	 *         if this expression is part of a select query or should be reduced
	 *         If it is a reducing expression we 1. create an expr reactor 2.
	 *         grab the reducing expression reactor from the frame 3. set that
	 *         reactor to the expr reactor and return the expr reactor The expr
	 *         reactor when executed will use that reducing expression reactor
	 *         to evaluate
	 */
	public static IReactor getReactor(String reactorId, String nodeString, ITableDataFrame frame, IReactor parentReactor) {
		IReactor reactor;
		try {
			// is this an expression?
			// we need to determine if we are treating this expression as a
			// reducer or as a selector
			if (expressionHash.containsKey(reactorId.toUpperCase())) {
				// if this expression is not a selector
				if (!(parentReactor instanceof QueryStructReactor)) {
					reactor = (IReactor) expressionHash.get(reactorId.toUpperCase()).newInstance();
					reactor.setPixel(reactorId, nodeString);
					return reactor;
				}
			}
			
			// see if it is a frame specific reactor
			if(frame != null) {
				reactor = getFrameReactor(frame.getDataMakerName(), reactorId);
				if(reactor != null) {
					reactor.setPixel(reactorId, nodeString);
					return reactor;
				}
			}
			
			// see if it is a generic one
			// if not an expression
			// search in the normal reactor hash
			if (reactorHash.containsKey(reactorId)) {
				reactor = (IReactor) reactorHash.get(reactorId).newInstance();
				reactor.setPixel(reactorId, nodeString);
				return reactor;
			}
		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
		}
		
		// ughhh... idk what you are trying to do
		// reactor = new SamplReactor();
		throw new IllegalArgumentException("Cannot find reactor for keyword = " + reactorId);
	}
	
	public static IReactor getFrameReactor(String frameName, String reactorId) {
		if(framePackageHash.containsKey(frameName) && frameReactorHash.containsKey(reactorId)) {
			String reactorPath = framePackageHash.get(frameName) + frameReactorHash.get(reactorId);
			try {
				return (IReactor) Class.forName(reactorPath).newInstance();
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				//e.printStackTrace();
			}
		}
		return null;
	}
	

	public static boolean hasReactor(String reactorId) {
		return reactorHash.containsKey(reactorId) || expressionHash.containsKey(reactorId.toUpperCase());
	}
}
