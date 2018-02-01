package prerna.sablecc2.reactor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import prerna.algorithm.api.ITableDataFrame;
import prerna.comments.AddInsightCommentReactor;
import prerna.comments.DeleteInsightCommentReactor;
import prerna.comments.GetInsightCommentsReactor;
import prerna.comments.ModifyInsightCommentReactor;
import prerna.date.DateReactor;
import prerna.ds.TinkerFrame;
import prerna.ds.h2.H2Frame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.ds.py.PandasFrame;
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
import prerna.sablecc2.reactor.algorithms.xray.GetCSVSchemaReactor;
import prerna.sablecc2.reactor.algorithms.xray.GetExternalDBSchemaReactor;
import prerna.sablecc2.reactor.algorithms.xray.GetLocalDBSchemaReactor;
import prerna.sablecc2.reactor.algorithms.xray.GetXLSchemaReactor;
import prerna.sablecc2.reactor.algorithms.xray.GetXrayConfigFileReactor;
import prerna.sablecc2.reactor.algorithms.xray.GetXrayConfigListReactor;
import prerna.sablecc2.reactor.algorithms.xray.XRayReactor;
import prerna.sablecc2.reactor.app.AppInfoReactor;
import prerna.sablecc2.reactor.app.AppInsightsReactor;
import prerna.sablecc2.reactor.app.MyAppsReactor;
import prerna.sablecc2.reactor.app.SetAppDescriptionReactor;
import prerna.sablecc2.reactor.app.SetAppImageReactor;
import prerna.sablecc2.reactor.app.SetAppTagsReactor;
import prerna.sablecc2.reactor.database.DeleteEngineReactor;
import prerna.sablecc2.reactor.database.DeleteInsightReactor;
import prerna.sablecc2.reactor.export.CollectGraphReactor;
import prerna.sablecc2.reactor.export.CollectReactor;
import prerna.sablecc2.reactor.export.GrabScalarElementReactor;
import prerna.sablecc2.reactor.export.IterateReactor;
import prerna.sablecc2.reactor.export.ToCsvReactor;
import prerna.sablecc2.reactor.expression.IfError;
import prerna.sablecc2.reactor.expression.OpAbsolute;
import prerna.sablecc2.reactor.expression.OpAsString;
import prerna.sablecc2.reactor.expression.OpIsEmpty;
import prerna.sablecc2.reactor.expression.OpLarge;
import prerna.sablecc2.reactor.expression.OpMatch;
import prerna.sablecc2.reactor.expression.OpMax;
import prerna.sablecc2.reactor.expression.OpMean;
import prerna.sablecc2.reactor.expression.OpMedian;
import prerna.sablecc2.reactor.expression.OpMin;
import prerna.sablecc2.reactor.expression.OpNotEmpty;
import prerna.sablecc2.reactor.expression.OpPower;
import prerna.sablecc2.reactor.expression.OpRound;
import prerna.sablecc2.reactor.expression.OpSmall;
import prerna.sablecc2.reactor.expression.OpSum;
import prerna.sablecc2.reactor.expression.OpSumIf;
import prerna.sablecc2.reactor.expression.OpSumIfs;
import prerna.sablecc2.reactor.expression.OpSumProduct;
import prerna.sablecc2.reactor.expression.filter.OpAnd;
import prerna.sablecc2.reactor.expression.filter.OpOr;
import prerna.sablecc2.reactor.frame.CreateFrameReactor;
import prerna.sablecc2.reactor.frame.CurrentFrameReactor;
import prerna.sablecc2.reactor.frame.FrameDuplicatesReactor;
import prerna.sablecc2.reactor.frame.FrameFilterModelReactor;
import prerna.sablecc2.reactor.frame.FrameTypeReactor;
import prerna.sablecc2.reactor.frame.GetFrameHeaderMetadataReactor;
import prerna.sablecc2.reactor.frame.InsightMetamodelReactor;
import prerna.sablecc2.reactor.frame.filter.AddFrameFilterReactor;
import prerna.sablecc2.reactor.frame.filter.DeleteFrameFilterReactor;
import prerna.sablecc2.reactor.frame.filter.GetFrameFilterReactor;
import prerna.sablecc2.reactor.frame.filter.RemoveFrameFilterReactor;
import prerna.sablecc2.reactor.frame.filter.ReplaceFrameFilterReactor;
import prerna.sablecc2.reactor.frame.filter.SetFrameFilterReactor;
import prerna.sablecc2.reactor.frame.filter.UnfilterFrameReactor;
import prerna.sablecc2.reactor.frame.r.GenerateFrameFromRVariableReactor;
import prerna.sablecc2.reactor.frame.r.GenerateH2FrameFromRVariableReactor;
import prerna.sablecc2.reactor.frame.r.SemanticBlendingReactor;
import prerna.sablecc2.reactor.frame.r.graph.ClusterGraphReactor;
import prerna.sablecc2.reactor.frame.r.graph.GraphLayoutReactor;
import prerna.sablecc2.reactor.imports.ImportDataReactor;
import prerna.sablecc2.reactor.imports.MergeDataReactor;
import prerna.sablecc2.reactor.insights.ClearInsightReactor;
import prerna.sablecc2.reactor.insights.DropInsightReactor;
import prerna.sablecc2.reactor.insights.GetCurrentRecipeReactor;
import prerna.sablecc2.reactor.insights.GetSavedInsightRecipeReactor;
import prerna.sablecc2.reactor.insights.InsightHandleReactor;
import prerna.sablecc2.reactor.insights.OpenEmptyInsightReactor;
import prerna.sablecc2.reactor.insights.OpenInsightReactor;
import prerna.sablecc2.reactor.insights.RemoveVariableReactor;
import prerna.sablecc2.reactor.insights.RetrieveInsightOrnamentReactor;
import prerna.sablecc2.reactor.insights.SaveInsightReactor;
import prerna.sablecc2.reactor.insights.SetInsightOrnamentReactor;
import prerna.sablecc2.reactor.insights.UpdateInsightImageReactor;
import prerna.sablecc2.reactor.insights.UpdateInsightReactor;
import prerna.sablecc2.reactor.insights.dashboard.DashboardInsightConfigReactor;
import prerna.sablecc2.reactor.job.JobReactor;
import prerna.sablecc2.reactor.masterdatabase.AddLogicalNameReactor;
import prerna.sablecc2.reactor.masterdatabase.AddMetaDescriptionReactor;
import prerna.sablecc2.reactor.masterdatabase.AddMetaLinkReactor;
import prerna.sablecc2.reactor.masterdatabase.AddMetaTagReactor;
import prerna.sablecc2.reactor.masterdatabase.ConnectedConceptsReactor;
import prerna.sablecc2.reactor.masterdatabase.DatabaseConceptPropertiesReactors;
import prerna.sablecc2.reactor.masterdatabase.DatabaseConceptsReactors;
import prerna.sablecc2.reactor.masterdatabase.DatabaseListReactor;
import prerna.sablecc2.reactor.masterdatabase.DatabaseMetamodelReactor;
import prerna.sablecc2.reactor.masterdatabase.DeleteMetaTagsReactor;
import prerna.sablecc2.reactor.masterdatabase.GetLogicalNamesReactor;
import prerna.sablecc2.reactor.masterdatabase.GetMetaDescriptionReactor;
import prerna.sablecc2.reactor.masterdatabase.GetMetaLinkReactor;
import prerna.sablecc2.reactor.masterdatabase.GetMetaTagReactor;
import prerna.sablecc2.reactor.masterdatabase.GetTraversalOptionsReactor;
import prerna.sablecc2.reactor.masterdatabase.RemoveLogicalNameReactor;
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
import prerna.sablecc2.reactor.panel.rules.AddPanelColorByValueReactor;
import prerna.sablecc2.reactor.panel.rules.DeletePanelColorByValueReactor;
import prerna.sablecc2.reactor.panel.rules.RetrievePanelColorByValueReactor;
import prerna.sablecc2.reactor.panel.sort.AddPanelSortReactor;
import prerna.sablecc2.reactor.panel.sort.RemovePanelSortReactor;
import prerna.sablecc2.reactor.panel.sort.SetPanelSortReactor;
import prerna.sablecc2.reactor.planner.GraphPlanReactor;
import prerna.sablecc2.reactor.planner.graph.ExecuteJavaGraphPlannerReactor;
import prerna.sablecc2.reactor.planner.graph.LoadGraphClient;
import prerna.sablecc2.reactor.planner.graph.UpdateGraphPlannerReactor2;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.reactor.qs.GroupByReactor;
import prerna.sablecc2.reactor.qs.JoinReactor;
import prerna.sablecc2.reactor.qs.LimitReactor;
import prerna.sablecc2.reactor.qs.OffsetReactor;
import prerna.sablecc2.reactor.qs.OrderByReactor;
import prerna.sablecc2.reactor.qs.QueryAllReactor;
import prerna.sablecc2.reactor.qs.QueryReactor;
import prerna.sablecc2.reactor.qs.WithReactor;
import prerna.sablecc2.reactor.qs.filter.QueryFilterReactor;
import prerna.sablecc2.reactor.qs.selectors.AverageReactor;
import prerna.sablecc2.reactor.qs.selectors.CountReactor;
import prerna.sablecc2.reactor.qs.selectors.GroupConcatReactor;
import prerna.sablecc2.reactor.qs.selectors.MaxReactor;
import prerna.sablecc2.reactor.qs.selectors.MedianReactor;
import prerna.sablecc2.reactor.qs.selectors.MinReactor;
import prerna.sablecc2.reactor.qs.selectors.QuerySelectReactor;
import prerna.sablecc2.reactor.qs.selectors.QuerySelectorExpressionAssimilator;
import prerna.sablecc2.reactor.qs.selectors.StandardDeviationReactor;
import prerna.sablecc2.reactor.qs.selectors.SumReactor;
import prerna.sablecc2.reactor.qs.selectors.UniqueCountReactor;
import prerna.sablecc2.reactor.qs.selectors.UniqueGroupConcatReactor;
import prerna.sablecc2.reactor.qs.source.DatabaseReactor;
import prerna.sablecc2.reactor.qs.source.DirectJdbcConnectionReactor;
import prerna.sablecc2.reactor.qs.source.FileSourceReactor;
import prerna.sablecc2.reactor.qs.source.FrameReactor;
import prerna.sablecc2.reactor.qs.source.JdbcEngineConnectorReactor;
import prerna.sablecc2.reactor.scheduler.ListAllJobsReactor;
import prerna.sablecc2.reactor.scheduler.RescheduleExistingJobReactor;
import prerna.sablecc2.reactor.scheduler.ScheduleJobReactor;
import prerna.sablecc2.reactor.scheduler.UnscheduleJobReactor;
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
import prerna.sablecc2.reactor.utils.AddOperationAliasReactor;
import prerna.sablecc2.reactor.utils.BackupDatabaseReactor;
import prerna.sablecc2.reactor.utils.HelpReactor;
import prerna.sablecc2.reactor.utils.SendEmailReactor;
import prerna.sablecc2.reactor.utils.VariableExistsReactor;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.ga.reactors.UpdateGAHistoricalDataReactor;
import prerna.util.ga.reactors.VisualizationRecommendationReactor;
import prerna.util.git.reactors.AddAppCollaborator;
import prerna.util.git.reactors.CopyAppRepo;
import prerna.util.git.reactors.DeleteAppRepo;
import prerna.util.git.reactors.DropAppRepo;
import prerna.util.git.reactors.GitStatusReactor;
import prerna.util.git.reactors.InitAppRepo;
import prerna.util.git.reactors.IsGit;
import prerna.util.git.reactors.ListAppCollaborators;
import prerna.util.git.reactors.ListAppRemotes;
import prerna.util.git.reactors.ListUserApps;
import prerna.util.git.reactors.LoginGit;
import prerna.util.git.reactors.RemoveAppCollaborator;
import prerna.util.git.reactors.RenameMosfitFileReactor;
import prerna.util.git.reactors.SearchAppCollaborator;
import prerna.util.git.reactors.SyncApp;
import prerna.util.git.reactors.SyncAppFiles;

public class ReactorFactory {

	// This holds the reactors that are frame agnostic and can be used by pixel
	public static Map<String, Class> reactorHash;

	// This holds the reactors that are expressions
	// example Sum, Max, Min
	// the reactors will handle how to execute
	// if it can be run via the frame (i.e. sql/gremlin) or needs to run external
	public static Map<String, Class> expressionHash;

	// this holds that base package name for frame specific reactors
	public static Map<String, Class> rFrameHash;
	public static Map<String, Class> pandasFrameHash;
	public static Map<String, Class> h2FrameHash;
	public static Map<String, Class> tinkerFrameHash;
	public static Map<String, Class> nativeFrameHash;
	public static final String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");	
	public static final String REACTOR_PROP_PATH = baseFolder +"\\src\\reactors.prop";
	public static final String EXPRESSION_PROP_PATH = baseFolder +"\\src\\expressionSetReactors.prop";
	public static final String R_FRAME_PROP_PATH = baseFolder + "\\src\\rFrameReactors.prop";
	public static final String PANDAS_FRAME_PROP_PATH = baseFolder + "\\src\\pyFrameReactors.prop";
	public static final String H2_FRAME_PROP_PATH = baseFolder + "\\src\\h2FrameReactors.prop";
	public static final String TINKER_FRAME_PROP_PATH = baseFolder + "\\src\\tinkerFrameReactors.prop";
	public static final String NATIVE_FRAME_PROP_PATH = baseFolder + "\\src\\nativeFrameReactors.prop";

	static {
		// check if external reactor paths exists if not load reactors defined in this class
		reactorHash = new HashMap<>();
		createReactorHash(reactorHash);
		// build generic reactor hash
		Path reactorPath = Paths.get(REACTOR_PROP_PATH);
		if (Files.exists(reactorPath)) {
			buildReactorHashFromPropertyFile(reactorHash, REACTOR_PROP_PATH);
		}

		// build expression hash
		expressionHash = new HashMap<>();
		populateExpressionSet(expressionHash);
		Path expressionPath = Paths.get(EXPRESSION_PROP_PATH);
		if (Files.exists(expressionPath)) {
			buildReactorHashFromPropertyFile(expressionHash, EXPRESSION_PROP_PATH);
		}

		// populate the frame specific hashes
		rFrameHash = new HashMap<>();
		populateRFrameHash(rFrameHash);
		Path rFramePath = Paths.get(R_FRAME_PROP_PATH);
		if (Files.exists(rFramePath)) {
			buildReactorHashFromPropertyFile(rFrameHash, R_FRAME_PROP_PATH);
		}

		pandasFrameHash = new HashMap<>();
		populatePandasFrameHash(pandasFrameHash);
		Path pyFramepath = Paths.get(PANDAS_FRAME_PROP_PATH);
		if (Files.exists(pyFramepath)) {
			buildReactorHashFromPropertyFile(pandasFrameHash, PANDAS_FRAME_PROP_PATH);
		}

		h2FrameHash = new HashMap<>();
		populateH2FrameHash(h2FrameHash);
		Path h2FramePath = Paths.get(H2_FRAME_PROP_PATH);
		if (Files.exists(h2FramePath)) {
			buildReactorHashFromPropertyFile(h2FrameHash, H2_FRAME_PROP_PATH);
		}
		
		tinkerFrameHash = new HashMap<>();
		populateTinkerFrameHash(tinkerFrameHash);
		Path tinkerFramePath = Paths.get(TINKER_FRAME_PROP_PATH);
		if (Files.exists(tinkerFramePath)) {
			buildReactorHashFromPropertyFile(tinkerFrameHash, TINKER_FRAME_PROP_PATH);
		}
	
		nativeFrameHash = new HashMap<>();
		populateNativeFrameHash(nativeFrameHash);
		Path nativeFramePath = Paths.get(NATIVE_FRAME_PROP_PATH);
		if (Files.exists(nativeFramePath)) {
			buildReactorHashFromPropertyFile(nativeFrameHash, NATIVE_FRAME_PROP_PATH);
		}
	}
	
	// populates the frame agnostic reactors used by pixel
	private static void createReactorHash(Map<String, Class> reactorHash) {
		// used to generate the base Job for the pksl commands being executed
		reactorHash.put("Job", JobReactor.class); // defines the job

		// Import Reactors
		// takes in a query struct and imports data to a new frame
		reactorHash.put("Import", ImportDataReactor.class);
		// takes in a query struct and merges data to an existing frame
		reactorHash.put("Merge", MergeDataReactor.class);

		// Utility Reactors
		reactorHash.put("VariableExists", VariableExistsReactor.class);
		reactorHash.put("SendEmail", SendEmailReactor.class);
		reactorHash.put("BackupDatabase", BackupDatabaseReactor.class);
		reactorHash.put("Help", HelpReactor.class);
		
		// Semantic blending
		reactorHash.put("SemanticBlending", SemanticBlendingReactor.class);
		
		// Query Struct Reactors
		// builds the select portion of the QS
		reactorHash.put("With", WithReactor.class);
		reactorHash.put("Select", QuerySelectReactor.class);
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
		reactorHash.put("JdbcSource", JdbcEngineConnectorReactor.class);
		reactorHash.put("DirectJDBCConnection", DirectJdbcConnectionReactor.class);
		reactorHash.put("DeleteEngine", DeleteEngineReactor.class);

		// specifies that our pksl operations after this point are dealing with the specified frame
		reactorHash.put("Frame", FrameReactor.class);
		reactorHash.put("CreateFrame", CreateFrameReactor.class);
		reactorHash.put("FrameType", FrameTypeReactor.class);
		reactorHash.put("GenerateFrameFromRVariable", GenerateFrameFromRVariableReactor.class);
		reactorHash.put("GenerateH2FrameFromRVariable", GenerateH2FrameFromRVariableReactor.class);
		//reactorHash.put("SynchronizeToR", SynchronizeToRReactor.class);

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
		reactorHash.put("ToCsv", ToCsvReactor.class); // take any task and output to a file
		// Task Operations
		reactorHash.put("Lambda", MapLambdaTaskReactor.class);
		reactorHash.put("FilterLambda", FilterLambdaTaskReactor.class);
		reactorHash.put("ToNumericType", ToNumericTypeTaskReactor.class);
		reactorHash.put("ToUrlType", ToUrlTypeTaskReactor.class);
		reactorHash.put("TransposeRows", TransposeRowTaskReactor.class);

		// Solr / Local Master Reactors
		reactorHash.put("GetDatabaseList", DatabaseListReactor.class);
		reactorHash.put("GetDatabaseConcepts", DatabaseConceptsReactors.class);
		reactorHash.put("GetTraversalOptions", GetTraversalOptionsReactor.class);
		reactorHash.put("GetConnectedConcepts", ConnectedConceptsReactor.class);
		reactorHash.put("GetConceptProperties", DatabaseConceptPropertiesReactors.class);
		reactorHash.put("GetEngineMetamodel", DatabaseMetamodelReactor.class);
		// Logical name operations
		reactorHash.put("AddLogicalName", AddLogicalNameReactor.class);
		reactorHash.put("GetLogicalNames", GetLogicalNamesReactor.class);
		reactorHash.put("RemoveLogicalNames", RemoveLogicalNameReactor.class);
		// concept description metadata 
		reactorHash.put("AddMetaDescription", AddMetaDescriptionReactor.class);
		reactorHash.put("GetMetaDescription", GetMetaDescriptionReactor.class);
		// concept tag metadata
		reactorHash.put("AddMetaTags", AddMetaTagReactor.class);
		reactorHash.put("GetMetaTags", GetMetaTagReactor.class);
		reactorHash.put("DeleteMetaTags", DeleteMetaTagsReactor.class);
		// concept link metadata
		reactorHash.put("AddMetaLinks", AddMetaLinkReactor.class);
		reactorHash.put("GetMetaLinks", GetMetaLinkReactor.class);

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
		// panel color by value
		reactorHash.put("AddPanelColorByValue", AddPanelColorByValueReactor.class);
		reactorHash.put("RetrievePanelColorByValue", RetrievePanelColorByValueReactor.class);
		reactorHash.put("RemovePanelColorByValue", DeletePanelColorByValueReactor.class);
		
		// new tab in browser
		reactorHash.put("OpenTab", OpenTabReactor.class);

		// Insight Reactors
		// OpenSavedInsight (InsightRecipe to be deleted) returns the insight recipe
		reactorHash.put("DeleteInsight", DeleteInsightReactor.class);
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
		reactorHash.put("UpdateInsightImage", UpdateInsightImageReactor.class);
		reactorHash.put("GetCurrentRecipe", GetCurrentRecipeReactor.class);

		// Save Reactors
		reactorHash.put("SaveInsight", SaveInsightReactor.class);
		reactorHash.put("UpdateInsight", UpdateInsightReactor.class);

		// Dashboard Reactors
		reactorHash.put("DashboardInsightConfig", DashboardInsightConfigReactor.class);

		// General Frame Reactors
		reactorHash.put("FrameHeaders", GetFrameHeaderMetadataReactor.class);
		reactorHash.put("AddFrameFilter", AddFrameFilterReactor.class);
		reactorHash.put("GetFrameFilters", GetFrameFilterReactor.class);
		reactorHash.put("SetFrameFilter", SetFrameFilterReactor.class);
		reactorHash.put("RemoveFrameFilter", RemoveFrameFilterReactor.class);
		reactorHash.put("ReplaceFrameFilter", ReplaceFrameFilterReactor.class);
		reactorHash.put("DeleteFrameFilter", DeleteFrameFilterReactor.class);
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
		
		// X-Ray reactors
		reactorHash.put("RunXray", XRayReactor.class);
		reactorHash.put("GetXrayConfigList", GetXrayConfigListReactor.class);
		reactorHash.put("GetXrayConfigFile", GetXrayConfigFileReactor.class);
		reactorHash.put("GetLocalSchema", GetLocalDBSchemaReactor.class);
		reactorHash.put("GetXLSchema", GetXLSchemaReactor.class);
		reactorHash.put("GetCSVSchema",GetCSVSchemaReactor.class);
		reactorHash.put("GetExternalSchema", GetExternalDBSchemaReactor.class);
		
		// these algorithms return viz data to the FE
		reactorHash.put("RunNumericalCorrelation", NumericalCorrelationReactor.class);
		reactorHash.put("RunMatrixRegression", MatrixRegressionReactor.class);
		reactorHash.put("RunClassification", WekaClassificationReactor.class);
		reactorHash.put("RunAssociatedLearning", WekaAprioriReactor.class);

		// In mem storage of data
		reactorHash.put("StoreValue", StoreValue.class);
		reactorHash.put("RetrieveValue", RetrieveValue.class);
		reactorHash.put("GraphPlan", GraphPlanReactor.class);
		
		// Pixel operation reactors 
		reactorHash.put("AddOperationAlias", AddOperationAliasReactor.class);

		// Tax specific handles
		reactorHash.put("LoadClient", LoadGraphClient.class);
		reactorHash.put("RunPlan", ExecuteJavaGraphPlannerReactor.class);
		reactorHash.put("UpdatePlan", UpdateGraphPlannerReactor2.class);
		reactorHash.put("TaxRetrieveValue", TaxRetrieveValue2.class);
		reactorHash.put("RunAliasMatch", AliasMatchTestReactor.class);
		reactorHash.put("SaveTaxScenario", TaxSaveScenarioReactor.class);
		
		// Git it
		reactorHash.put("InitAppRepo", InitAppRepo.class);
		reactorHash.put("AddAppCollaborator", AddAppCollaborator.class);
		reactorHash.put("RemoveAppCollaborator", RemoveAppCollaborator.class);
		reactorHash.put("SearchAppCollaborator", SearchAppCollaborator.class);
		reactorHash.put("ListAppCollaborators", ListAppCollaborators.class);
		reactorHash.put("CopyAppRepo", CopyAppRepo.class);
		reactorHash.put("DeleteAppRepo", DeleteAppRepo.class);
		reactorHash.put("DropAppRepo", DropAppRepo.class);
		reactorHash.put("SyncApp", SyncApp.class);
		reactorHash.put("SyncAppFiles", SyncAppFiles.class);
		reactorHash.put("ListAppRemotes", ListAppRemotes.class);
		reactorHash.put("ListUserApps", ListUserApps.class);
		reactorHash.put("IsGit", IsGit.class);
		reactorHash.put("Login", LoginGit.class);
		reactorHash.put("GitStatus", GitStatusReactor.class);
		reactorHash.put("RenameMosfitFile", RenameMosfitFileReactor.class);
		reactorHash.put("Version", prerna.util.git.reactors.Version.class);
		
		// App Metadata
		reactorHash.put("MyApps", MyAppsReactor.class);
		reactorHash.put("AppInfo", AppInfoReactor.class);
		reactorHash.put("GetAppInsights", AppInsightsReactor.class);
		reactorHash.put("SetAppDescription", SetAppDescriptionReactor.class);
		reactorHash.put("SetAppTags", SetAppTagsReactor.class);
		reactorHash.put("SetAppImage", SetAppImageReactor.class);

		
		// Insight Comments
		reactorHash.put("AddInsightComment", AddInsightCommentReactor.class);
		reactorHash.put("DeleteInsightComment", DeleteInsightCommentReactor.class);
		reactorHash.put("ModifyInsightComment", ModifyInsightCommentReactor.class);
		reactorHash.put("GetInsightComments", GetInsightCommentsReactor.class);
		
		// Scheduler
		reactorHash.put("ScheduleJob", ScheduleJobReactor.class);
		reactorHash.put("UnscheduleJob", UnscheduleJobReactor.class);
		reactorHash.put("ListAllJobs", ListAllJobsReactor.class);
		reactorHash.put("RescheduleExistingJob", RescheduleExistingJobReactor.class);

		// GA
		reactorHash.put("VizRecommendations", VisualizationRecommendationReactor.class);
		reactorHash.put("UpdateGAHistoricalData", UpdateGAHistoricalDataReactor.class);
		
		// Dates
		reactorHash.put("Date", DateReactor.class);
	}

	private static void populateNativeFrameHash(Map<String, Class> nativeFrameHash) {
		// this merge will not try to modify the filters on the QS since we are most
		// likely dealing with large data
		nativeFrameHash.put("Merge", prerna.sablecc2.reactor.frame.nativeframe.NativeFrameMergeDataReactor.class);
	}

	private static void populateH2FrameHash(Map<String, Class> h2FrameHash) {
		h2FrameHash.put("AddColumn", prerna.sablecc2.reactor.frame.rdbms.AddColumnReactor.class);
		h2FrameHash.put("ChangeColumnType", prerna.sablecc2.reactor.frame.rdbms.ChangeColumnTypeReactor.class);
		h2FrameHash.put("CountIf", prerna.sablecc2.reactor.frame.rdbms.CountIfReactor.class);
		h2FrameHash.put("DropColumn", prerna.sablecc2.reactor.frame.rdbms.DropColumnReactor.class);
		h2FrameHash.put("DropRows", prerna.sablecc2.reactor.frame.rdbms.DropRowsReactor.class);
		h2FrameHash.put("ExtractLetters", prerna.sablecc2.reactor.frame.rdbms.ExtractAlphaCharsReactor.class);
		h2FrameHash.put("ExtractNumbers", prerna.sablecc2.reactor.frame.rdbms.ExtractNumbersReactor.class);
		h2FrameHash.put("JoinColumns", prerna.sablecc2.reactor.frame.rdbms.JoinColumnsReactor.class);
		h2FrameHash.put("RenameColumn", prerna.sablecc2.reactor.frame.rdbms.RenameColumnReactor.class);
		h2FrameHash.put("SortColumn", prerna.sablecc2.reactor.frame.rdbms.SortColumnReactor.class);
		h2FrameHash.put("SplitColumns", prerna.sablecc2.reactor.frame.rdbms.SplitColumnReactor.class);
		h2FrameHash.put("ToLowerCase", prerna.sablecc2.reactor.frame.rdbms.ToLowerCaseReactor.class);
		h2FrameHash.put("ToUpperCase", prerna.sablecc2.reactor.frame.rdbms.ToUpperCaseReactor.class);
		h2FrameHash.put("TrimColumns", prerna.sablecc2.reactor.frame.rdbms.TrimReactor.class);
	}

	private static void populateRFrameHash(Map<String, Class> rFrameHash) {
		rFrameHash.put("AddColumn", prerna.sablecc2.reactor.frame.r.AddColumnReactor.class);
		rFrameHash.put("ChangeColumnType", prerna.sablecc2.reactor.frame.r.ChangeColumnTypeReactor.class);
		rFrameHash.put("CountIf", prerna.sablecc2.reactor.frame.r.CountIfReactor.class);
		rFrameHash.put("CollisionResolver", prerna.sablecc2.reactor.frame.r.CollisionResolverReactor.class);
		rFrameHash.put("DropColumn", prerna.sablecc2.reactor.frame.r.DropColumnReactor.class);
		rFrameHash.put("DropRows", prerna.sablecc2.reactor.frame.r.DropRowsReactor.class);
		rFrameHash.put("ExtractLetters", prerna.sablecc2.reactor.frame.r.ExtractAlphaCharsReactor.class);
		rFrameHash.put("ExtractNumbers", prerna.sablecc2.reactor.frame.r.ExtractNumbersReactor.class);
		rFrameHash.put("JoinColumns", prerna.sablecc2.reactor.frame.r.JoinColumnsReactor.class);
		rFrameHash.put("Pivot", prerna.sablecc2.reactor.frame.r.PivotReactor.class);
		rFrameHash.put("RegexReplaceColumnValue", prerna.sablecc2.reactor.frame.r.RegexReplaceColumnValueReactor.class);
		rFrameHash.put("RemoveDuplicateRows", prerna.sablecc2.reactor.frame.r.RemoveDuplicateRowsReactor.class);
		rFrameHash.put("RenameColumn", prerna.sablecc2.reactor.frame.r.RenameColumnReactor.class);
		rFrameHash.put("ReplaceColumnValue", prerna.sablecc2.reactor.frame.r.ReplaceColumnValueReactor.class);
		rFrameHash.put("SortColumn", prerna.sablecc2.reactor.frame.r.SortColumnReactor.class);
		rFrameHash.put("SplitColumns", prerna.sablecc2.reactor.frame.r.SplitColumnReactor.class);
		rFrameHash.put("SplitUnpivot", prerna.sablecc2.reactor.frame.r.SplitUnpivotReactor.class);
		rFrameHash.put("ToLowerCase", prerna.sablecc2.reactor.frame.r.ToLowerCaseReactor.class);
		rFrameHash.put("ToUpperCase", prerna.sablecc2.reactor.frame.r.ToUpperCaseReactor.class);
		rFrameHash.put("TrimColumns", prerna.sablecc2.reactor.frame.r.TrimReactor.class);
		rFrameHash.put("Transpose", prerna.sablecc2.reactor.frame.r.TransposeReactor.class);
		rFrameHash.put("Unpivot", prerna.sablecc2.reactor.frame.r.UnpivotReactor.class);
		rFrameHash.put("UpdateRowValues", prerna.sablecc2.reactor.frame.r.UpdateRowValuesWhereColumnContainsValueReactor.class);
		// frame stats
		rFrameHash.put("ColumnCount", prerna.sablecc2.reactor.frame.r.ColumnCountReactor.class);
		rFrameHash.put("DescriptiveStats", prerna.sablecc2.reactor.frame.r.DescriptiveStatsReactor.class);
		rFrameHash.put("Histogram", prerna.sablecc2.reactor.frame.r.HistogramReactor.class);
	}

	private static void populateTinkerFrameHash(Map<String, Class> tinkerFrameHash) {
		tinkerFrameHash.put("ChangeGraphLayout", GraphLayoutReactor.class);
		tinkerFrameHash.put("ClusterGraph", ClusterGraphReactor.class);
		
	}
	
	private static void populatePandasFrameHash(Map<String, Class> rFrameHash) {
		// TODO Auto-generated method stub

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
		IReactor reactor = null;

		try {
			// is this an expression?
			// we need to determine if we are treating this expression as a
			// reducer or as a selector
			if (expressionHash.containsKey(reactorId.toUpperCase())) {
				// if this expression is not a selector
				if (!(parentReactor instanceof AbstractQueryStructReactor) && 
						!(parentReactor instanceof QuerySelectorExpressionAssimilator)) {
					reactor = (IReactor) expressionHash.get(reactorId.toUpperCase()).newInstance();
					reactor.setPixel(reactorId, nodeString);
					return reactor;
				}
			}
			
			// see if it is a frame specific reactor
			if (frame != null) {
				// identify the correct hash to use
				if (frame instanceof H2Frame) {
					// see if the hash contains the reactor id
					if (h2FrameHash.containsKey(reactorId)) {
						reactor = (IReactor) h2FrameHash.get(reactorId).newInstance();
					}
				} else if (frame instanceof RDataTable) {
					if (rFrameHash.containsKey(reactorId)) {
						reactor = (IReactor) rFrameHash.get(reactorId).newInstance();
					}
				} else if (frame instanceof TinkerFrame) {
					if (tinkerFrameHash.containsKey(reactorId)) {
						reactor = (IReactor) tinkerFrameHash.get(reactorId).newInstance();
					}
				} else if (frame instanceof NativeFrame) {
					if (nativeFrameHash.containsKey(reactorId)) {
						reactor = (IReactor) nativeFrameHash.get(reactorId).newInstance();
					}
				} else if(frame instanceof PandasFrame) {
					if (nativeFrameHash.containsKey(reactorId)) {
						reactor = (IReactor) nativeFrameHash.get(reactorId).newInstance();
				}
				}

				// if we have retrieved a reactor from a frame hash
				if (reactor != null) {
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

	public static boolean hasReactor(String reactorId) {
		return reactorHash.containsKey(reactorId) || expressionHash.containsKey(reactorId.toUpperCase());
	} 
	
	/**
	 * This method takes in a prop file to build the reactorHash
	 * 
	 * @param propFile
	 *            - the path to the prop file with the reactor names and classes
	 * @param reactorHash
	 *            - the specific reactor hash object that we are building
	 * 
	 */
	public static void buildReactorHashFromPropertyFile(Map<String, Class> hash, String propFile) {
		// move info from the prop file into a Properties object
		Properties properties = Utility.loadProperties(propFile);
		// for each line in the file
		// each line maps a reactor (operation) to a class
		for (Object operation : properties.keySet()) {
			try {
				// identify the class that corresponds to each reactor
				String reactorClass = properties.get(operation).toString();
				Class reactor = (Class.forName(reactorClass));
				// put the operation and the class into the reactor hash
				hash.put(operation.toString(), reactor);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) {
		writeHashToFile(ReactorFactory.reactorHash, ReactorFactory.REACTOR_PROP_PATH);
		writeHashToFile(ReactorFactory.expressionHash, ReactorFactory.EXPRESSION_PROP_PATH);
		writeHashToFile(ReactorFactory.rFrameHash, ReactorFactory.R_FRAME_PROP_PATH);
		writeHashToFile(ReactorFactory.h2FrameHash, ReactorFactory.H2_FRAME_PROP_PATH);
		writeHashToFile(ReactorFactory.tinkerFrameHash, ReactorFactory.TINKER_FRAME_PROP_PATH);
		writeHashToFile(ReactorFactory.nativeFrameHash, ReactorFactory.NATIVE_FRAME_PROP_PATH);
	}
	
	private static void writeHashToFile (Map<String, Class> hash, String path) {
		try {
			PrintWriter pw = new PrintWriter(new File(path));
			StringBuilder sb = new StringBuilder();
			Object[] keys = hash.keySet().toArray();
			Arrays.sort(keys);
			for (Object operation: keys) {
				Class reactor = hash.get(operation);
				sb.append(operation + " " + reactor.getName() + "\n");
			}
			pw.write(sb.toString());
			pw.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
}
