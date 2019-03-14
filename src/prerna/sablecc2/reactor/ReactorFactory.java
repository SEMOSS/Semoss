package prerna.sablecc2.reactor;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.fasterxml.jackson.databind.ObjectMapper;

import prerna.algorithm.api.ITableDataFrame;
//import prerna.cluster.util.PullCloudAppReactor;
import prerna.cluster.util.PushAppsReactor;
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
import prerna.forms.FormsReactor;
import prerna.io.connector.surveymonkey.GetSurveyMonkeySurveysReactor;
import prerna.poi.main.helper.excel.ExcelDataValidationReactor;
import prerna.query.querystruct.delete.QueryDeleteReactor;
import prerna.query.querystruct.update.reactors.QueryUpdateReactor;
import prerna.sablecc2.reactor.algorithms.AnomalyReactor;
import prerna.sablecc2.reactor.algorithms.ClusteringAlgorithmReactor;
import prerna.sablecc2.reactor.algorithms.GenericRReactor;
import prerna.sablecc2.reactor.algorithms.LOFAlgorithmReactor;
import prerna.sablecc2.reactor.algorithms.MatrixRegressionReactor;
import prerna.sablecc2.reactor.algorithms.MultiClusteringAlgorithmReactor;
import prerna.sablecc2.reactor.algorithms.NLSQueryHelperReactor;
import prerna.sablecc2.reactor.algorithms.NaturalLanguageSearchReactor;
import prerna.sablecc2.reactor.algorithms.NumericalCorrelationReactor;
import prerna.sablecc2.reactor.algorithms.OutlierAlgorithmReactor;
import prerna.sablecc2.reactor.algorithms.RatioAlgorithmReactor;
import prerna.sablecc2.reactor.algorithms.SimilarityAlgorithmReactor;
import prerna.sablecc2.reactor.algorithms.UpdateNLPHistoryReactor;
import prerna.sablecc2.reactor.algorithms.WekaAprioriReactor;
import prerna.sablecc2.reactor.algorithms.WekaClassificationReactor;
import prerna.sablecc2.reactor.algorithms.xray.GetCSVSchemaReactor;
import prerna.sablecc2.reactor.algorithms.xray.GetExternalDBSchemaReactor;
import prerna.sablecc2.reactor.algorithms.xray.GetLocalDBSchemaReactor;
import prerna.sablecc2.reactor.algorithms.xray.GetXLSchemaReactor;
import prerna.sablecc2.reactor.algorithms.xray.GetXrayConfigFileReactor;
import prerna.sablecc2.reactor.algorithms.xray.GetXrayConfigListReactor;
import prerna.sablecc2.reactor.algorithms.xray.XRayReactor;
import prerna.sablecc2.reactor.algorithms.xray.XrayMetamodelReactor;
import prerna.sablecc2.reactor.app.DatabaseColumnUniqueReactor;
import prerna.sablecc2.reactor.app.GetAppWidgetsReactor;
import prerna.sablecc2.reactor.app.metaeditor.GetOwlDescriptionsReactor;
import prerna.sablecc2.reactor.app.metaeditor.GetOwlLogicalNamesReactor;
import prerna.sablecc2.reactor.app.metaeditor.OwlDictionaryReactor;
import prerna.sablecc2.reactor.app.metaeditor.OwlMetamodelReactor;
import prerna.sablecc2.reactor.app.metaeditor.ReloadAppOwlReactor;
import prerna.sablecc2.reactor.app.metaeditor.concepts.AddOwlConceptReactor;
import prerna.sablecc2.reactor.app.metaeditor.concepts.EditOwlConceptConceptualNameReactor;
import prerna.sablecc2.reactor.app.metaeditor.concepts.EditOwlConceptDataTypeReactor;
import prerna.sablecc2.reactor.app.metaeditor.concepts.RemoveOwlConceptReactor;
import prerna.sablecc2.reactor.app.metaeditor.meta.AddOwlDescriptionReactor;
import prerna.sablecc2.reactor.app.metaeditor.meta.AddOwlLogicalNamesReactor;
import prerna.sablecc2.reactor.app.metaeditor.meta.EditOwlDescriptionReactor;
import prerna.sablecc2.reactor.app.metaeditor.meta.EditOwlLogicalNamesReactor;
import prerna.sablecc2.reactor.app.metaeditor.meta.RemoveOwlDescriptionReactor;
import prerna.sablecc2.reactor.app.metaeditor.meta.RemoveOwlLogicalNamesReactor;
import prerna.sablecc2.reactor.app.metaeditor.properties.AddOwlPropertyReactor;
import prerna.sablecc2.reactor.app.metaeditor.properties.EditOwlPropertyConceptualNameReactor;
import prerna.sablecc2.reactor.app.metaeditor.properties.EditOwlPropertyDataTypeReactor;
import prerna.sablecc2.reactor.app.metaeditor.properties.RemoveOwlPropertyReactor;
import prerna.sablecc2.reactor.app.metaeditor.relationships.AddBulkOwlRelationshipsReactor;
import prerna.sablecc2.reactor.app.metaeditor.relationships.AddOwlRelationshipReactor;
import prerna.sablecc2.reactor.app.metaeditor.relationships.RemoveOwlRelationshipReactor;
import prerna.sablecc2.reactor.app.metaeditor.routines.OwlColumnSemanticCosineSimilarityMatchReactor;
import prerna.sablecc2.reactor.app.metaeditor.routines.OwlDirectNameMatchReactor;
import prerna.sablecc2.reactor.app.metaeditor.routines.OwlIndirectNameMatchReactor;
import prerna.sablecc2.reactor.app.metaeditor.routines.OwlInstanceSemanticCosineSimilarityMatchReactor;
import prerna.sablecc2.reactor.app.metaeditor.routines.PredictOwlDescriptionsReactor;
import prerna.sablecc2.reactor.app.metaeditor.routines.PredictOwlLogicalNamesReactor;
import prerna.sablecc2.reactor.app.upload.CheckHeadersReactor;
import prerna.sablecc2.reactor.app.upload.GenerateEmptyAppReactor;
import prerna.sablecc2.reactor.app.upload.ParseMetamodelReactor;
import prerna.sablecc2.reactor.app.upload.PredictCsvMetamodelReactor;
import prerna.sablecc2.reactor.app.upload.PredictExcelDataTypesReactor;
import prerna.sablecc2.reactor.app.upload.PredictExcelRangeMetadataReactor;
import prerna.sablecc2.reactor.app.upload.PredictFileDataTypesReactor;
import prerna.sablecc2.reactor.app.upload.gremlin.external.CreateExternalDSEGraphDBReactor;
import prerna.sablecc2.reactor.app.upload.gremlin.external.CreateExternalGraphDBReactor;
import prerna.sablecc2.reactor.app.upload.gremlin.external.GetDSEGraphMetaModelReactor;
import prerna.sablecc2.reactor.app.upload.gremlin.external.GetDSEGraphPropertiesReactor;
import prerna.sablecc2.reactor.app.upload.gremlin.external.GetGraphMetaModelReactor;
import prerna.sablecc2.reactor.app.upload.gremlin.external.GetGraphPropertiesReactor;
import prerna.sablecc2.reactor.app.upload.gremlin.file.TinkerCsvUploadReactor;
import prerna.sablecc2.reactor.app.upload.rdbms.csv.RdbmsCsvUploadReactor;
import prerna.sablecc2.reactor.app.upload.rdbms.csv.RdbmsFlatCsvUploadReactor;
import prerna.sablecc2.reactor.app.upload.rdbms.excel.RdbmsFlatExcelUploadReactor;
import prerna.sablecc2.reactor.app.upload.rdbms.excel.RdbmsLoaderSheetUploadReactor;
import prerna.sablecc2.reactor.app.upload.rdbms.external.ExternalJdbcSchemaReactor;
import prerna.sablecc2.reactor.app.upload.rdbms.external.ExternalJdbcTablesAndViewsReactor;
import prerna.sablecc2.reactor.app.upload.rdbms.external.RdbmsExternalUploadReactor;
import prerna.sablecc2.reactor.app.upload.rdf.RdfCsvUploadReactor;
import prerna.sablecc2.reactor.app.upload.rdf.RdfLoaderSheetUploadReactor;
import prerna.sablecc2.reactor.cluster.CleanUpAppsReactor;
import prerna.sablecc2.reactor.cluster.OpenAppReactor;
//import prerna.sablecc2.reactor.cluster.SyncRedisReactor;
import prerna.sablecc2.reactor.cluster.UpdateAppReactor;
import prerna.sablecc2.reactor.cluster.VersionReactor;
import prerna.sablecc2.reactor.export.AsTaskReactor;
import prerna.sablecc2.reactor.export.CollectAllReactor;
import prerna.sablecc2.reactor.export.CollectGraphReactor;
import prerna.sablecc2.reactor.export.CollectReactor;
import prerna.sablecc2.reactor.export.DropBoxUploaderReactor;
import prerna.sablecc2.reactor.export.EmptyDataReactor;
import prerna.sablecc2.reactor.export.GoogleUploaderReactor;
import prerna.sablecc2.reactor.export.GrabScalarElementReactor;
import prerna.sablecc2.reactor.export.IterateReactor;
import prerna.sablecc2.reactor.export.OneDriveUploaderReactor;
import prerna.sablecc2.reactor.export.ToCsvReactor;
import prerna.sablecc2.reactor.export.ToDatabaseReactor;
import prerna.sablecc2.reactor.export.ToExcelReactor;
import prerna.sablecc2.reactor.export.ToLoaderSheetReactor;
import prerna.sablecc2.reactor.export.ToTsvReactor;
import prerna.sablecc2.reactor.export.ToTxtReactor;
import prerna.sablecc2.reactor.expression.IfError;
import prerna.sablecc2.reactor.expression.OpAbsolute;
import prerna.sablecc2.reactor.expression.OpArrayLength;
import prerna.sablecc2.reactor.expression.OpAsString;
import prerna.sablecc2.reactor.expression.OpConcat;
import prerna.sablecc2.reactor.expression.OpContains;
import prerna.sablecc2.reactor.expression.OpIsEmpty;
import prerna.sablecc2.reactor.expression.OpLarge;
import prerna.sablecc2.reactor.expression.OpLen;
import prerna.sablecc2.reactor.expression.OpMatch;
import prerna.sablecc2.reactor.expression.OpMax;
import prerna.sablecc2.reactor.expression.OpMean;
import prerna.sablecc2.reactor.expression.OpMedian;
import prerna.sablecc2.reactor.expression.OpMin;
import prerna.sablecc2.reactor.expression.OpNotEmpty;
import prerna.sablecc2.reactor.expression.OpPaste;
import prerna.sablecc2.reactor.expression.OpPaste0;
import prerna.sablecc2.reactor.expression.OpPower;
import prerna.sablecc2.reactor.expression.OpRound;
import prerna.sablecc2.reactor.expression.OpSmall;
import prerna.sablecc2.reactor.expression.OpSum;
import prerna.sablecc2.reactor.expression.OpSumIf;
import prerna.sablecc2.reactor.expression.OpSumIfs;
import prerna.sablecc2.reactor.expression.OpSumProduct;
import prerna.sablecc2.reactor.expression.filter.OpAnd;
import prerna.sablecc2.reactor.expression.filter.OpOr;
import prerna.sablecc2.reactor.federation.AdvancedFederationBlend;
import prerna.sablecc2.reactor.federation.AdvancedFederationGetBestMatch;
import prerna.sablecc2.reactor.frame.CreateFrameReactor;
import prerna.sablecc2.reactor.frame.CurrentFrameReactor;
import prerna.sablecc2.reactor.frame.FrameDuplicatesReactor;
import prerna.sablecc2.reactor.frame.FrameHeaderExistsReactor;
import prerna.sablecc2.reactor.frame.FrameTypeReactor;
import prerna.sablecc2.reactor.frame.GetFrameHeaderMetadataReactor;
import prerna.sablecc2.reactor.frame.InsightMetamodelReactor;
import prerna.sablecc2.reactor.frame.SetCurrentFrameReactor;
import prerna.sablecc2.reactor.frame.filter.AddFrameFilterReactor;
import prerna.sablecc2.reactor.frame.filter.DeleteFrameFilterReactor;
import prerna.sablecc2.reactor.frame.filter.GetFrameFilterReactor;
import prerna.sablecc2.reactor.frame.filter.RemoveFrameFilterReactor;
import prerna.sablecc2.reactor.frame.filter.ReplaceFrameFilterReactor;
import prerna.sablecc2.reactor.frame.filter.SetFrameFilterReactor;
import prerna.sablecc2.reactor.frame.filter.UnfilterFrameReactor;
import prerna.sablecc2.reactor.frame.filtermodel.FrameFilterModelFilteredValuesReactor;
import prerna.sablecc2.reactor.frame.filtermodel.FrameFilterModelNumericRangeReactor;
import prerna.sablecc2.reactor.frame.filtermodel.FrameFilterModelReactor;
import prerna.sablecc2.reactor.frame.filtermodel.FrameFilterModelVisibleValuesReactor;
import prerna.sablecc2.reactor.frame.graph.ConnectedNodesReactor;
import prerna.sablecc2.reactor.frame.graph.FindPathsConnectingGroupsReactor;
import prerna.sablecc2.reactor.frame.graph.FindPathsConnectingNodesReactor;
import prerna.sablecc2.reactor.frame.graph.RemoveIntermediaryNodeReactor;
import prerna.sablecc2.reactor.frame.py.GenerateFrameFromPyVariableReactor;
import prerna.sablecc2.reactor.frame.py.PyReactor;
import prerna.sablecc2.reactor.frame.r.CompareDbSemanticSimiliarity;
import prerna.sablecc2.reactor.frame.r.GenerateFrameFromRVariableReactor;
import prerna.sablecc2.reactor.frame.r.GenerateH2FrameFromRVariableReactor;
import prerna.sablecc2.reactor.frame.r.GetSemanticDescription;
import prerna.sablecc2.reactor.frame.r.RReactor;
import prerna.sablecc2.reactor.frame.r.SemanticBlendingReactor;
import prerna.sablecc2.reactor.frame.r.analytics.RAprioriReactor;
import prerna.sablecc2.reactor.frame.r.analytics.RClassificationAlgorithmReactor;
import prerna.sablecc2.reactor.frame.r.analytics.RClusteringAlgorithmReactor;
import prerna.sablecc2.reactor.frame.r.analytics.RDocumentCosineSimilarityReactor;
import prerna.sablecc2.reactor.frame.r.analytics.RGenerateDescriptionColumnReactor;
import prerna.sablecc2.reactor.frame.r.analytics.RLOFAlgorithmReactor;
import prerna.sablecc2.reactor.frame.r.analytics.RMatrixRegressionReactor;
import prerna.sablecc2.reactor.frame.r.analytics.RNumericalColumnSimilarityReactor;
import prerna.sablecc2.reactor.frame.r.analytics.RNumericalCorrelationReactor;
import prerna.sablecc2.reactor.frame.r.analytics.RNumericalModelAlgorithmReactor;
import prerna.sablecc2.reactor.frame.r.analytics.ROutlierAlgorithmReactor;
import prerna.sablecc2.reactor.frame.r.analytics.RRandomForestAlgorithmReactor;
import prerna.sablecc2.reactor.frame.r.analytics.RRandomForestResultsReactor;
import prerna.sablecc2.reactor.frame.r.analytics.RSimilarityAlgorithmReactor;
import prerna.sablecc2.reactor.frame.r.analytics.RSimilarityHeatReactor;
import prerna.sablecc2.reactor.frame.r.graph.ClusterGraphReactor;
import prerna.sablecc2.reactor.frame.r.graph.GraphLayoutReactor;
import prerna.sablecc2.reactor.frame.r.graph.NodeDetailsReactor;
import prerna.sablecc2.reactor.frame.r.util.RSourceReactor;
import prerna.sablecc2.reactor.imports.ImportDataReactor;
import prerna.sablecc2.reactor.imports.MergeDataReactor;
import prerna.sablecc2.reactor.insights.ClearInsightReactor;
import prerna.sablecc2.reactor.insights.DropInsightReactor;
import prerna.sablecc2.reactor.insights.GetCurrentRecipeReactor;
import prerna.sablecc2.reactor.insights.GetSavedInsightRecipeReactor;
import prerna.sablecc2.reactor.insights.InsightHandleReactor;
import prerna.sablecc2.reactor.insights.OpenEmptyInsightReactor;
import prerna.sablecc2.reactor.insights.OpenInsightReactor;
import prerna.sablecc2.reactor.insights.OpenOptimizedInsightReactor;
import prerna.sablecc2.reactor.insights.RetrieveInsightOrnamentReactor;
import prerna.sablecc2.reactor.insights.SetInsightOrnamentReactor;
import prerna.sablecc2.reactor.insights.dashboard.DashboardInsightConfigReactor;
import prerna.sablecc2.reactor.insights.dashboard.ReloadInsightReactor;
import prerna.sablecc2.reactor.insights.save.DeleteInsightCacheReactor;
import prerna.sablecc2.reactor.insights.save.DeleteInsightReactor;
import prerna.sablecc2.reactor.insights.save.SaveInsightReactor;
import prerna.sablecc2.reactor.insights.save.SetInsightCacheableReactor;
import prerna.sablecc2.reactor.insights.save.SetInsightNameReactor;
import prerna.sablecc2.reactor.insights.save.UpdateInsightImageReactor;
import prerna.sablecc2.reactor.insights.save.UpdateInsightReactor;
import prerna.sablecc2.reactor.job.JobReactor;
import prerna.sablecc2.reactor.masterdatabase.AddLogicalNameReactor;
import prerna.sablecc2.reactor.masterdatabase.AddMetaDescriptionReactor;
import prerna.sablecc2.reactor.masterdatabase.AddMetaTagReactor;
import prerna.sablecc2.reactor.masterdatabase.DatabaseConceptPropertiesReactors;
import prerna.sablecc2.reactor.masterdatabase.DatabaseConceptsReactors;
import prerna.sablecc2.reactor.masterdatabase.DatabaseConnectionsReactor;
import prerna.sablecc2.reactor.masterdatabase.DatabaseListReactor;
import prerna.sablecc2.reactor.masterdatabase.DatabaseMetamodelReactor;
import prerna.sablecc2.reactor.masterdatabase.DatabaseQueryTranslator;
import prerna.sablecc2.reactor.masterdatabase.DatabaseSpecificConceptPropertiesReactor;
import prerna.sablecc2.reactor.masterdatabase.DatabaseTableStructureReactor;
import prerna.sablecc2.reactor.masterdatabase.DeleteMetaTagsReactor;
import prerna.sablecc2.reactor.masterdatabase.GetLogicalNamesReactor;
import prerna.sablecc2.reactor.masterdatabase.GetMetaDescriptionReactor;
import prerna.sablecc2.reactor.masterdatabase.GetMetaTagReactor;
import prerna.sablecc2.reactor.masterdatabase.GetPhysicalToLogicalMapping;
import prerna.sablecc2.reactor.masterdatabase.GetPhysicalToPhysicalMapping;
import prerna.sablecc2.reactor.masterdatabase.GetTraversalOptionsReactor;
import prerna.sablecc2.reactor.masterdatabase.RemoveLogicalNameReactor;
import prerna.sablecc2.reactor.masterdatabase.SyncAppWithLocalMasterReactor;
import prerna.sablecc2.reactor.panel.AddPanelConfigReactor;
import prerna.sablecc2.reactor.panel.AddPanelIfAbsentReactor;
import prerna.sablecc2.reactor.panel.AddPanelReactor;
import prerna.sablecc2.reactor.panel.CachedPanelCloneReactor;
import prerna.sablecc2.reactor.panel.CachedPanelReactor;
import prerna.sablecc2.reactor.panel.ClosePanelReactor;
import prerna.sablecc2.reactor.panel.GetInsightPanelsReactor;
import prerna.sablecc2.reactor.panel.GetPanelIdReactor;
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
import prerna.sablecc2.reactor.panel.rules.GetPanelColorByValueReactor;
import prerna.sablecc2.reactor.panel.rules.RetrievePanelColorByValueReactor;
import prerna.sablecc2.reactor.panel.sort.AddPanelSortReactor;
import prerna.sablecc2.reactor.panel.sort.RemovePanelSortReactor;
import prerna.sablecc2.reactor.panel.sort.SetPanelSortReactor;
import prerna.sablecc2.reactor.planner.GraphPlanReactor;
import prerna.sablecc2.reactor.planner.graph.ExecuteJavaGraphPlannerReactor;
import prerna.sablecc2.reactor.planner.graph.LoadGraphClient;
import prerna.sablecc2.reactor.planner.graph.UpdateGraphPlannerReactor2;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.reactor.qs.DistinctReactor;
import prerna.sablecc2.reactor.qs.ExecQueryReactor;
import prerna.sablecc2.reactor.qs.GroupByReactor;
import prerna.sablecc2.reactor.qs.JoinReactor;
import prerna.sablecc2.reactor.qs.LimitReactor;
import prerna.sablecc2.reactor.qs.OffsetReactor;
import prerna.sablecc2.reactor.qs.OrderByReactor;
import prerna.sablecc2.reactor.qs.OverrideImplicitFiltersReactor;
import prerna.sablecc2.reactor.qs.QueryAllReactor;
import prerna.sablecc2.reactor.qs.QueryInsertReactor;
import prerna.sablecc2.reactor.qs.QueryReactor;
import prerna.sablecc2.reactor.qs.WithReactor;
import prerna.sablecc2.reactor.qs.filter.QueryFilterReactor;
import prerna.sablecc2.reactor.qs.filter.QueryHavingFilterReactor;
import prerna.sablecc2.reactor.qs.selectors.AverageReactor;
import prerna.sablecc2.reactor.qs.selectors.CountReactor;
import prerna.sablecc2.reactor.qs.selectors.GenericSelectorFunctionReactor;
import prerna.sablecc2.reactor.qs.selectors.GroupConcatReactor;
import prerna.sablecc2.reactor.qs.selectors.MaxReactor;
import prerna.sablecc2.reactor.qs.selectors.MedianReactor;
import prerna.sablecc2.reactor.qs.selectors.MinReactor;
import prerna.sablecc2.reactor.qs.selectors.PathSelectorReactor;
import prerna.sablecc2.reactor.qs.selectors.QuerySelectReactor;
import prerna.sablecc2.reactor.qs.selectors.QuerySelectorExpressionAssimilator;
import prerna.sablecc2.reactor.qs.selectors.QuerySelectorPathReactor;
import prerna.sablecc2.reactor.qs.selectors.SelectTableReactor;
import prerna.sablecc2.reactor.qs.selectors.StandardDeviationReactor;
import prerna.sablecc2.reactor.qs.selectors.SumReactor;
import prerna.sablecc2.reactor.qs.selectors.UniqueAverageReactor;
import prerna.sablecc2.reactor.qs.selectors.UniqueCountReactor;
import prerna.sablecc2.reactor.qs.selectors.UniqueGroupConcatReactor;
import prerna.sablecc2.reactor.qs.selectors.UniqueSumReactor;
import prerna.sablecc2.reactor.qs.source.APIReactor;
import prerna.sablecc2.reactor.qs.source.AuditDatabaseReactor;
import prerna.sablecc2.reactor.qs.source.DatabaseReactor;
import prerna.sablecc2.reactor.qs.source.DirectJdbcConnectionReactor;
import prerna.sablecc2.reactor.qs.source.DropBoxFileRetrieverReactor;
import prerna.sablecc2.reactor.qs.source.DropBoxListFilesReactor;
import prerna.sablecc2.reactor.qs.source.FileSourceReactor;
import prerna.sablecc2.reactor.qs.source.FrameReactor;
import prerna.sablecc2.reactor.qs.source.GoogleFileRetrieverReactor;
import prerna.sablecc2.reactor.qs.source.GoogleListFilesReactor;
import prerna.sablecc2.reactor.qs.source.JdbcEngineConnectorReactor;
import prerna.sablecc2.reactor.qs.source.OneDriveFileRetrieverReactor;
import prerna.sablecc2.reactor.qs.source.OneDriveListFilesReactor;
import prerna.sablecc2.reactor.qs.source.SharePointDriveSelectorReactor;
import prerna.sablecc2.reactor.qs.source.SharePointFileRetrieverReactor;
import prerna.sablecc2.reactor.qs.source.SharePointListFilesReactor;
import prerna.sablecc2.reactor.qs.source.SharePointSiteSelectorReactor;
import prerna.sablecc2.reactor.qs.source.SharePointWebDavPullReactor;
import prerna.sablecc2.reactor.qs.source.URLSourceReactor;
import prerna.sablecc2.reactor.runtime.JavaReactor;
import prerna.sablecc2.reactor.runtime.codeexec.CodeExecReactor;
import prerna.sablecc2.reactor.scheduler.ListAllJobsReactor;
import prerna.sablecc2.reactor.scheduler.RescheduleExistingJobReactor;
import prerna.sablecc2.reactor.scheduler.ScheduleJobReactor;
import prerna.sablecc2.reactor.scheduler.UnscheduleJobReactor;
import prerna.sablecc2.reactor.storage.RetrieveValue;
import prerna.sablecc2.reactor.storage.StoreValue;
import prerna.sablecc2.reactor.storage.TaxRetrieveValue2;
import prerna.sablecc2.reactor.storage.TaxSaveScenarioReactor;
import prerna.sablecc2.reactor.task.AutoTaskOptionsReactor;
import prerna.sablecc2.reactor.task.RemoveTaskReactor;
import prerna.sablecc2.reactor.task.ResetPanelTasksReactor;
import prerna.sablecc2.reactor.task.ResetTaskReactor;
import prerna.sablecc2.reactor.task.TaskFormatReactor;
import prerna.sablecc2.reactor.task.TaskLookupReactor;
import prerna.sablecc2.reactor.task.TaskMetaCollectorReactor;
import prerna.sablecc2.reactor.task.TaskOptionsReactor;
import prerna.sablecc2.reactor.task.lambda.map.function.ApplyFormattingTaskReactor;
import prerna.sablecc2.reactor.task.modifiers.FilterLambdaTaskReactor;
import prerna.sablecc2.reactor.task.modifiers.FlatMapLambdaTaskReactor;
import prerna.sablecc2.reactor.task.modifiers.GenericMapLambdaTaskReactor;
import prerna.sablecc2.reactor.task.modifiers.MapLambdaTaskReactor;
import prerna.sablecc2.reactor.task.modifiers.ToNumericTypeTaskReactor;
import prerna.sablecc2.reactor.task.modifiers.ToUrlTypeTaskReactor;
import prerna.sablecc2.reactor.task.modifiers.TransposeRowTaskReactor;
import prerna.sablecc2.reactor.test.AliasMatchTestReactor;
import prerna.sablecc2.reactor.test.LSASpaceColumnLearnedReactor;
import prerna.sablecc2.reactor.test.RunLSILearnedReactor;
import prerna.sablecc2.reactor.utils.AddOperationAliasReactor;
import prerna.sablecc2.reactor.utils.BackupAppReactor;
import prerna.sablecc2.reactor.utils.BitlyAddReactor;
import prerna.sablecc2.reactor.utils.BitlyDeleteReactor;
import prerna.sablecc2.reactor.utils.BitlyQReactor;
import prerna.sablecc2.reactor.utils.BitlyUpdateReactor;
import prerna.sablecc2.reactor.utils.CheckRPackagesReactor;
import prerna.sablecc2.reactor.utils.CheckRecommendOptimizationReactor;
import prerna.sablecc2.reactor.utils.DatabaseProfileReactor;
import prerna.sablecc2.reactor.utils.DeleteAppReactor;
import prerna.sablecc2.reactor.utils.ExportAppReactor;
import prerna.sablecc2.reactor.utils.ExternalDatabaseProfileReactor;
import prerna.sablecc2.reactor.utils.GetRequestReactor;
import prerna.sablecc2.reactor.utils.GetUserInfoReactor;
import prerna.sablecc2.reactor.utils.GetWebTableHeader;
import prerna.sablecc2.reactor.utils.GetWebTableNum;
import prerna.sablecc2.reactor.utils.HelpReactor;
import prerna.sablecc2.reactor.utils.ImageCaptureReactor;
import prerna.sablecc2.reactor.utils.IsAppInsightReactor;
import prerna.sablecc2.reactor.utils.PostRequestReactor;
import prerna.sablecc2.reactor.utils.RemoveVariableReactor;
import prerna.sablecc2.reactor.utils.SendEmailReactor;
import prerna.sablecc2.reactor.utils.VariableExistsReactor;
import prerna.sablecc2.reactor.workflow.GetOptimizedRecipeReactor;
import prerna.sablecc2.reactor.workflow.InsightDatasourcesReactor;
import prerna.sablecc2.reactor.workflow.ModifyInsightDatasourceReactor;
import prerna.solr.reactor.AppInfoReactor;
import prerna.solr.reactor.AppInsightsReactor;
import prerna.solr.reactor.AppUsersReactors;
import prerna.solr.reactor.MyAppsReactor;
import prerna.solr.reactor.SetAppDescriptionReactor;
import prerna.solr.reactor.SetAppTagsReactor;
//import prerna.solr.reactor.SetInsightDescriptionReactor;
//import prerna.solr.reactor.SetInsightTagsReactor;
import prerna.util.DIHelper;
import prerna.util.Utility;
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
import prerna.util.git.reactors.SyncAppFilesOAuth;
import prerna.util.git.reactors.SyncAppOAuth;
import prerna.util.usertracking.reactors.AppMetaExtractor;
import prerna.util.usertracking.reactors.UpdateQueryDataReactor;
import prerna.util.usertracking.reactors.UpdateSemanticDataReactor;
import prerna.util.usertracking.reactors.WidgetTrackingReactor;
import prerna.util.usertracking.reactors.recommendations.DatabaseRecommendationReactor;
import prerna.util.usertracking.reactors.recommendations.GetDatabasesByDescriptionReactor;
import prerna.util.usertracking.reactors.recommendations.VisualizationRecommendationReactor;

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
	
	static {
		reactorHash = new HashMap<String, Class>();
		createReactorHash(reactorHash);
		// build expression hash
		expressionHash = new HashMap<String, Class>();
		populateExpressionSet(expressionHash);
		// populate the frame specific hashes
		rFrameHash = new HashMap<String, Class>();
		populateRFrameHash(rFrameHash);
		pandasFrameHash = new HashMap<String, Class>();
		populatePandasFrameHash(pandasFrameHash);
		h2FrameHash = new HashMap<String, Class>();
		populateH2FrameHash(h2FrameHash);
		tinkerFrameHash = new HashMap<String, Class>();
		populateTinkerFrameHash(tinkerFrameHash);
		nativeFrameHash = new HashMap<String, Class>();
		populateNativeFrameHash(nativeFrameHash);
		
		String additionalReactorsPath = "";
		try {
			additionalReactorsPath = DIHelper.getInstance().getProperty("ADDITIONAL_REACTORS");	
			File f = new File(additionalReactorsPath);
			if(f.exists()) {
				loadAdditionalReactor(f);
			}
		} catch(Exception e) {
			// ignore
			// this would only be null during testing
			// and DIHelper isn't loaded
			// hopefully you dont have anything in a prop file you care about
			// or update the var directly
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
		reactorHash.put("AddOperationAlias", AddOperationAliasReactor.class);
		reactorHash.put("VariableExists", VariableExistsReactor.class);
		reactorHash.put("RemoveVariable", RemoveVariableReactor.class);
		reactorHash.put("rm", RemoveVariableReactor.class);
		reactorHash.put("SendEmail", SendEmailReactor.class);
		reactorHash.put("BackupApp", BackupAppReactor.class);
		reactorHash.put("ExportApp", ExportAppReactor.class);
		reactorHash.put("DeleteApp", DeleteAppReactor.class);
		reactorHash.put("ImageCapture", ImageCaptureReactor.class);
		reactorHash.put("Help", HelpReactor.class);
		reactorHash.put("help", HelpReactor.class);
		reactorHash.put("DatabaseProfile", DatabaseProfileReactor.class);
		reactorHash.put("DatabaseColumnUnique", DatabaseColumnUniqueReactor.class);
		reactorHash.put("ExternalDatabaseProfile", ExternalDatabaseProfileReactor.class);
		reactorHash.put("GetRequest", GetRequestReactor.class);
		reactorHash.put("PostRequest", PostRequestReactor.class);
		reactorHash.put("IsAppInsight", IsAppInsightReactor.class);
		reactorHash.put("CheckRPackages", CheckRPackagesReactor.class);
		reactorHash.put("CheckRecommendOptimization", CheckRecommendOptimizationReactor.class);
		reactorHash.put("PredictExcelRangeMetadata", PredictExcelRangeMetadataReactor.class);
		reactorHash.put("DeleteInsightCache", DeleteInsightCacheReactor.class);
		reactorHash.put("WidgetT", WidgetTrackingReactor.class);
		reactorHash.put("GetUserInfo", GetUserInfoReactor.class);
		
		// Database uploading utils
		reactorHash.put("CheckHeaders", CheckHeadersReactor.class);
		reactorHash.put("PredictDataTypes", PredictFileDataTypesReactor.class);
		reactorHash.put("PredictExcelDataTypes", PredictExcelDataTypesReactor.class);
		reactorHash.put("PredictMetamodel", PredictCsvMetamodelReactor.class);
		reactorHash.put("ParseMetamodel", ParseMetamodelReactor.class);
		reactorHash.put("ExtractAppMeta", AppMetaExtractor.class);
		// Excel Data validation
		reactorHash.put("GetExcelForm", ExcelDataValidationReactor.class);
		
		// Database Uploading
		reactorHash.put("ExternalJdbcSchema", ExternalJdbcSchemaReactor.class);
		reactorHash.put("ExternalJdbcTablesAndViews", ExternalJdbcTablesAndViewsReactor.class);
		reactorHash.put("GenerateEmptyApp", GenerateEmptyAppReactor.class);
		reactorHash.put("RdbmsUploadTableData", RdbmsFlatCsvUploadReactor.class);
		reactorHash.put("RdbmsUploadExcelData", RdbmsFlatExcelUploadReactor.class);
		reactorHash.put("RdbmsExternalUpload", RdbmsExternalUploadReactor.class);
		reactorHash.put("RdbmsCsvUpload", RdbmsCsvUploadReactor.class);
		reactorHash.put("RdbmsLoaderSheetUpload", RdbmsLoaderSheetUploadReactor.class);
		reactorHash.put("RdfCsvUpload", RdfCsvUploadReactor.class);
		reactorHash.put("RdfLoaderSheetUpload", RdfLoaderSheetUploadReactor.class);
		reactorHash.put("TinkerCsvUpload", TinkerCsvUploadReactor.class);
			
		// external graph engine
		reactorHash.put("GetGraphProperties", GetGraphPropertiesReactor.class);
		reactorHash.put("GetGraphMetaModel", GetGraphMetaModelReactor.class);
		reactorHash.put("CreateExternalGraphDatabase", CreateExternalGraphDBReactor.class);
		// datastax graph reactors
		reactorHash.put("GetDSEGraphProperties", GetDSEGraphPropertiesReactor.class);
		reactorHash.put("GetDSEGraphMetaModel", GetDSEGraphMetaModelReactor.class);
		reactorHash.put("CreateExternalDSEGraphDatabase", CreateExternalDSEGraphDBReactor.class);
		
		// Query Struct Reactors
		// builds the select portion of the QS
		reactorHash.put("With", WithReactor.class);
		reactorHash.put("Select", QuerySelectReactor.class);
		reactorHash.put("SelectTable", SelectTableReactor.class);
		reactorHash.put("PSelect", QuerySelectorPathReactor.class);
		reactorHash.put("PCol", PathSelectorReactor.class);
		reactorHash.put("Average", AverageReactor.class);
		reactorHash.put("Mean", AverageReactor.class);
		reactorHash.put("UniqueAverage", UniqueAverageReactor.class);
		reactorHash.put("UniqueMean", UniqueAverageReactor.class);
		reactorHash.put("Sum", SumReactor.class);
		reactorHash.put("UniqueSum", UniqueSumReactor.class);
		reactorHash.put("Max", MaxReactor.class);
		reactorHash.put("Min", MinReactor.class);
		reactorHash.put("Median", MedianReactor.class);
		reactorHash.put("StandardDeviation", StandardDeviationReactor.class);
		reactorHash.put("Count", CountReactor.class);
		reactorHash.put("UniqueCount", UniqueCountReactor.class);
		reactorHash.put("GroupConcat", GroupConcatReactor.class);
		reactorHash.put("UniqueGroupConcat", UniqueGroupConcatReactor.class);
		reactorHash.put("Group", GroupByReactor.class);
		reactorHash.put("GroupBy", GroupByReactor.class);
		reactorHash.put("Sort", OrderByReactor.class);
		reactorHash.put("Order", OrderByReactor.class);
		reactorHash.put("Limit", LimitReactor.class);
		reactorHash.put("Offset", OffsetReactor.class);
		reactorHash.put("Join", JoinReactor.class);
		reactorHash.put("Filter", QueryFilterReactor.class);
		reactorHash.put("Having", QueryHavingFilterReactor.class);
		reactorHash.put("Query", QueryReactor.class);
		reactorHash.put("Distinct", DistinctReactor.class);
		reactorHash.put("ImplicitFilterOverride", OverrideImplicitFiltersReactor.class);
		reactorHash.put("QueryAll", QueryAllReactor.class);

		// modifications to database
		reactorHash.put("Insert", QueryInsertReactor.class);
		reactorHash.put("Delete", QueryDeleteReactor.class);
		reactorHash.put("Update", QueryUpdateReactor.class);
		reactorHash.put("ExecQuery", ExecQueryReactor.class);
		
		// If is in its own category
		reactorHash.put("if", IfReactor.class);

		// Data Source Reactors
		// specifies that our pixel operations after this point are dealing with the specified database
		reactorHash.put("Database", DatabaseReactor.class);
		reactorHash.put("AuditDatabase", AuditDatabaseReactor.class);
		reactorHash.put("API", APIReactor.class);
		reactorHash.put("FileRead", FileSourceReactor.class);
		reactorHash.put("JdbcSource", JdbcEngineConnectorReactor.class);
		reactorHash.put("DirectJDBCConnection", DirectJdbcConnectionReactor.class);
		reactorHash.put("URLSource", URLSourceReactor.class);
		// drop box
		reactorHash.put("DropBoxUploader", DropBoxUploaderReactor.class);
		reactorHash.put("DropBoxListFiles", DropBoxListFilesReactor.class);
		reactorHash.put("DropBoxFileRetriever", DropBoxFileRetrieverReactor.class);
		// one drive
		reactorHash.put("OneDriveUploader", OneDriveUploaderReactor.class);
		reactorHash.put("OneDriveListFiles", OneDriveListFilesReactor.class);
		reactorHash.put("OneDriveFileRetriever", OneDriveFileRetrieverReactor.class);
		// google
		reactorHash.put("GoogleUploader", GoogleUploaderReactor.class);
		reactorHash.put("GoogleListFiles", GoogleListFilesReactor.class);
		reactorHash.put("GoogleFileRetriever", GoogleFileRetrieverReactor.class);
		// share point
		reactorHash.put("SharePointListFiles", SharePointListFilesReactor.class);
		reactorHash.put("SharePointFileRetriever", SharePointFileRetrieverReactor.class);
		reactorHash.put("SharePointSiteSelector", SharePointSiteSelectorReactor.class);
		reactorHash.put("SharePointDriveSelector", SharePointDriveSelectorReactor.class);
		reactorHash.put("SharePointWebDavPull", SharePointWebDavPullReactor.class);
		// survey monkey
		reactorHash.put("SurveyMonkeyListSurveys", GetSurveyMonkeySurveysReactor.class);
		reactorHash.put("NaturalLanguageSearch", NaturalLanguageSearchReactor.class);
		
		// specifies that our pksl operations after this point are dealing with the specified frame
		reactorHash.put("Frame", FrameReactor.class);
		reactorHash.put("CreateFrame", CreateFrameReactor.class);
		reactorHash.put("FrameType", FrameTypeReactor.class);
		reactorHash.put("GenerateFrameFromRVariable", GenerateFrameFromRVariableReactor.class);
		reactorHash.put("GenerateFrameFromPyVariable", GenerateFrameFromPyVariableReactor.class);
		reactorHash.put("GenerateH2FrameFromRVariable", GenerateH2FrameFromRVariableReactor.class);
		//reactorHash.put("SynchronizeToR", SynchronizeToRReactor.class);

		// Task Reactors
		reactorHash.put("Iterate", IterateReactor.class);
		reactorHash.put("Task", TaskLookupReactor.class); // defines the task
		reactorHash.put("ResetTask", ResetTaskReactor.class); // reset a task
		reactorHash.put("ResetAll", ResetPanelTasksReactor.class); // reset all panel tasks
		reactorHash.put("RemoveTask", RemoveTaskReactor.class);
		reactorHash.put("Collect", CollectReactor.class); // collect from task
		reactorHash.put("CollectAll", CollectAllReactor.class); // collect from task
		reactorHash.put("CollectGraph", CollectGraphReactor.class); // collect from task
		reactorHash.put("GrabScalarElement", GrabScalarElementReactor.class);
		reactorHash.put("AsTask", AsTaskReactor.class);
		reactorHash.put("EmptyData", EmptyDataReactor.class);
		reactorHash.put("CollectMeta", TaskMetaCollectorReactor.class); // collect meta from task
		reactorHash.put("Format", TaskFormatReactor.class); // set formats
		reactorHash.put("TaskOptions", TaskOptionsReactor.class); // set options
		reactorHash.put("AutoTaskOptions", AutoTaskOptionsReactor.class);
		reactorHash.put("ToCsv", ToCsvReactor.class); // take any task and output to a file
		reactorHash.put("ToTsv", ToTsvReactor.class); // take any task and output to a file
		reactorHash.put("ToTxt", ToTxtReactor.class); // take any task and output to a file
		reactorHash.put("ToExcel", ToExcelReactor.class); // take any task and output to a file
		reactorHash.put("ToDatabase", ToDatabaseReactor.class);
		reactorHash.put("ToLoaderSheet", ToLoaderSheetReactor.class);
		
		// Task Operations
		reactorHash.put("CodeLambda", GenericMapLambdaTaskReactor.class);
		reactorHash.put("FlatMapLambda", FlatMapLambdaTaskReactor.class);
		reactorHash.put("MapLambda", MapLambdaTaskReactor.class);
		reactorHash.put("FilterLambda", FilterLambdaTaskReactor.class);
		reactorHash.put("ToNumericType", ToNumericTypeTaskReactor.class);
		reactorHash.put("ToUrlType", ToUrlTypeTaskReactor.class);
		reactorHash.put("TransposeRows", TransposeRowTaskReactor.class);
		reactorHash.put("ApplyFormatting", ApplyFormattingTaskReactor.class);

		// Local Master Reactors
		// TODO: remove ones no longer used
		reactorHash.put("GetDatabaseList", DatabaseListReactor.class);
		reactorHash.put("GetDatabaseConcepts", DatabaseConceptsReactors.class);
		reactorHash.put("GetTraversalOptions", GetTraversalOptionsReactor.class);
		reactorHash.put("GetDatabaseMetamodel", DatabaseMetamodelReactor.class);
		reactorHash.put("GetConceptProperties", DatabaseConceptPropertiesReactors.class);
		
		// NEW FEDERATE
		reactorHash.put("GetDatabaseConnections", DatabaseConnectionsReactor.class);
		reactorHash.put("GetDatabaseTableStructure", DatabaseTableStructureReactor.class);
		reactorHash.put("GetSpecificConceptProperties", DatabaseSpecificConceptPropertiesReactor.class);
		reactorHash.put("FederationBlend", AdvancedFederationBlend.class);
		reactorHash.put("FederationBestMatches", AdvancedFederationGetBestMatch.class);

		// app meta and local master utilities
		reactorHash.put("ReloadAppOwl", ReloadAppOwlReactor.class);
		reactorHash.put("GetOwlMetamodel", OwlMetamodelReactor.class);
		reactorHash.put("GetOwlDictionary", OwlDictionaryReactor.class);
		// owl concepts
		reactorHash.put("AddOwlConcept", AddOwlConceptReactor.class);
		reactorHash.put("RemoveOwlConcept", RemoveOwlConceptReactor.class);
		// owl properties
		reactorHash.put("AddOwlProperty", AddOwlPropertyReactor.class);
		reactorHash.put("RemoveOwlProperty", RemoveOwlPropertyReactor.class);
		// owl relationships
		reactorHash.put("AddOwlRelationship", AddOwlRelationshipReactor.class);
		reactorHash.put("AddBulkOwlRelationships", AddBulkOwlRelationshipsReactor.class);
		reactorHash.put("RemoveOwlRelationship", RemoveOwlRelationshipReactor.class);
		// conceptual names 
		reactorHash.put("EditOwlConceptConceptualName", EditOwlConceptConceptualNameReactor.class);
		reactorHash.put("EditOwlPropertyConceptualName", EditOwlPropertyConceptualNameReactor.class);
		// data types
		reactorHash.put("EditOwlConceptDataType", EditOwlConceptDataTypeReactor.class);
		reactorHash.put("EditOwlPropertyDataType", EditOwlPropertyDataTypeReactor.class);
		// logical names
		reactorHash.put("AddOwlLogicalNames", AddOwlLogicalNamesReactor.class);
		reactorHash.put("EditOwlLogicalNames", EditOwlLogicalNamesReactor.class);
		reactorHash.put("RemoveOwlLogicalNames", RemoveOwlLogicalNamesReactor.class);
		reactorHash.put("GetOwlLogicalNames", GetOwlLogicalNamesReactor.class);
		reactorHash.put("PredictOwlLogicalNames", PredictOwlLogicalNamesReactor.class);
		// descriptions
		reactorHash.put("AddOwlDescription", AddOwlDescriptionReactor.class);
		reactorHash.put("EditOwlDescription", EditOwlDescriptionReactor.class);
		reactorHash.put("RemoveOwlDescription", RemoveOwlDescriptionReactor.class);
		reactorHash.put("GetOwlDescriptions", GetOwlDescriptionsReactor.class);
		reactorHash.put("PredictOwlDescription", PredictOwlDescriptionsReactor.class);
		// routines to predict owl information
		reactorHash.put("FindDirectOwlRelationships", OwlDirectNameMatchReactor.class);
		reactorHash.put("FindIndirectOwlRelationships", OwlIndirectNameMatchReactor.class);
		reactorHash.put("FindSemanticColumnOwlRelationships", OwlColumnSemanticCosineSimilarityMatchReactor.class);
		reactorHash.put("FindSemanticInstanceOwlRelationships", OwlInstanceSemanticCosineSimilarityMatchReactor.class);
		
		reactorHash.put("SyncAppWithLocalMaster", SyncAppWithLocalMasterReactor.class);
		reactorHash.put("QueryTranslator", DatabaseQueryTranslator.class);
		// logical name operations
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

		// Panel Reactors
		reactorHash.put("InsightPanelIds", GetInsightPanelsReactor.class);
		reactorHash.put("Panel", PanelReactor.class);
		reactorHash.put("CachedPanel", CachedPanelReactor.class);
		reactorHash.put("CachedPanelClone", CachedPanelCloneReactor.class);
		reactorHash.put("AddPanel", AddPanelReactor.class);
		reactorHash.put("AddPanelIfAbsent", AddPanelIfAbsentReactor.class);
		reactorHash.put("GetPanelId", GetPanelIdReactor.class);
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
		// panel configuration
		reactorHash.put("AddPanelConfig", AddPanelConfigReactor.class);
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
		reactorHash.put("GetPanelColorByValue", GetPanelColorByValueReactor.class);
		
		// new tab in browser
		reactorHash.put("OpenTab", OpenTabReactor.class);

		// Insight Reactors
		reactorHash.put("InsightRecipe", GetSavedInsightRecipeReactor.class);
		reactorHash.put("OpenInsight", OpenInsightReactor.class);
		reactorHash.put("ReloadInsight", ReloadInsightReactor.class);
		reactorHash.put("OpenOptimizedInsight", OpenOptimizedInsightReactor.class);
		reactorHash.put("OpenEmptyInsight", OpenEmptyInsightReactor.class);
		reactorHash.put("DropInsight", DropInsightReactor.class);
		reactorHash.put("ClearInsight", ClearInsightReactor.class);
		reactorHash.put("InsightHandle", InsightHandleReactor.class);
		reactorHash.put("SetInsightOrnament", SetInsightOrnamentReactor.class);
		reactorHash.put("RetrieveInsightOrnament", RetrieveInsightOrnamentReactor.class);
		reactorHash.put("UpdateInsightImage", UpdateInsightImageReactor.class);
		reactorHash.put("GetCurrentRecipe", GetCurrentRecipeReactor.class);

		// Save Reactors
		reactorHash.put("SaveInsight", SaveInsightReactor.class);
		reactorHash.put("UpdateInsight", UpdateInsightReactor.class);
		reactorHash.put("DeleteInsight", DeleteInsightReactor.class);
		reactorHash.put("SetInsightName", SetInsightNameReactor.class);
		reactorHash.put("SetInsightCacheable", SetInsightCacheableReactor.class);

		// Dashboard Reactors
		reactorHash.put("DashboardInsightConfig", DashboardInsightConfigReactor.class);

		// General Frame Reactors
		reactorHash.put("FrameHeaders", GetFrameHeaderMetadataReactor.class);
		reactorHash.put("FrameHeaderExists", FrameHeaderExistsReactor.class);
		reactorHash.put("AddFrameFilter", AddFrameFilterReactor.class);
		reactorHash.put("GetFrameFilters", GetFrameFilterReactor.class);
		reactorHash.put("SetFrameFilter", SetFrameFilterReactor.class);
		reactorHash.put("RemoveFrameFilter", RemoveFrameFilterReactor.class);
		reactorHash.put("ReplaceFrameFilter", ReplaceFrameFilterReactor.class);
		reactorHash.put("DeleteFrameFilter", DeleteFrameFilterReactor.class);
		reactorHash.put("UnfilterFrame", UnfilterFrameReactor.class);
		reactorHash.put("InsightMetamodel", InsightMetamodelReactor.class);
		reactorHash.put("HasDuplicates", FrameDuplicatesReactor.class);
		reactorHash.put("CurrentFrame", CurrentFrameReactor.class);
		reactorHash.put("SetCurrentFrame", SetCurrentFrameReactor.class);
		// filter model
		reactorHash.put("FrameFilterModel", FrameFilterModelReactor.class);
		reactorHash.put("FrameFilterModelFilteredValues", FrameFilterModelFilteredValuesReactor.class);
		reactorHash.put("FrameFilterModelVisibleValues", FrameFilterModelVisibleValuesReactor.class);
		reactorHash.put("FrameFilterModelNumericRange", FrameFilterModelNumericRangeReactor.class);

		// Algorithm Reactors
		reactorHash.put("rAlg", GenericRReactor.class);
		reactorHash.put("RunClustering", ClusteringAlgorithmReactor.class);
		reactorHash.put("RunMultiClustering", MultiClusteringAlgorithmReactor.class);
		reactorHash.put("RunLOF", LOFAlgorithmReactor.class);
		reactorHash.put("RunSimilarity", SimilarityAlgorithmReactor.class);
		reactorHash.put("RunOutlier", OutlierAlgorithmReactor.class);
		reactorHash.put("Ratio", RatioAlgorithmReactor.class);
		reactorHash.put("RunAnomaly", AnomalyReactor.class);
		
		// X-Ray reactors
		reactorHash.put("RunXray", XRayReactor.class);
		reactorHash.put("GetXrayConfigList", GetXrayConfigListReactor.class);
		reactorHash.put("GetXrayConfigFile", GetXrayConfigFileReactor.class);
		reactorHash.put("GetLocalSchema", GetLocalDBSchemaReactor.class);
		reactorHash.put("GetXLSchema", GetXLSchemaReactor.class);
		reactorHash.put("GetCSVSchema",GetCSVSchemaReactor.class);
		reactorHash.put("GetExternalSchema", GetExternalDBSchemaReactor.class);
		reactorHash.put("XrayMetamodel", XrayMetamodelReactor.class);
		reactorHash.put("MetaSemanticSimilarity", CompareDbSemanticSimiliarity.class);
		
		reactorHash.put("SemanticBlending", SemanticBlendingReactor.class);
		reactorHash.put("SemanticDescription", GetSemanticDescription.class);
		// similar reactors to x-ray
		reactorHash.put("GetPhysicalToLogicalMapping", GetPhysicalToLogicalMapping.class);
		reactorHash.put("GetPhysicalToPhysicalMapping", GetPhysicalToPhysicalMapping.class);

		// these algorithms return viz data to the FE
		reactorHash.put("RunNumericalCorrelation", NumericalCorrelationReactor.class);
		reactorHash.put("RunMatrixRegression", MatrixRegressionReactor.class);
		reactorHash.put("RunClassification", WekaClassificationReactor.class);
		reactorHash.put("RunAssociatedLearning", WekaAprioriReactor.class);

		// In mem storage of data
		reactorHash.put("StoreValue", StoreValue.class);
		reactorHash.put("RetrieveValue", RetrieveValue.class);
		reactorHash.put("GraphPlan", GraphPlanReactor.class);
		
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
		reactorHash.put("CreateAsset", prerna.util.git.reactors.CreateAssetReactor.class);
		reactorHash.put("UpdateAsset", prerna.util.git.reactors.UpdateAssetReactor.class);
		reactorHash.put("DeleteAsset", prerna.util.git.reactors.DeleteAssetReactor.class);
		reactorHash.put("SyncAppO", SyncAppOAuth.class);
		reactorHash.put("SyncAppFilesO", SyncAppFilesOAuth.class);
		
		// App Metadata
		reactorHash.put("MyApps", MyAppsReactor.class);
		reactorHash.put("AppInfo", AppInfoReactor.class);
		reactorHash.put("AppUsers", AppUsersReactors.class);
		// TODO: to be removed once FE changes to only use GetInsights
		reactorHash.put("GetAppInsights", AppInsightsReactor.class);
		reactorHash.put("GetInsights", AppInsightsReactor.class);
		reactorHash.put("SetAppDescription", SetAppDescriptionReactor.class);
		reactorHash.put("SetAppTags", SetAppTagsReactor.class);
		reactorHash.put("GetAppWidgets", GetAppWidgetsReactor.class);
		// Insight Metadata
//		reactorHash.put("SetInsightTags", SetInsightTagsReactor.class);
//		reactorHash.put("SetInsightDescription", SetInsightDescriptionReactor.class);


		// Insight Comments
		reactorHash.put("AddInsightComment", AddInsightCommentReactor.class);
		reactorHash.put("DeleteInsightComment", DeleteInsightCommentReactor.class);
		reactorHash.put("ModifyInsightComment", ModifyInsightCommentReactor.class);
		reactorHash.put("GetInsightComments", GetInsightCommentsReactor.class);
		
		// Clusters
		reactorHash.put("OpenApp", OpenAppReactor.class);
		reactorHash.put("UpdateApp", UpdateAppReactor.class);
		reactorHash.put("CleanUpApps", CleanUpAppsReactor.class);
		reactorHash.put("Version", VersionReactor.class);
		//reactorHash.put("PullCloudApp", PullCloudAppReactor.class);
		//reactorHash.put("SyncRedis", SyncRedisReactor.class);
		reactorHash.put("PushApps", PushAppsReactor.class);

		
		

		// Scheduler
		reactorHash.put("ScheduleJob", ScheduleJobReactor.class);
		reactorHash.put("UnscheduleJob", UnscheduleJobReactor.class);
		reactorHash.put("ListAllJobs", ListAllJobsReactor.class);
		reactorHash.put("RescheduleExistingJob", RescheduleExistingJobReactor.class);
		// User Tracking
		reactorHash.put("UpdateSemanticData", UpdateSemanticDataReactor.class);
		reactorHash.put("UpdateQueryData", UpdateQueryDataReactor.class);
		// Recommendations
		reactorHash.put("DatabaseRecommendations", DatabaseRecommendationReactor.class);
		reactorHash.put("VizRecommendations", VisualizationRecommendationReactor.class);
		reactorHash.put("GetDatabasesByDescription", GetDatabasesByDescriptionReactor.class);
		reactorHash.put("UpdateNLPHistory", UpdateNLPHistoryReactor.class);
		reactorHash.put("NLSQueryHelper", NLSQueryHelperReactor.class);
		
		// Dates
		reactorHash.put("Date", DateReactor.class);
		
		// Forms
		reactorHash.put("UpdateForm", FormsReactor.class);
		
		// Pixels for legacy playsheets
		reactorHash.put("RunPlaysheetMethod", prerna.sablecc2.reactor.legacy.playsheets.RunPlaysheetMethodReactor.class);
		reactorHash.put("RunPlaysheet", prerna.sablecc2.reactor.legacy.playsheets.RunPlaysheetReactor.class);
		reactorHash.put("GetPlaysheetParams", prerna.sablecc2.reactor.legacy.playsheets.GetPlaysheetParamsReactor.class);
		
		//LSA
		reactorHash.put("LSASpaceColumnLearned", LSASpaceColumnLearnedReactor.class);
		reactorHash.put("RunLSILearned", RunLSILearnedReactor.class); 
		//reactorHash.put("LSADescriptionAdd", LSADescriptionAddReactor.class); 
		//reactorHash.put("CreateCriteriaFromText", CreateCriteriaFromTextReactor.class); 
		//reactorHash.put("LSASpaceColumn", LSASpaceColumnReactor.class); 
		//reactorHash.put("RunLSI", RunLSIReactor.class); 
		
		// General Code Execution
		reactorHash.put("CodeExec", CodeExecReactor.class);
		reactorHash.put("Py", PyReactor.class);
		reactorHash.put("R", RReactor.class);
		reactorHash.put("RSource", RSourceReactor.class);
		reactorHash.put("Java", JavaReactor.class);
		
		// Pixel Recipe Parsing / Manipulation
		reactorHash.put("GetInsightDatasources", InsightDatasourcesReactor.class);
		reactorHash.put("ModifyInsightDatasource", ModifyInsightDatasourceReactor.class);
		reactorHash.put("GetOptimizedRecipe", GetOptimizedRecipeReactor.class);
		
		
		// web scrape engine
		reactorHash.put("GetTableHeader", GetWebTableHeader.class);
		reactorHash.put("GetNumTable", GetWebTableNum.class);
		
		// Tax specific handles
		reactorHash.put("LoadClient", LoadGraphClient.class);
		reactorHash.put("RunPlan", ExecuteJavaGraphPlannerReactor.class);
		reactorHash.put("UpdatePlan", UpdateGraphPlannerReactor2.class);
		reactorHash.put("TaxRetrieveValue", TaxRetrieveValue2.class);
		reactorHash.put("RunAliasMatch", AliasMatchTestReactor.class);
		reactorHash.put("SaveTaxScenario", TaxSaveScenarioReactor.class);
		
		// bitly
		reactorHash.put("badd", BitlyAddReactor.class);
		reactorHash.put("bupd", BitlyUpdateReactor.class);
		reactorHash.put("bdel", BitlyDeleteReactor.class);
		reactorHash.put("bq", BitlyQReactor.class);
		
	}

	private static void populateNativeFrameHash(Map<String, Class> nativeFrameHash) {

	}

	private static void populateH2FrameHash(Map<String, Class> h2FrameHash) {
		h2FrameHash.put("AddColumn", prerna.sablecc2.reactor.frame.rdbms.AddColumnReactor.class);
		h2FrameHash.put("ChangeColumnType", prerna.sablecc2.reactor.frame.rdbms.ChangeColumnTypeReactor.class);
		h2FrameHash.put("CountIf", prerna.sablecc2.reactor.frame.rdbms.CountIfReactor.class);
		h2FrameHash.put("DropColumn", prerna.sablecc2.reactor.frame.rdbms.DropColumnReactor.class);
		h2FrameHash.put("DropRows", prerna.sablecc2.reactor.frame.rdbms.DropRowsReactor.class);
		h2FrameHash.put("DuplicateColumn", prerna.sablecc2.reactor.frame.rdbms.DuplicateColumnReactor.class);
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
		rFrameHash.put("AutoCleanColumn", prerna.sablecc2.reactor.frame.r.AutoCleanColumnReactor.class);
		rFrameHash.put("ChangeColumnType", prerna.sablecc2.reactor.frame.r.ChangeColumnTypeReactor.class);
		rFrameHash.put("CountIf", prerna.sablecc2.reactor.frame.r.CountIfReactor.class);
		rFrameHash.put("Collapse", prerna.sablecc2.reactor.frame.r.CollapseReactor.class);
		rFrameHash.put("DropColumn", prerna.sablecc2.reactor.frame.r.DropColumnReactor.class);
		rFrameHash.put("DropRows", prerna.sablecc2.reactor.frame.r.DropRowsReactor.class);
		rFrameHash.put("DuplicateColumn", prerna.sablecc2.reactor.frame.r.DuplicateColumnReactor.class);
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
		rFrameHash.put("ToProperCase", prerna.sablecc2.reactor.frame.r.ToProperCaseReactor.class);
		rFrameHash.put("TrimColumns", prerna.sablecc2.reactor.frame.r.TrimReactor.class);
		rFrameHash.put("Transpose", prerna.sablecc2.reactor.frame.r.TransposeReactor.class);
		rFrameHash.put("Unpivot", prerna.sablecc2.reactor.frame.r.UnpivotReactor.class);
		rFrameHash.put("UpdateRowValues", prerna.sablecc2.reactor.frame.r.UpdateRowValuesWhereColumnContainsValueReactor.class);
		rFrameHash.put("Discretize", prerna.sablecc2.reactor.frame.r.DiscretizeReactor.class);

		// frame stats
		rFrameHash.put("ColumnCount", prerna.sablecc2.reactor.frame.r.ColumnCountReactor.class);
		rFrameHash.put("DescriptiveStats", prerna.sablecc2.reactor.frame.r.DescriptiveStatsReactor.class);
		rFrameHash.put("SummaryStats", prerna.sablecc2.reactor.frame.r.SummaryStatsReactor.class);
		rFrameHash.put("Histogram", prerna.sablecc2.reactor.frame.r.HistogramReactor.class);
		
		// rules
		rFrameHash.put("AddRule", prerna.sablecc2.reactor.frame.r.rules.AddRuleReactor.class);
		rFrameHash.put("RunRules", prerna.sablecc2.reactor.frame.r.rules.RunRulesReactor.class);
		rFrameHash.put("GetRuleTypes", prerna.sablecc2.reactor.frame.r.rules.GetRuleTypesReactor.class);
		
		// algorithms
		rFrameHash.put("RunAssociatedLearning", RAprioriReactor.class);
		rFrameHash.put("RunClassification", RClassificationAlgorithmReactor.class);
		rFrameHash.put("RunClustering", RClusteringAlgorithmReactor.class);
		rFrameHash.put("RunDescriptionGenerator", RGenerateDescriptionColumnReactor.class);
		rFrameHash.put("RunDocCosSimilarity", RDocumentCosineSimilarityReactor.class);
		rFrameHash.put("RunLOF", RLOFAlgorithmReactor.class);
		rFrameHash.put("RunMatrixRegression", RMatrixRegressionReactor.class);
		rFrameHash.put("RunNumericalColumnSimilarity", RNumericalColumnSimilarityReactor.class);
		rFrameHash.put("RunNumericalCorrelation", RNumericalCorrelationReactor.class);
		rFrameHash.put("RunNumericalModel", RNumericalModelAlgorithmReactor.class);
		rFrameHash.put("RunOutlier", ROutlierAlgorithmReactor.class);
		rFrameHash.put("RunRandomForest", RRandomForestAlgorithmReactor.class);
		rFrameHash.put("GetRFResults", RRandomForestResultsReactor.class);
		rFrameHash.put("RunSimilarity", RSimilarityAlgorithmReactor.class);
		rFrameHash.put("RunSimilarityHeat", RSimilarityHeatReactor.class);
		rFrameHash.put("MatchColumnValues", prerna.sablecc2.reactor.frame.r.PredictSimilarColumnValuesReactor.class);
		rFrameHash.put("UpdateMatchColumnValues", prerna.sablecc2.reactor.frame.r.UpdateSimilarColumnValuesReactor.class);
		rFrameHash.put("MetaSemanticSimilarity", CompareDbSemanticSimiliarity.class);

		
	}

	private static void populateTinkerFrameHash(Map<String, Class> tinkerFrameHash) {
		tinkerFrameHash.put("ConnectedNodes", ConnectedNodesReactor.class);
		tinkerFrameHash.put("RemoveIntermediaryNode", RemoveIntermediaryNodeReactor.class);
		tinkerFrameHash.put("FindPathsConnectingNodes", FindPathsConnectingNodesReactor.class);
		tinkerFrameHash.put("FindPathsConnectingGroups", FindPathsConnectingGroupsReactor.class);
		// require r
		tinkerFrameHash.put("ChangeGraphLayout", GraphLayoutReactor.class);
		tinkerFrameHash.put("ClusterGraph", ClusterGraphReactor.class);
		tinkerFrameHash.put("NodeDetails", NodeDetailsReactor.class);
	}
	
	private static void populatePandasFrameHash(Map<String, Class> pandasFrameHash) {
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
		expressionHash.put("LEN", OpLen.class);
		expressionHash.put("IFERROR", IfError.class);
		expressionHash.put("NOTEMPTY", OpNotEmpty.class);
		expressionHash.put("ISEMPTY", OpIsEmpty.class);
		expressionHash.put("ASSTRING", OpAsString.class);
		expressionHash.put("CONCAT", OpConcat.class);
		
		// none excel functions
		expressionHash.put("PASTE0", OpPaste0.class);
		expressionHash.put("PASTE", OpPaste.class);
		expressionHash.put("CONTAINS", OpContains.class);
		expressionHash.put("ARRAYLENGTH", OpArrayLength.class);
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
		
		/*
		 * I want to account for various functions that a person wants to execute
		 * I will just create this as a generic function reactor 
		 * that creates a function selector to return 
		 */
		if (parentReactor instanceof AbstractQueryStructReactor || 
				parentReactor instanceof QuerySelectorExpressionAssimilator) {
			reactor = new GenericSelectorFunctionReactor();
			reactor.setPixel(reactorId, nodeString);
			// set the fuction name
			((GenericSelectorFunctionReactor) reactor).setFunction(reactorId);
			return reactor;
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
	
	/**
	 * Loads the JSON for additional reactors
	 * IF NAMES COLLIDE, THE PROP FILE WILL TAKE PRECEDENCE
	 * @param jsonFile 
	 * 
	 */
	private static void loadAdditionalReactor(File jsonFile) {
		Map<String, Map<String, String>> jsonData = null;
		try {
			jsonData = new ObjectMapper().readValue(jsonFile, Map.class);
		} catch(Exception e) {
			// oops...
			System.out.println("COULDN'T LOAD JSON FILE FOR ADDITIOANL REACTORS!!!!!");
		}
		if(jsonData != null) {
			for(String key : jsonData.keySet()) {
				Map<String, Class> hash = getReactorsForType(key);
				if(hash != null) {
					Map<String, String> reactorNameToClass = jsonData.get(key);
					for(String reactorName : reactorNameToClass.keySet()) {
						String classname = reactorNameToClass.get(reactorName);
						Class reactor;
						try {
							reactor = (Class.forName(classname));
							hash.put(reactorName, reactor);
						} catch (ClassNotFoundException e) {
							System.out.println("COULDN'T FIND THE REACTOR!");
							e.printStackTrace();
						}
					}
				}
			}
		}
	}
	
	private static Map<String, Class> getReactorsForType(String key) {
		key = key.toUpperCase();
		if(key.equals("GENERAL")) {
			return reactorHash;
		} else if(key.equals("H2")) {
			return h2FrameHash;
		} else if(key.equals("R")) {
			return rFrameHash;
		} else if(key.equals("PY")) {
			return pandasFrameHash;
		} else if(key.equals("NATIVE")) {
			return nativeFrameHash;
		} else if(key.equals("TINKER")) {
			return tinkerFrameHash;
		}
		return null;
	}
	
}
