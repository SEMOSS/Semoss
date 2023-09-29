package prerna.sablecc2.reactor;

import java.io.File;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import me.xdrop.fuzzywuzzy.FuzzySearch;
import me.xdrop.fuzzywuzzy.model.ExtractedResult;
import prerna.algorithm.api.ITableDataFrame;
import prerna.aws.s3.PushAssetToS3Reactor;
import prerna.aws.s3.S3FileRetrieverReactor;
import prerna.aws.s3.S3ListBucketsReactor;
import prerna.aws.s3.S3ListFilesReactor;
import prerna.comments.AddInsightCommentReactor;
import prerna.comments.DeleteInsightCommentReactor;
import prerna.comments.GetInsightCommentsReactor;
import prerna.comments.ModifyInsightCommentReactor;
import prerna.date.reactor.DateReactor;
import prerna.date.reactor.DayReactor;
import prerna.date.reactor.MonthReactor;
import prerna.date.reactor.WeekReactor;
import prerna.date.reactor.YearReactor;
import prerna.ds.TinkerFrame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.ds.py.PandasFrame;
import prerna.ds.r.RDataTable;
import prerna.ds.rdbms.AbstractRdbmsFrame;
import prerna.forms.UpdateFormReactor;
import prerna.io.connector.surveymonkey.SurveyMonkeyListSurveysReactor;
import prerna.poi.main.helper.excel.GetExcelFormReactor;
import prerna.query.querystruct.delete.DeleteReactor;
import prerna.query.querystruct.update.reactors.UpdateReactor;
import prerna.sablecc2.reactor.algorithms.CreateNLPVizReactor;
import prerna.sablecc2.reactor.algorithms.NLPInstanceCacheReactor;
import prerna.sablecc2.reactor.algorithms.NLSQueryHelperReactor;
import prerna.sablecc2.reactor.algorithms.NaturalLanguageSearchReactor;
import prerna.sablecc2.reactor.algorithms.RAlgReactor;
import prerna.sablecc2.reactor.algorithms.RatioReactor;
import prerna.sablecc2.reactor.algorithms.RunAnomalyReactor;
import prerna.sablecc2.reactor.algorithms.RunAssociatedLearningReactor;
import prerna.sablecc2.reactor.algorithms.RunClassificationReactor;
import prerna.sablecc2.reactor.algorithms.RunClusteringReactor;
import prerna.sablecc2.reactor.algorithms.RunLOFReactor;
import prerna.sablecc2.reactor.algorithms.RunMatrixRegressionReactor;
import prerna.sablecc2.reactor.algorithms.RunMultiClusteringReactor;
import prerna.sablecc2.reactor.algorithms.RunNumericalCorrelationReactor;
import prerna.sablecc2.reactor.algorithms.RunOutlierReactor;
import prerna.sablecc2.reactor.algorithms.RunSimilarityReactor;
import prerna.sablecc2.reactor.algorithms.UpdateNLPHistoryReactor;
import prerna.sablecc2.reactor.algorithms.dataquality.GetDQRulesReactor;
import prerna.sablecc2.reactor.algorithms.dataquality.RunDataQualityReactor;
import prerna.sablecc2.reactor.algorithms.xray.GetCSVSchemaReactor;
//import prerna.sablecc2.reactor.algorithms.xray.GetExternalSchemaReactor;
import prerna.sablecc2.reactor.algorithms.xray.GetLocalSchemaReactor;
import prerna.sablecc2.reactor.algorithms.xray.GetXLSchemaReactor;
import prerna.sablecc2.reactor.algorithms.xray.GetXrayConfigFileReactor;
import prerna.sablecc2.reactor.algorithms.xray.GetXrayConfigListReactor;
//import prerna.sablecc2.reactor.algorithms.xray.RunXRayReactor;
//import prerna.sablecc2.reactor.algorithms.xray.XrayMetamodelReactor;
import prerna.sablecc2.reactor.cluster.CleanUpDatabasesReactor;
import prerna.sablecc2.reactor.cluster.OpenDatabaseReactor;
import prerna.sablecc2.reactor.cluster.VersionReactor;
import prerna.sablecc2.reactor.database.AddDefaultInsightsReactor;
import prerna.sablecc2.reactor.database.DatabaseColumnUniqueReactor;
import prerna.sablecc2.reactor.database.metaeditor.GetOwlDescriptionsReactor;
import prerna.sablecc2.reactor.database.metaeditor.GetOwlDictionaryReactor;
import prerna.sablecc2.reactor.database.metaeditor.GetOwlLogicalNamesReactor;
import prerna.sablecc2.reactor.database.metaeditor.GetOwlMetamodelReactor;
import prerna.sablecc2.reactor.database.metaeditor.ReloadDatabaseOwlReactor;
import prerna.sablecc2.reactor.database.metaeditor.concepts.AddOwlConceptReactor;
import prerna.sablecc2.reactor.database.metaeditor.concepts.EditOwlConceptConceptualNameReactor;
import prerna.sablecc2.reactor.database.metaeditor.concepts.EditOwlConceptDataTypeReactor;
import prerna.sablecc2.reactor.database.metaeditor.concepts.RemoveOwlConceptReactor;
import prerna.sablecc2.reactor.database.metaeditor.meta.AddOwlDescriptionReactor;
import prerna.sablecc2.reactor.database.metaeditor.meta.AddOwlLogicalNamesReactor;
import prerna.sablecc2.reactor.database.metaeditor.meta.EditOwlDescriptionReactor;
import prerna.sablecc2.reactor.database.metaeditor.meta.EditOwlLogicalNamesReactor;
import prerna.sablecc2.reactor.database.metaeditor.meta.RemoveOwlDescriptionReactor;
import prerna.sablecc2.reactor.database.metaeditor.meta.RemoveOwlLogicalNamesReactor;
import prerna.sablecc2.reactor.database.metaeditor.properties.AddOwlPropertyReactor;
import prerna.sablecc2.reactor.database.metaeditor.properties.EditOwlPropertyConceptualNameReactor;
import prerna.sablecc2.reactor.database.metaeditor.properties.EditOwlPropertyDataTypeReactor;
import prerna.sablecc2.reactor.database.metaeditor.properties.RemoveOwlPropertyReactor;
import prerna.sablecc2.reactor.database.metaeditor.relationships.AddBulkOwlRelationshipsReactor;
import prerna.sablecc2.reactor.database.metaeditor.relationships.AddOwlRelationshipReactor;
import prerna.sablecc2.reactor.database.metaeditor.relationships.RemoveOwlRelationshipReactor;
import prerna.sablecc2.reactor.database.metaeditor.routines.FindDirectOwlRelationshipsReactor;
import prerna.sablecc2.reactor.database.metaeditor.routines.FindIndirectOwlRelationshipsReactor;
import prerna.sablecc2.reactor.database.metaeditor.routines.FindSemanticColumnOwlRelationshipsReactor;
import prerna.sablecc2.reactor.database.metaeditor.routines.FindSemanticInstanceOwlRelationshipsReactor;
import prerna.sablecc2.reactor.database.metaeditor.routines.PredictOwlDescriptionReactor;
import prerna.sablecc2.reactor.database.metaeditor.routines.PredictOwlLogicalNamesReactor;
import prerna.sablecc2.reactor.database.upload.CheckHeadersReactor;
import prerna.sablecc2.reactor.database.upload.ParseMetamodelReactor;
import prerna.sablecc2.reactor.database.upload.PredictDataTypesReactor;
import prerna.sablecc2.reactor.database.upload.PredictExcelDataTypesReactor;
import prerna.sablecc2.reactor.database.upload.PredictExcelRangeMetadataReactor;
import prerna.sablecc2.reactor.database.upload.PredictMetamodelReactor;
import prerna.sablecc2.reactor.database.upload.gremlin.external.CreateExternalDSEGraphDatabaseReactor;
import prerna.sablecc2.reactor.database.upload.gremlin.external.CreateExternalGraphDatabaseReactor;
import prerna.sablecc2.reactor.database.upload.gremlin.external.CreateJanusGraphDatabaseReactor;
import prerna.sablecc2.reactor.database.upload.gremlin.external.GetDSEGraphMetaModelReactor;
import prerna.sablecc2.reactor.database.upload.gremlin.external.GetDSEGraphPropertiesReactor;
import prerna.sablecc2.reactor.database.upload.gremlin.external.GetGraphMetaModelReactor;
import prerna.sablecc2.reactor.database.upload.gremlin.external.GetGraphPropertiesReactor;
import prerna.sablecc2.reactor.database.upload.gremlin.external.GetJanusGraphMetaModelReactor;
import prerna.sablecc2.reactor.database.upload.gremlin.external.GetJanusGraphPropertiesReactor;
import prerna.sablecc2.reactor.database.upload.gremlin.file.TinkerCsvUploadReactor;
import prerna.sablecc2.reactor.database.upload.rdbms.csv.RdbmsCsvUploadReactor;
import prerna.sablecc2.reactor.database.upload.rdbms.csv.RdbmsUploadTableReactor;
import prerna.sablecc2.reactor.database.upload.rdbms.excel.RdbmsLoaderSheetUploadReactor;
import prerna.sablecc2.reactor.database.upload.rdbms.excel.RdbmsUploadExcelDataReactor;
import prerna.sablecc2.reactor.database.upload.rdbms.external.ExternalJdbcSchemaReactor;
import prerna.sablecc2.reactor.database.upload.rdbms.external.ExternalJdbcTablesAndViewsReactor;
import prerna.sablecc2.reactor.database.upload.rdbms.external.RdbmsExternalUploadReactor;
import prerna.sablecc2.reactor.database.upload.rdf.RdfCsvUploadReactor;
import prerna.sablecc2.reactor.database.upload.rdf.RdfLoaderSheetUploadReactor;
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
import prerna.sablecc2.reactor.export.ToS3Reactor;
import prerna.sablecc2.reactor.export.ToTsvReactor;
import prerna.sablecc2.reactor.export.ToTxtReactor;
import prerna.sablecc2.reactor.expression.IfError;
import prerna.sablecc2.reactor.expression.OpAbsolute;
import prerna.sablecc2.reactor.expression.OpArrayLength;
import prerna.sablecc2.reactor.expression.OpAsString;
import prerna.sablecc2.reactor.expression.OpConcat;
import prerna.sablecc2.reactor.expression.OpContains;
import prerna.sablecc2.reactor.expression.OpIsEmpty;
import prerna.sablecc2.reactor.expression.OpIsObjectEmpty;
import prerna.sablecc2.reactor.expression.OpLarge;
import prerna.sablecc2.reactor.expression.OpLen;
import prerna.sablecc2.reactor.expression.OpList;
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
import prerna.sablecc2.reactor.federation.FederationBestMatches;
import prerna.sablecc2.reactor.federation.FederationBlend;
import prerna.sablecc2.reactor.federation.FuzzyMatchesReactor;
import prerna.sablecc2.reactor.federation.FuzzyMergeReactor;
import prerna.sablecc2.reactor.frame.CreateFrameReactor;
import prerna.sablecc2.reactor.frame.CurrentFrameReactor;
import prerna.sablecc2.reactor.frame.FrameHeaderExistsReactor;
import prerna.sablecc2.reactor.frame.FrameHeadersReactor;
import prerna.sablecc2.reactor.frame.FrameTypeReactor;
import prerna.sablecc2.reactor.frame.HasDuplicatesReactor;
import prerna.sablecc2.reactor.frame.SetCurrentFrameReactor;
import prerna.sablecc2.reactor.frame.convert.ConvertReactor;
import prerna.sablecc2.reactor.frame.filter.AddFrameFilterReactor;
import prerna.sablecc2.reactor.frame.filter.DeleteFrameFilterReactor;
import prerna.sablecc2.reactor.frame.filter.GetFrameFiltersReactor;
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
import prerna.sablecc2.reactor.frame.graph.r.ChangeGraphLayoutReactor;
import prerna.sablecc2.reactor.frame.graph.r.ClusterGraphReactor;
import prerna.sablecc2.reactor.frame.graph.r.NodeDetailsReactor;
import prerna.sablecc2.reactor.frame.py.GenerateFrameFromPyVariableReactor;
import prerna.sablecc2.reactor.frame.py.PyReactor;
import prerna.sablecc2.reactor.frame.py.PySourceReactor;
import prerna.sablecc2.reactor.frame.r.CancelRReactor;
import prerna.sablecc2.reactor.frame.r.GenerateFrameFromRVariableReactor;
import prerna.sablecc2.reactor.frame.r.GenerateH2FrameFromRVariableReactor;
import prerna.sablecc2.reactor.frame.r.RReactor;
import prerna.sablecc2.reactor.frame.r.SemanticBlendingReactor;
import prerna.sablecc2.reactor.frame.r.SemanticDescription;
import prerna.sablecc2.reactor.frame.r.util.RClearAllUserRservesReactor;
import prerna.sablecc2.reactor.frame.r.util.REnableUserRecoveryReactor;
import prerna.sablecc2.reactor.frame.r.util.RSourceReactor;
import prerna.sablecc2.reactor.imports.ImportReactor;
import prerna.sablecc2.reactor.imports.MergeReactor;
import prerna.sablecc2.reactor.insights.ClearInsightReactor;
import prerna.sablecc2.reactor.insights.CurrentVariablesReactor;
import prerna.sablecc2.reactor.insights.DropInsightReactor;
import prerna.sablecc2.reactor.insights.InsightHandleReactor;
import prerna.sablecc2.reactor.insights.LoadInsightReactor;
import prerna.sablecc2.reactor.insights.OpenEmptyInsightReactor;
import prerna.sablecc2.reactor.insights.OpenInsightReactor;
import prerna.sablecc2.reactor.insights.RetrieveInsightOrnamentReactor;
import prerna.sablecc2.reactor.insights.SetInsightOrnamentReactor;
import prerna.sablecc2.reactor.insights.copy.CopyInsightReactor;
import prerna.sablecc2.reactor.insights.dashboard.DashboardInsightConfigReactor;
import prerna.sablecc2.reactor.insights.dashboard.ReloadInsightReactor;
import prerna.sablecc2.reactor.insights.recipemanagement.GetCurrentRecipeReactor;
import prerna.sablecc2.reactor.insights.recipemanagement.InsightRecipeReactor;
import prerna.sablecc2.reactor.insights.recipemanagement.RetrieveInsightPipelineReactor;
import prerna.sablecc2.reactor.insights.save.DeleteInsightCacheReactor;
import prerna.sablecc2.reactor.insights.save.DeleteInsightReactor;
import prerna.sablecc2.reactor.insights.save.SaveInsightReactor;
import prerna.sablecc2.reactor.insights.save.SetInsightCacheableReactor;
import prerna.sablecc2.reactor.insights.save.SetInsightNameReactor;
import prerna.sablecc2.reactor.insights.save.UpdateInsightImageReactor;
import prerna.sablecc2.reactor.insights.save.UpdateInsightReactor;
import prerna.sablecc2.reactor.job.JobReactor;
import prerna.sablecc2.reactor.masterdatabase.AllConceptualNamesReactor;
import prerna.sablecc2.reactor.masterdatabase.CLPModelReactor;
import prerna.sablecc2.reactor.masterdatabase.GetConceptPropertiesReactor;
import prerna.sablecc2.reactor.masterdatabase.GetDatabaseConceptsReactor;
import prerna.sablecc2.reactor.masterdatabase.GetDatabaseConnectionsReactor;
import prerna.sablecc2.reactor.masterdatabase.GetDatabaseListReactor;
import prerna.sablecc2.reactor.masterdatabase.GetDatabaseMetamodelReactor;
import prerna.sablecc2.reactor.masterdatabase.GetDatabaseTableStructureReactor;
import prerna.sablecc2.reactor.masterdatabase.GetPhysicalToLogicalMapping;
import prerna.sablecc2.reactor.masterdatabase.GetPhysicalToPhysicalMapping;
import prerna.sablecc2.reactor.masterdatabase.GetSpecificConceptPropertiesReactor;
import prerna.sablecc2.reactor.masterdatabase.GetTraversalOptionsReactor;
import prerna.sablecc2.reactor.masterdatabase.QueryTranslatorReactor;
import prerna.sablecc2.reactor.masterdatabase.SyncDatabaseWithLocalMasterReactor;
import prerna.sablecc2.reactor.panel.AddPanelConfigReactor;
import prerna.sablecc2.reactor.panel.AddPanelIfAbsentReactor;
import prerna.sablecc2.reactor.panel.AddPanelReactor;
import prerna.sablecc2.reactor.panel.CachedPanelCloneReactor;
import prerna.sablecc2.reactor.panel.CachedPanelReactor;
import prerna.sablecc2.reactor.panel.CloneReactor;
import prerna.sablecc2.reactor.panel.ClosePanelReactor;
import prerna.sablecc2.reactor.panel.GetPanelIdReactor;
import prerna.sablecc2.reactor.panel.InsightPanelIds;
import prerna.sablecc2.reactor.panel.PanelExistsReactor;
import prerna.sablecc2.reactor.panel.PanelReactor;
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
import prerna.sablecc2.reactor.panel.rules.GetPanelColorByValueReactor;
import prerna.sablecc2.reactor.panel.rules.RemovePanelColorByValueReactor;
import prerna.sablecc2.reactor.panel.rules.RetrievePanelColorByValueReactor;
import prerna.sablecc2.reactor.panel.sort.AddPanelSortReactor;
import prerna.sablecc2.reactor.panel.sort.SetPanelSortReactor;
import prerna.sablecc2.reactor.panel.sort.UnsortPanelReactor;
import prerna.sablecc2.reactor.planner.GraphPlanReactor;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.reactor.qs.DistinctReactor;
import prerna.sablecc2.reactor.qs.ExecQueryReactor;
import prerna.sablecc2.reactor.qs.GroupReactor;
import prerna.sablecc2.reactor.qs.ImplicitFilterOverrideReactor;
import prerna.sablecc2.reactor.qs.InsertReactor;
import prerna.sablecc2.reactor.qs.JoinReactor;
import prerna.sablecc2.reactor.qs.LimitReactor;
import prerna.sablecc2.reactor.qs.OffsetReactor;
import prerna.sablecc2.reactor.qs.QueryAllReactor;
import prerna.sablecc2.reactor.qs.QueryReactor;
import prerna.sablecc2.reactor.qs.SortReactor;
import prerna.sablecc2.reactor.qs.WithReactor;
import prerna.sablecc2.reactor.qs.filter.HavingReactor;
import prerna.sablecc2.reactor.qs.filter.RegexFilterReactor;
import prerna.sablecc2.reactor.qs.selectors.AverageReactor;
import prerna.sablecc2.reactor.qs.selectors.CountReactor;
import prerna.sablecc2.reactor.qs.selectors.GenericSelectorFunctionReactor;
import prerna.sablecc2.reactor.qs.selectors.GroupConcatReactor;
import prerna.sablecc2.reactor.qs.selectors.LowerReactor;
import prerna.sablecc2.reactor.qs.selectors.MaxReactor;
import prerna.sablecc2.reactor.qs.selectors.MedianReactor;
import prerna.sablecc2.reactor.qs.selectors.MinReactor;
import prerna.sablecc2.reactor.qs.selectors.PColReactor;
import prerna.sablecc2.reactor.qs.selectors.PSelectReactor;
import prerna.sablecc2.reactor.qs.selectors.QuerySelectorExpressionAssimilator;
import prerna.sablecc2.reactor.qs.selectors.SelectReactor;
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
import prerna.sablecc2.reactor.qs.source.FileReadReactor;
import prerna.sablecc2.reactor.qs.source.FrameReactor;
import prerna.sablecc2.reactor.qs.source.GoogleFileRetrieverReactor;
import prerna.sablecc2.reactor.qs.source.GoogleListFilesReactor;
import prerna.sablecc2.reactor.qs.source.JdbcSourceReactor;
import prerna.sablecc2.reactor.qs.source.OneDriveFileRetrieverReactor;
import prerna.sablecc2.reactor.qs.source.OneDriveListFilesReactor;
import prerna.sablecc2.reactor.qs.source.SharePointDriveSelectorReactor;
import prerna.sablecc2.reactor.qs.source.SharePointFileRetrieverReactor;
import prerna.sablecc2.reactor.qs.source.SharePointListFilesReactor;
import prerna.sablecc2.reactor.qs.source.SharePointSiteSelectorReactor;
import prerna.sablecc2.reactor.qs.source.SharePointWebDavPullReactor;
import prerna.sablecc2.reactor.qs.source.URLSourceReactor;
import prerna.sablecc2.reactor.runtime.JavaReactor;
import prerna.sablecc2.reactor.scheduler.ListAllJobsReactor;
import prerna.sablecc2.reactor.scheduler.PauseJobTriggerReactor;
import prerna.sablecc2.reactor.scheduler.ResumeJobTriggerReactor;
import prerna.sablecc2.reactor.scheduler.ScheduleJobReactor;
import prerna.sablecc2.reactor.task.AutoTaskOptionsReactor;
import prerna.sablecc2.reactor.task.CollectMetaReactor;
import prerna.sablecc2.reactor.task.FormatReactor;
import prerna.sablecc2.reactor.task.RefreshPanelTaskReactor;
import prerna.sablecc2.reactor.task.RemoveTaskReactor;
import prerna.sablecc2.reactor.task.ResetTaskReactor;
import prerna.sablecc2.reactor.task.TaskOptionsReactor;
import prerna.sablecc2.reactor.task.TaskReactor;
import prerna.sablecc2.reactor.task.lambda.map.MapLambdaReactor;
import prerna.sablecc2.reactor.task.lambda.map.function.ApplyFormattingReactor;
import prerna.sablecc2.reactor.task.modifiers.CodeLambdaReactor;
import prerna.sablecc2.reactor.task.modifiers.FilterLambdaReactor;
import prerna.sablecc2.reactor.task.modifiers.FlatMapLambdaReactor;
import prerna.sablecc2.reactor.task.modifiers.ToNumericTypeReactor;
import prerna.sablecc2.reactor.task.modifiers.ToUrlTypeReactor;
import prerna.sablecc2.reactor.task.modifiers.TransposeRowsReactor;
import prerna.sablecc2.reactor.tax.RetrieveValue;
import prerna.sablecc2.reactor.tax.StoreValue;
import prerna.sablecc2.reactor.test.LSASpaceColumnLearnedReactor;
import prerna.sablecc2.reactor.test.RunLSILearnedReactor;
import prerna.sablecc2.reactor.utils.AddOperationAliasReactor;
import prerna.sablecc2.reactor.utils.BDelReactor;
import prerna.sablecc2.reactor.utils.BQReactor;
import prerna.sablecc2.reactor.utils.BackupDatabaseReactor;
import prerna.sablecc2.reactor.utils.BaddReactor;
import prerna.sablecc2.reactor.utils.BupdReactor;
import prerna.sablecc2.reactor.utils.CheckRPackagesReactor;
import prerna.sablecc2.reactor.utils.CheckRecommendOptimizationReactor;
import prerna.sablecc2.reactor.utils.DatabaseProfileReactor;
import prerna.sablecc2.reactor.utils.DeleteDatabaseReactor;
import prerna.sablecc2.reactor.utils.ExportDatabaseReactor;
import prerna.sablecc2.reactor.utils.ExternalDatabaseProfileReactor;
import prerna.sablecc2.reactor.utils.GetNumTableReactor;
import prerna.sablecc2.reactor.utils.GetRequestReactor;
import prerna.sablecc2.reactor.utils.GetTableHeader;
import prerna.sablecc2.reactor.utils.GetUserInfoReactor;
import prerna.sablecc2.reactor.utils.HelpReactor;
import prerna.sablecc2.reactor.utils.ImageCaptureReactor;
import prerna.sablecc2.reactor.utils.PostRequestReactor;
import prerna.sablecc2.reactor.utils.RemoveVariableReactor;
import prerna.sablecc2.reactor.utils.SendEmailReactor;
import prerna.sablecc2.reactor.utils.VariableExistsReactor;
import prerna.sablecc2.reactor.workflow.GetInsightDatasourcesReactor;
import prerna.sablecc2.reactor.workflow.GetOptimizedRecipeReactor;
import prerna.sablecc2.reactor.workflow.ModifyInsightDatasourceReactor;
import prerna.sablecc2.reactor.workspace.DeleteUserAssetReactor;
import prerna.sablecc2.reactor.workspace.MoveUserAssetReactor;
import prerna.sablecc2.reactor.workspace.NewDirReactor;
import prerna.sablecc2.reactor.workspace.UploadUserFileReactor;
import prerna.sablecc2.reactor.workspace.UserDirReactor;
import prerna.solr.reactor.DatabaseInfoReactor;
import prerna.solr.reactor.DatabaseUsersReactor;
import prerna.solr.reactor.GetInsightsReactor;
import prerna.solr.reactor.MyDatabasesReactor;
import prerna.util.Constants;
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
import prerna.util.git.reactors.LoginReactor;
import prerna.util.git.reactors.RemoveAppCollaborator;
import prerna.util.git.reactors.SearchAppCollaborator;
import prerna.util.git.reactors.SyncApp;
import prerna.util.git.reactors.SyncAppFiles;
import prerna.util.git.reactors.SyncAppFilesO;
import prerna.util.git.reactors.SyncAppOReactor;
import prerna.util.usertracking.reactors.ExtractDatabaseMetaReactor;
import prerna.util.usertracking.reactors.UpdateQueryDataReactor;
import prerna.util.usertracking.reactors.UpdateSemanticDataReactor;
import prerna.util.usertracking.reactors.WidgetTReactor;
import prerna.util.usertracking.reactors.recommendations.DatabaseRecommendationsReactor;
import prerna.util.usertracking.reactors.recommendations.GetDatabasesByDescriptionReactor;
import prerna.util.usertracking.reactors.recommendations.VizRecommendationsReactor;

public class ReactorFactory {

	private static final Logger logger = LogManager.getLogger(ReactorFactory.class);
	
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
	
	public static Map <String, Class> reactors = new HashMap<String, Class>();
	
	public static List <String> nmList = new ArrayList<String>();
	public static List <Class> classList = new ArrayList<Class>();
	public static boolean write = true;
	
	static {
		reactorHash = new HashMap<String, Class>();
		createReactorHash(reactorHash);
		// build expression hash
		expressionHash = new HashMap<String, Class>();
		populateExpressionSet(expressionHash);
		// populate the frame specific hashes
		rFrameHash = new HashMap<String, Class>();
		//populateRFrameHash(rFrameHash);
		pandasFrameHash = new HashMap<String, Class>();
		//populatePandasFrameHash(pandasFrameHash);
		h2FrameHash = new HashMap<String, Class>();
		//populateH2FrameHash(h2FrameHash);
		tinkerFrameHash = new HashMap<String, Class>();
		//populateTinkerFrameHash(tinkerFrameHash);
		nativeFrameHash = new HashMap<String, Class>();
		//populateNativeFrameHash(nativeFrameHash);

		
		String additionalReactorsPath = "";
		try {
			additionalReactorsPath = DIHelper.getInstance().getProperty(Constants.ADDITIONAL_REACTORS);
			if(additionalReactorsPath != null) {
				logger.info("Loading additional reactors from file");
				File f = new File(additionalReactorsPath);
				if(f.exists()) {
					loadAdditionalReactor(f);
				}
			}
		} catch(Exception e) {
			// ignore
			// this would only be null during testing
			// and DIHelper isn't loaded
			// hopefully you dont have anything in a prop file you care about
			// or update the var directly
		}
		
		// load it through the inspect
		List<String> packagesToLoad = new ArrayList<>();
		packagesToLoad.add("prerna");
		String additionalPackages = null;
		try {
			additionalPackages = DIHelper.getInstance().getProperty(Constants.ADDITIONAL_REACTOR_PACKAGES);
			if(additionalPackages != null && !(additionalPackages=additionalPackages.trim()).isEmpty()) {
				logger.info("Loading additional reactors from packages [" + additionalPackages + "]");
				String[] packagesArr = additionalPackages.split(",");
				for(String thisPackage : packagesArr) {
					if(!(thisPackage=thisPackage.trim()).isEmpty()) {
						packagesToLoad.add(thisPackage);
					}
				}
			}
		} catch(Exception e) {
			// ignore
			// this would only be null during testing
			// and DIHelper isn't loaded
			// hopefully you dont have anything in a prop file you care about
			// or update the var directly
		}
		
		loadFromCP(packagesToLoad.toArray(new String[] {}));
	}
	
	public static void main(String [] args)
	{
		// print the inconsistencies
		
		printInconsistency("Generic", reactorHash);
		printInconsistency("h2", h2FrameHash);
		printInconsistency("R", rFrameHash);
		printInconsistency("Pandas", pandasFrameHash);
	}
	
	public static void printInconsistency(String mapname, Map map)
	{
		
		Iterator keys = map.keySet().iterator();
		
		System.out.println(mapname);
		System.out.println("-----------");
		
		while(keys.hasNext())
		{
			String key = (String)keys.next();
			Class value = (Class)map.get(key);
			
			String name = value.getSimpleName();

			name = name.replaceAll("Reactor","");
			if(!name.equals(key))
				System.out.println(key + " <<>> " + name);
			
		}
		System.out.println("-----------");
		
	}
	
	/**
	 * Load reactors based on the class path
	 * @param packages
	 */
	private static void loadFromCP(String... packages) {
		try {
			ScanResult sr = new ClassGraph().whitelistPackages(packages).scan();
			ClassInfoList classes = sr.getClassesImplementing("prerna.sablecc2.reactor.IReactor");
			
			for(int classIndex = 0;classIndex < classes.size();classIndex++) {
				String name = classes.get(classIndex).getSimpleName();
				String packageName = classes.get(classIndex).getPackageName();
				Class actualClass = classes.get(classIndex).loadClass();

				// ignore abstract
				if(!Modifier.isAbstract( actualClass.getModifiers() )) {
					String [] packagePaths = packageName.split("\\.");
					//System.out.println("Package name " + packageName);
					packageName = packagePaths[packagePaths.length - 1];
					boolean frame = false;
					if (packagePaths.length > 2) {
						frame = packagePaths[packagePaths.length - 2].equalsIgnoreCase("frame");
					}
					// we will allow for 1 more level 
					// i.e. things of the form so *.frame.r.?
					if(!frame && packagePaths.length > 3) {
						packageName = packagePaths[packagePaths.length - 2];
						frame = packagePaths[packagePaths.length - 3].equalsIgnoreCase("frame");
					}
					String reactorName = name;
					final String REACTOR_KEY = "REACTOR";
					if(reactorName.toUpperCase().endsWith(REACTOR_KEY)) {
						reactorName = reactorName.substring(0, reactorName.length()-REACTOR_KEY.length());
					}
					
					if(frame) {
						if(packageName.equalsIgnoreCase("rdbms")) {
							h2FrameHash.put(reactorName, actualClass);
						} else if(packageName.equalsIgnoreCase("r")) {
							rFrameHash.put(reactorName, actualClass);
						} else if(packageName.equalsIgnoreCase("py")) {
							pandasFrameHash.put(reactorName, actualClass);
						} else if(packageName.equalsIgnoreCase("tinker") || packageName.equalsIgnoreCase("graph")) {
							tinkerFrameHash.put(reactorName, actualClass);
						} else {// nullify the package name
							packageName = null;
							// general reactor
							// example - frame filters
							reactorHash.put(reactorName, actualClass);
						}
						if(packageName != null) {
							reactorName = packageName + "_" + reactorName;
						}
					} else {
						reactorHash.put(reactorName, actualClass);
					}
					nmList.add(reactorName);
					classList.add(actualClass);
					
					reactors.put(reactorName.toUpperCase(), actualClass);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
/*	public static void writeReacFile()
	{
		if(write)
		{
			String reacFileName = "c:/users/pkapaleeswaran/workspacej3/temp/reactornames.txt";
			System.err.println("Writing file.. ");
			try {
				PrintWriter br = new PrintWriter(new OutputStreamWriter(new FileOutputStream(reacFileName)));
				Iterator rIt = reactors.keySet().iterator();
				while(rIt.hasNext())
					br.write(rIt.next().toString() + "\n");
				{
				}
				br.flush();
				br.close();
				write = false;
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
*/	
	// populates the frame agnostic reactors used by pixel
	private static void createReactorHash(Map<String, Class> reactorHash) {
		// used to generate the base Job for the pksl commands being executed
		reactorHash.put("Job", JobReactor.class); // defines the job

		// Import Reactors
		// takes in a query struct and imports data to a new frame
		reactorHash.put("Import", ImportReactor.class);
		// takes in a query struct and merges data to an existing frame
		reactorHash.put("Merge", MergeReactor.class);

		// Utility Reactors
		reactorHash.put("AddOperationAlias", AddOperationAliasReactor.class);
		reactorHash.put("VariableExists", VariableExistsReactor.class);
		reactorHash.put("RemoveVariable", RemoveVariableReactor.class);
		reactorHash.put("SendEmail", SendEmailReactor.class);
		reactorHash.put("BackupDatabase", BackupDatabaseReactor.class);
		reactorHash.put("ExportDatabase", ExportDatabaseReactor.class);
		reactorHash.put("DeleteDatabase", DeleteDatabaseReactor.class);
		reactorHash.put("ImageCapture", ImageCaptureReactor.class);
		reactorHash.put("Help", HelpReactor.class);
		reactorHash.put("help", HelpReactor.class);
		reactorHash.put("DatabaseProfile", DatabaseProfileReactor.class);
		reactorHash.put("DatabaseColumnUnique", DatabaseColumnUniqueReactor.class);
		reactorHash.put("ExternalDatabaseProfile", ExternalDatabaseProfileReactor.class);
		reactorHash.put("GetRequest", GetRequestReactor.class);
		reactorHash.put("PostRequest", PostRequestReactor.class);
		reactorHash.put("CheckRPackages", CheckRPackagesReactor.class);
		reactorHash.put("CheckRecommendOptimization", CheckRecommendOptimizationReactor.class);
		reactorHash.put("PredictExcelRangeMetadata", PredictExcelRangeMetadataReactor.class);
		reactorHash.put("DeleteInsightCache", DeleteInsightCacheReactor.class);
		reactorHash.put("WidgetT", WidgetTReactor.class);
		reactorHash.put("GetUserInfo", GetUserInfoReactor.class);
		
		// Database uploading utils
		reactorHash.put("CheckHeaders", CheckHeadersReactor.class);
		reactorHash.put("PredictDataTypes", PredictDataTypesReactor.class);
		reactorHash.put("PredictExcelDataTypes", PredictExcelDataTypesReactor.class);
		reactorHash.put("PredictMetamodel", PredictMetamodelReactor.class);
		reactorHash.put("ParseMetamodel", ParseMetamodelReactor.class);
		reactorHash.put("ExtractAppMeta", ExtractDatabaseMetaReactor.class);
		reactorHash.put("NLPInstanceCache", NLPInstanceCacheReactor.class);
		
		// Excel Data validation
		reactorHash.put("GetExcelForm", GetExcelFormReactor.class);
		
		// Database Uploading
		reactorHash.put("ExternalJdbcSchema", ExternalJdbcSchemaReactor.class);
		reactorHash.put("ExternalJdbcTablesAndViews", ExternalJdbcTablesAndViewsReactor.class);
//		reactorHash.put("GenerateEmptyApp", GenerateEmptyAppReactor.class);
		reactorHash.put("RdbmsUploadTableData", RdbmsUploadTableReactor.class);
		reactorHash.put("RdbmsUploadExcelData", RdbmsUploadExcelDataReactor.class);
		reactorHash.put("RdbmsExternalUpload", RdbmsExternalUploadReactor.class);
		reactorHash.put("RdbmsCsvUpload", RdbmsCsvUploadReactor.class);
		reactorHash.put("RdbmsLoaderSheetUpload", RdbmsLoaderSheetUploadReactor.class);
		reactorHash.put("RdfCsvUpload", RdfCsvUploadReactor.class);
		reactorHash.put("RdfLoaderSheetUpload", RdfLoaderSheetUploadReactor.class);
		reactorHash.put("TinkerCsvUpload", TinkerCsvUploadReactor.class);
//		reactorHash.put("CatalogDescriptionGenerator", CatalogDescriptionGeneratorReactor.class);
//		reactorHash.put("SimilarCatalog", SimilarCatalogReactor.class);
//		reactorHash.put("CatalogSearch", CatalogSearchReactor.class);
			
		// external graph engine
		reactorHash.put("GetGraphProperties", GetGraphPropertiesReactor.class);
		reactorHash.put("GetGraphMetaModel", GetGraphMetaModelReactor.class);
		reactorHash.put("CreateExternalGraphDatabase", CreateExternalGraphDatabaseReactor.class);
		// datastax graph reactors
		reactorHash.put("GetDSEGraphProperties", GetDSEGraphPropertiesReactor.class);
		reactorHash.put("GetDSEGraphMetaModel", GetDSEGraphMetaModelReactor.class);
		reactorHash.put("CreateExternalDSEGraphDatabase", CreateExternalDSEGraphDatabaseReactor.class);
		// janus graph reactors
		reactorHash.put("GetJanusGraphProperties", GetJanusGraphPropertiesReactor.class);
		reactorHash.put("GetJanusGraphMetaModel", GetJanusGraphMetaModelReactor.class);
		reactorHash.put("CreateJanusGraphDatabase", CreateJanusGraphDatabaseReactor.class);
		
		// Query Struct Reactors
		// builds the select portion of the QS
		reactorHash.put("With", WithReactor.class);
		reactorHash.put("Select", SelectReactor.class);
		reactorHash.put("SelectTable", SelectTableReactor.class);
		reactorHash.put("PSelect", PSelectReactor.class);
		reactorHash.put("PCol", PColReactor.class);
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
		reactorHash.put("Lower", LowerReactor.class);
		reactorHash.put("Group", GroupReactor.class);
		reactorHash.put("GroupBy", GroupReactor.class);
		reactorHash.put("Sort", SortReactor.class);
		reactorHash.put("Order", SortReactor.class);
		reactorHash.put("Limit", LimitReactor.class);
		reactorHash.put("Offset", OffsetReactor.class);
		reactorHash.put("Join", JoinReactor.class);
		reactorHash.put("Filter", FilterReactor.class);
		reactorHash.put("RegexFilter", RegexFilterReactor.class);
		reactorHash.put("Having", HavingReactor.class);
		reactorHash.put("Query", QueryReactor.class);
		reactorHash.put("Distinct", DistinctReactor.class);
		reactorHash.put("ImplicitFilterOverride", ImplicitFilterOverrideReactor.class);
		reactorHash.put("QueryAll", QueryAllReactor.class);

		// modifications to database
		reactorHash.put("Insert", InsertReactor.class);
		reactorHash.put("Delete", DeleteReactor.class);
		reactorHash.put("Update", UpdateReactor.class);
		reactorHash.put("ExecQuery", ExecQueryReactor.class);
		
		// Data Source Reactors
		// specifies that our pixel operations after this point are dealing with the specified database
		reactorHash.put("Database", DatabaseReactor.class);
		reactorHash.put("AuditDatabase", AuditDatabaseReactor.class);
		reactorHash.put("API", APIReactor.class);
		reactorHash.put("FileRead", FileReadReactor.class);
		reactorHash.put("JdbcSource", JdbcSourceReactor.class);
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
		//S3
		reactorHash.put("S3FileRetriever", S3FileRetrieverReactor.class);
		reactorHash.put("ToS3", ToS3Reactor.class);
		reactorHash.put("PushAssetToS3", PushAssetToS3Reactor.class);
		reactorHash.put("S3ListBuckets", S3ListBucketsReactor.class);
		reactorHash.put("S3ListFiles", S3ListFilesReactor.class);

		// share point
		reactorHash.put("SharePointListFiles", SharePointListFilesReactor.class);
		reactorHash.put("SharePointFileRetriever", SharePointFileRetrieverReactor.class);
		reactorHash.put("SharePointSiteSelector", SharePointSiteSelectorReactor.class);
		reactorHash.put("SharePointDriveSelector", SharePointDriveSelectorReactor.class);
		reactorHash.put("SharePointWebDavPull", SharePointWebDavPullReactor.class);
		// survey monkey
		reactorHash.put("SurveyMonkeyListSurveys", SurveyMonkeyListSurveysReactor.class);
		reactorHash.put("NaturalLanguageSearch", NaturalLanguageSearchReactor.class);
		
		// specifies that our pixel operations after this point are dealing with the specified frame
		reactorHash.put("Frame", FrameReactor.class);
		reactorHash.put("CreateFrame", CreateFrameReactor.class);
		reactorHash.put("FrameType", FrameTypeReactor.class);
		reactorHash.put("Convert", ConvertReactor.class);
		reactorHash.put("GenerateFrameFromRVariable", GenerateFrameFromRVariableReactor.class);
		reactorHash.put("GenerateFrameFromPyVariable", GenerateFrameFromPyVariableReactor.class);
		reactorHash.put("GenerateH2FrameFromRVariable", GenerateH2FrameFromRVariableReactor.class);
		//reactorHash.put("SynchronizeToR", SynchronizeToRReactor.class);

		// Task Reactors
		reactorHash.put("Iterate", IterateReactor.class);
		reactorHash.put("Task", TaskReactor.class); // defines the task
		reactorHash.put("ResetTask", ResetTaskReactor.class); // reset a task
		reactorHash.put("ResetAll", RefreshPanelTaskReactor.class); // reset all panel tasks
		reactorHash.put("RemoveTask", RemoveTaskReactor.class);
		reactorHash.put("Collect", CollectReactor.class); // collect from task
		reactorHash.put("CollectAll", CollectAllReactor.class); // collect from task
		reactorHash.put("CollectGraph", CollectGraphReactor.class); // collect from task
		reactorHash.put("GrabScalarElement", GrabScalarElementReactor.class);
		reactorHash.put("AsTask", AsTaskReactor.class);
		reactorHash.put("EmptyData", EmptyDataReactor.class);
		reactorHash.put("CollectMeta", CollectMetaReactor.class); // collect meta from task
		reactorHash.put("Format", FormatReactor.class); // set formats
		reactorHash.put("TaskOptions", TaskOptionsReactor.class); // set options
		reactorHash.put("AutoTaskOptions", AutoTaskOptionsReactor.class);
		reactorHash.put("ToCsv", ToCsvReactor.class); // take any task and output to a file
		reactorHash.put("ToTsv", ToTsvReactor.class); // take any task and output to a file
		reactorHash.put("ToTxt", ToTxtReactor.class); // take any task and output to a file
		reactorHash.put("ToExcel", ToExcelReactor.class); // take any task and output to a file
		reactorHash.put("ToDatabase", ToDatabaseReactor.class);
		reactorHash.put("ToLoaderSheet", ToLoaderSheetReactor.class);
		
		// Task Operations
		reactorHash.put("CodeLambda", CodeLambdaReactor.class);
		reactorHash.put("FlatMapLambda", FlatMapLambdaReactor.class);
		reactorHash.put("MapLambda", MapLambdaReactor.class);
		reactorHash.put("FilterLambda", FilterLambdaReactor.class);
		reactorHash.put("ToNumericType", ToNumericTypeReactor.class);
		reactorHash.put("ToUrlType", ToUrlTypeReactor.class);
		reactorHash.put("TransposeRows", TransposeRowsReactor.class);
		reactorHash.put("ApplyFormatting", ApplyFormattingReactor.class);

		// Local Master Reactors
		// TODO: remove ones no longer used
		reactorHash.put("GetDatabaseList", GetDatabaseListReactor.class);
		reactorHash.put("GetDatabaseConcepts", GetDatabaseConceptsReactor.class);
		reactorHash.put("GetTraversalOptions", GetTraversalOptionsReactor.class);
		reactorHash.put("GetDatabaseMetamodel", GetDatabaseMetamodelReactor.class);
		reactorHash.put("GetConceptProperties", GetConceptPropertiesReactor.class);
		
		// NEW FEDERATE
		reactorHash.put("GetDatabaseConnections", GetDatabaseConnectionsReactor.class);
		reactorHash.put("GetDatabaseTableStructure", GetDatabaseTableStructureReactor.class);
		reactorHash.put("GetSpecificConceptProperties", GetSpecificConceptPropertiesReactor.class);
		reactorHash.put("FuzzyMatches", FuzzyMatchesReactor.class);
		reactorHash.put("FuzzyMerge", FuzzyMergeReactor.class);

		// depcreated
		reactorHash.put("FederationBlend", FederationBlend.class);
		reactorHash.put("FederationBestMatches", FederationBestMatches.class);
		
		// app meta and local master utilities
		reactorHash.put("ReloadDatabaseOwl", ReloadDatabaseOwlReactor.class);
		reactorHash.put("GetOwlMetamodel", GetOwlMetamodelReactor.class);
		reactorHash.put("GetOwlDictionary", GetOwlDictionaryReactor.class);
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
		reactorHash.put("PredictOwlDescription", PredictOwlDescriptionReactor.class);
		// routines to predict owl information
		reactorHash.put("FindDirectOwlRelationships", FindDirectOwlRelationshipsReactor.class);
		reactorHash.put("FindIndirectOwlRelationships", FindIndirectOwlRelationshipsReactor.class);
		reactorHash.put("FindSemanticColumnOwlRelationships", FindSemanticColumnOwlRelationshipsReactor.class);
		reactorHash.put("FindSemanticInstanceOwlRelationships", FindSemanticInstanceOwlRelationshipsReactor.class);
		
		reactorHash.put("SyncDatabaseWithLocalMaster", SyncDatabaseWithLocalMasterReactor.class);
		reactorHash.put("QueryTranslator", QueryTranslatorReactor.class);
		reactorHash.put("AllConceptualNames", AllConceptualNamesReactor.class);
		reactorHash.put("CLPModel", CLPModelReactor.class);
		// logical name operations
//		reactorHash.put("AddLogicalName", AddLogicalNameReactor.class);
//		reactorHash.put("GetLogicalNames", GetLogicalNamesReactor.class);
//		reactorHash.put("RemoveLogicalNames", RemoveLogicalNamesReactor.class);
//		// concept description metadata 
//		reactorHash.put("AddMetaDescription", AddMetaDescriptionReactor.class);
//		reactorHash.put("GetMetaDescription", GetMetaDescriptionReactor.class);
//		// concept tag metadata
//		reactorHash.put("AddMetaTags", AddMetaTagsReactor.class);
//		reactorHash.put("GetMetaTags", GetMetaTagsReactor.class);
//		reactorHash.put("DeleteMetaTags", DeleteMetaTagsReactor.class);
		
		// Panel Reactors
		reactorHash.put("InsightPanelIds", InsightPanelIds.class);
		reactorHash.put("Panel", PanelReactor.class);
		reactorHash.put("CachedPanel", CachedPanelReactor.class);
		reactorHash.put("CachedPanelClone", CachedPanelCloneReactor.class);
		reactorHash.put("AddPanel", AddPanelReactor.class);
		reactorHash.put("AddPanelIfAbsent", AddPanelIfAbsentReactor.class);
		reactorHash.put("GetPanelId", GetPanelIdReactor.class);
		reactorHash.put("ClosePanel", ClosePanelReactor.class);
		reactorHash.put("PanelExists", PanelExistsReactor.class);
		reactorHash.put("Clone", CloneReactor.class);
		reactorHash.put("SetPanelLabel", SetPanelLabelReactor.class);
		reactorHash.put("SetPanelView", SetPanelViewReactor.class);
		// panel filters
		reactorHash.put("AddPanelFilter", AddPanelFilterReactor.class);
		reactorHash.put("SetPanelFilter", SetPanelFilterReactor.class);
		reactorHash.put("UnfilterPanel", UnfilterPanelReactor.class);
		// panel sort
		reactorHash.put("AddPanelSort", AddPanelSortReactor.class);
		reactorHash.put("SetPanelSort", SetPanelSortReactor.class);
		reactorHash.put("RemovePanelSort", UnsortPanelReactor.class);
		reactorHash.put("UnsortPanel", UnsortPanelReactor.class);
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
		// panel color by value
		reactorHash.put("AddPanelColorByValue", AddPanelColorByValueReactor.class);
		reactorHash.put("RetrievePanelColorByValue", RetrievePanelColorByValueReactor.class);
		reactorHash.put("RemovePanelColorByValue", RemovePanelColorByValueReactor.class);
		reactorHash.put("GetPanelColorByValue", GetPanelColorByValueReactor.class);
		
		// new tab in browser
		reactorHash.put("OpenTab", OpenTabReactor.class);

		// Insight Reactors
		reactorHash.put("InsightRecipe", InsightRecipeReactor.class);
		reactorHash.put("CurrentVariables", CurrentVariablesReactor.class);
		reactorHash.put("OpenInsight", OpenInsightReactor.class);
		reactorHash.put("LoadInsight", LoadInsightReactor.class);
		reactorHash.put("ReloadInsight", ReloadInsightReactor.class);
		reactorHash.put("CopyInsight", CopyInsightReactor.class);
		reactorHash.put("OpenEmptyInsight", OpenEmptyInsightReactor.class);
		reactorHash.put("DropInsight", DropInsightReactor.class);
		reactorHash.put("ClearInsight", ClearInsightReactor.class);
		reactorHash.put("InsightHandle", InsightHandleReactor.class);
		reactorHash.put("SetInsightOrnament", SetInsightOrnamentReactor.class);
		reactorHash.put("RetrieveInsightOrnament", RetrieveInsightOrnamentReactor.class);
		reactorHash.put("UpdateInsightImage", UpdateInsightImageReactor.class);
		reactorHash.put("GetCurrentRecipe", GetCurrentRecipeReactor.class);
		reactorHash.put("RetrieveInsightPipeline", RetrieveInsightPipelineReactor.class);
		
		// Save Reactors
		reactorHash.put("SaveInsight", SaveInsightReactor.class);
		reactorHash.put("UpdateInsight", UpdateInsightReactor.class);
		reactorHash.put("DeleteInsight", DeleteInsightReactor.class);
		reactorHash.put("SetInsightName", SetInsightNameReactor.class);
		reactorHash.put("SetInsightCacheable", SetInsightCacheableReactor.class);

		// Dashboard Reactors
		reactorHash.put("DashboardInsightConfig", DashboardInsightConfigReactor.class);

		// General Frame Reactors
		reactorHash.put("FrameHeaders", FrameHeadersReactor.class);
		reactorHash.put("FrameHeaderExists", FrameHeaderExistsReactor.class);
		reactorHash.put("AddFrameFilter", AddFrameFilterReactor.class);
		reactorHash.put("GetFrameFilters", GetFrameFiltersReactor.class);
		reactorHash.put("SetFrameFilter", SetFrameFilterReactor.class);
		reactorHash.put("RemoveFrameFilter", RemoveFrameFilterReactor.class);
		reactorHash.put("ReplaceFrameFilter", ReplaceFrameFilterReactor.class);
		reactorHash.put("DeleteFrameFilter", DeleteFrameFilterReactor.class);
		reactorHash.put("UnfilterFrame", UnfilterFrameReactor.class);
		reactorHash.put("HasDuplicates", HasDuplicatesReactor.class);
		reactorHash.put("CurrentFrame", CurrentFrameReactor.class);
		reactorHash.put("SetCurrentFrame", SetCurrentFrameReactor.class);
		// filter model
		reactorHash.put("FrameFilterModel", FrameFilterModelReactor.class);
		reactorHash.put("FrameFilterModelFilteredValues", FrameFilterModelFilteredValuesReactor.class);
		reactorHash.put("FrameFilterModelVisibleValues", FrameFilterModelVisibleValuesReactor.class);
		reactorHash.put("FrameFilterModelNumericRange", FrameFilterModelNumericRangeReactor.class);

		// Algorithm Reactors
		reactorHash.put("rAlg", RAlgReactor.class);
		reactorHash.put("RunClustering", RunClusteringReactor.class);
		reactorHash.put("RunMultiClustering", RunMultiClusteringReactor.class);
		reactorHash.put("RunLOF", RunLOFReactor.class);
		reactorHash.put("RunSimilarity", RunSimilarityReactor.class);
		reactorHash.put("RunOutlier", RunOutlierReactor.class);
		reactorHash.put("Ratio", RatioReactor.class);
		reactorHash.put("RunAnomaly", RunAnomalyReactor.class);
		
		// X-Ray reactors
//		reactorHash.put("RunXray", RunXRayReactor.class);
		reactorHash.put("GetXrayConfigList", GetXrayConfigListReactor.class);
		reactorHash.put("GetXrayConfigFile", GetXrayConfigFileReactor.class);
		reactorHash.put("GetLocalSchema", GetLocalSchemaReactor.class);
		reactorHash.put("GetXLSchema", GetXLSchemaReactor.class);
		reactorHash.put("GetCSVSchema",GetCSVSchemaReactor.class);
//		reactorHash.put("GetExternalSchema", GetExternalSchemaReactor.class);
//		reactorHash.put("XrayMetamodel", XrayMetamodelReactor.class);
//		reactorHash.put("MetaSemanticSimilarity", MetaSemanticSimilarityReactor.class);
		
		reactorHash.put("SemanticBlending", SemanticBlendingReactor.class);
		reactorHash.put("SemanticDescription", SemanticDescription.class);
		// similar reactors to x-ray
		reactorHash.put("GetPhysicalToLogicalMapping", GetPhysicalToLogicalMapping.class);
		reactorHash.put("GetPhysicalToPhysicalMapping", GetPhysicalToPhysicalMapping.class);

		// these algorithms return viz data to the FE
		reactorHash.put("RunNumericalCorrelation", RunNumericalCorrelationReactor.class);
		reactorHash.put("RunMatrixRegression", RunMatrixRegressionReactor.class);
		reactorHash.put("RunClassification", RunClassificationReactor.class);
		reactorHash.put("RunAssociatedLearning", RunAssociatedLearningReactor.class);

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
		reactorHash.put("Login", LoginReactor.class);
		reactorHash.put("GitStatus", GitStatusReactor.class);
		reactorHash.put("GitVersion", prerna.util.git.reactors.GitVersion.class);
		reactorHash.put("CreateAsset", prerna.util.git.reactors.CreateAssetReactor.class);
		reactorHash.put("UpdateAsset", prerna.util.git.reactors.UpdateAssetReactor.class);
		reactorHash.put("DeleteAsset", prerna.util.git.reactors.DeleteAssetReactor.class);
		reactorHash.put("SyncAppO", SyncAppOReactor.class);
		reactorHash.put("SyncAppFilesO", SyncAppFilesO.class);
		
		// App Metadata
//		reactorHash.put("MyApps", MyAppsReactor.class);
		reactorHash.put("MyDatabases", MyDatabasesReactor.class);
		reactorHash.put("DatabaseInfo", DatabaseInfoReactor.class);
		reactorHash.put("DatabaseUsersReactor", DatabaseUsersReactor.class);
		// TODO: to be removed once FE changes to only use GetInsights
		reactorHash.put("GetAppInsights", GetInsightsReactor.class);
		reactorHash.put("GetInsights", GetInsightsReactor.class);
//		reactorHash.put("SetAppDescription", SetDatabaseDescriptionReactor.class);
//		reactorHash.put("SetAppTags", SetDatabaseTagsReactor.class);
//		reactorHash.put("GetAppWidgets", GetAppWidgetsReactor.class);
		reactorHash.put("AddDefaultInsights", AddDefaultInsightsReactor.class);
		// Insight Metadata
//		reactorHash.put("SetInsightTags", SetInsightTagsReactor.class);
//		reactorHash.put("SetInsightDescription", SetInsightDescriptionReactor.class);

		// Insight Comments
		reactorHash.put("AddInsightComment", AddInsightCommentReactor.class);
		reactorHash.put("DeleteInsightComment", DeleteInsightCommentReactor.class);
		reactorHash.put("ModifyInsightComment", ModifyInsightCommentReactor.class);
		reactorHash.put("GetInsightComments", GetInsightCommentsReactor.class);
		
		// Clusters
		reactorHash.put("OpenDatabase", OpenDatabaseReactor.class);
		reactorHash.put("CleanUpDatabases", CleanUpDatabasesReactor.class);
		reactorHash.put("Version", VersionReactor.class);
		//reactorHash.put("PullCloudApp", PullCloudAppReactor.class);
		//reactorHash.put("SyncRedis", SyncRedisReactor.class);
		//reactorHash.put("PullUserSpace", PullUserSpaceReactor.class);
		
		// User Space
		reactorHash.put("UploadUserFile", UploadUserFileReactor.class);
		reactorHash.put("UserDir", UserDirReactor.class);
		reactorHash.put("DeleteUserAsset", DeleteUserAssetReactor.class);
		reactorHash.put("NewDir", NewDirReactor.class);
		reactorHash.put("MoveUserAsset", MoveUserAssetReactor.class);

		// Scheduler
		reactorHash.put("ScheduleJob", ScheduleJobReactor.class);
		reactorHash.put("PauseJobTrigger", PauseJobTriggerReactor.class);
		reactorHash.put("ListAllJobs", ListAllJobsReactor.class);
		reactorHash.put("ResumeJobTrigger", ResumeJobTriggerReactor.class);
		// User Tracking
		reactorHash.put("UpdateSemanticData", UpdateSemanticDataReactor.class);
		reactorHash.put("UpdateQueryData", UpdateQueryDataReactor.class);
		// Recommendations
		reactorHash.put("DatabaseRecommendations", DatabaseRecommendationsReactor.class);
		reactorHash.put("VizRecommendations", VizRecommendationsReactor.class);
		reactorHash.put("PredictViz", CreateNLPVizReactor.class);
		reactorHash.put("GetDatabasesByDescription", GetDatabasesByDescriptionReactor.class);
		reactorHash.put("UpdateNLPHistory", UpdateNLPHistoryReactor.class);
		reactorHash.put("NLSQueryHelper", NLSQueryHelperReactor.class);
		
		// Forms
		reactorHash.put("UpdateForm", UpdateFormReactor.class);
		
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
		reactorHash.put("Py", PyReactor.class);
		reactorHash.put("R", RReactor.class);
		reactorHash.put("CancelR", CancelRReactor.class);
		reactorHash.put("REnableUserRecovery", REnableUserRecoveryReactor.class);
		reactorHash.put("RClearAllUserRservesReactor", RClearAllUserRservesReactor.class);
		reactorHash.put("RSource", RSourceReactor.class);
		reactorHash.put("PySource", PySourceReactor.class);
		reactorHash.put("Java", JavaReactor.class);
		
		// Pixel Recipe Parsing / Manipulation
		reactorHash.put("GetInsightDatasources", GetInsightDatasourcesReactor.class);
		reactorHash.put("ModifyInsightDatasource", ModifyInsightDatasourceReactor.class);
		reactorHash.put("GetOptimizedRecipe", GetOptimizedRecipeReactor.class);
		
		
		// web scrape engine
		reactorHash.put("GetTableHeader", GetTableHeader.class);
		reactorHash.put("GetNumTable", GetNumTableReactor.class);
		
		// Tax specific handles
//		reactorHash.put("LoadClient", LoadClientReactor.class);
//		reactorHash.put("RunPlan", RunPlanReactor.class);
//		reactorHash.put("UpdatePlan", UpdateGraphPlannerReactor2.class);
//		reactorHash.put("TaxRetrieveValue", TaxRetrieveValue2.class);
//		reactorHash.put("RunAliasMatch", RunAliasMatchReactor.class);
//		reactorHash.put("SaveTaxScenario", SaveTaxScenarioReactor.class);
		
		// bitly
		reactorHash.put("badd", BaddReactor.class);
		reactorHash.put("bupd", BupdReactor.class);
		reactorHash.put("bdel", BDelReactor.class);
		reactorHash.put("bq", BQReactor.class);
		
		// Dates
		reactorHash.put("DATE", DateReactor.class);
		reactorHash.put("DAY", DayReactor.class);
		reactorHash.put("WEEK", WeekReactor.class);
		reactorHash.put("MONTH", MonthReactor.class);
		reactorHash.put("YEAR", YearReactor.class);
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
		h2FrameHash.put("ExtractLetters", prerna.sablecc2.reactor.frame.rdbms.ExtractLettersReactor.class);
		h2FrameHash.put("ExtractNumbers", prerna.sablecc2.reactor.frame.rdbms.ExtractNumbersReactor.class);
		h2FrameHash.put("JoinColumns", prerna.sablecc2.reactor.frame.rdbms.JoinColumnsReactor.class);
		h2FrameHash.put("RenameColumn", prerna.sablecc2.reactor.frame.rdbms.RenameColumnReactor.class);
		h2FrameHash.put("SplitColumns", prerna.sablecc2.reactor.frame.rdbms.SplitColumnsReactor.class);
		h2FrameHash.put("ToLowerCase", prerna.sablecc2.reactor.frame.rdbms.ToLowerCaseReactor.class);
		h2FrameHash.put("ToUpperCase", prerna.sablecc2.reactor.frame.rdbms.ToUpperCaseReactor.class);
		h2FrameHash.put("TrimColumns", prerna.sablecc2.reactor.frame.rdbms.TrimColumnReactor.class);
	}

	private static void populateRFrameHash(Map<String, Class> rFrameHash) {
		rFrameHash.put("AddColumn", prerna.sablecc2.reactor.frame.r.AddColumnReactor.class);
		rFrameHash.put("AutoCleanColumn", prerna.sablecc2.reactor.frame.r.AutoCleanColumnReactor.class);
		rFrameHash.put("ChangeColumnType", prerna.sablecc2.reactor.frame.r.ChangeColumnTypeReactor.class);
		rFrameHash.put("CountIf", prerna.sablecc2.reactor.frame.r.CountIfReactor.class);
		rFrameHash.put("Collapse", prerna.sablecc2.reactor.frame.r.CollapseReactor.class);
		rFrameHash.put("Concatenate", prerna.sablecc2.reactor.frame.r.ConcatenateReactor.class);
		rFrameHash.put("DropColumn", prerna.sablecc2.reactor.frame.r.DropColumnReactor.class);
		rFrameHash.put("DropRows", prerna.sablecc2.reactor.frame.r.DropRowsReactor.class);
		rFrameHash.put("DuplicateColumn", prerna.sablecc2.reactor.frame.r.DuplicateColumnReactor.class);
		rFrameHash.put("ExtractLetters", prerna.sablecc2.reactor.frame.r.ExtractLettersReactor.class);
		rFrameHash.put("ExtractNumbers", prerna.sablecc2.reactor.frame.r.ExtractNumbersReactor.class);
		rFrameHash.put("JoinColumns", prerna.sablecc2.reactor.frame.r.JoinColumnsReactor.class);
		rFrameHash.put("Pivot", prerna.sablecc2.reactor.frame.r.PivotReactor.class);
		rFrameHash.put("RegexReplaceColumnValue", prerna.sablecc2.reactor.frame.r.RegexReplaceColumnValueReactor.class);
		rFrameHash.put("RemoveDuplicateRows", prerna.sablecc2.reactor.frame.r.RemoveDuplicateRowsReactor.class);
		rFrameHash.put("RenameColumn", prerna.sablecc2.reactor.frame.r.RenameColumnReactor.class);
		rFrameHash.put("ReplaceColumnValue", prerna.sablecc2.reactor.frame.r.ReplaceColumnValueReactor.class);
		rFrameHash.put("SortColumn", prerna.sablecc2.reactor.frame.r.SortColumnReactor.class);
		rFrameHash.put("SplitColumns", prerna.sablecc2.reactor.frame.r.SplitColumnsReactor.class);
		rFrameHash.put("SplitUnpivot", prerna.sablecc2.reactor.frame.r.SplitUnpivotReactor.class);
		rFrameHash.put("ToLowerCase", prerna.sablecc2.reactor.frame.r.ToLowerCaseReactor.class);
		rFrameHash.put("ToUpperCase", prerna.sablecc2.reactor.frame.r.ToUpperCaseReactor.class);
		rFrameHash.put("ToProperCase", prerna.sablecc2.reactor.frame.r.ToProperCaseReactor.class);
		rFrameHash.put("TrimColumns", prerna.sablecc2.reactor.frame.r.TrimColumnsReactor.class);
		rFrameHash.put("Transpose", prerna.sablecc2.reactor.frame.r.TransposeReactor.class);
		rFrameHash.put("Unpivot", prerna.sablecc2.reactor.frame.r.UnpivotReactor.class);
		rFrameHash.put("UpdateRowValues", prerna.sablecc2.reactor.frame.r.UpdateRowValuesReactor.class);
		rFrameHash.put("Discretize", prerna.sablecc2.reactor.frame.r.DiscretizeReactor.class);
		rFrameHash.put("DateExpander", prerna.sablecc2.reactor.frame.r.DateExpanderReactor.class);
		rFrameHash.put("DateDifference", prerna.sablecc2.reactor.frame.r.DateDifferenceReactor.class);
		rFrameHash.put("DateAddValue", prerna.sablecc2.reactor.frame.r.DateAddValueReactor.class);

		// frame stats
		rFrameHash.put("ColumnCount", prerna.sablecc2.reactor.frame.r.ColumnCountReactor.class);
		rFrameHash.put("DescriptiveStats", prerna.sablecc2.reactor.frame.r.DescriptiveStatsReactor.class);
		rFrameHash.put("SummaryStats", prerna.sablecc2.reactor.frame.r.SummaryStatsReactor.class);
		rFrameHash.put("Histogram", prerna.sablecc2.reactor.frame.r.HistogramReactor.class);
		
		// algorithms
		rFrameHash.put("RunAssociatedLearning", prerna.sablecc2.reactor.frame.r.analytics.RunAssociatedLearningReactor.class);
		rFrameHash.put("RunClassification", prerna.sablecc2.reactor.frame.r.analytics.RunClassificationReactor.class);
		rFrameHash.put("RunClustering", prerna.sablecc2.reactor.frame.r.analytics.RunClusteringReactor.class);
		rFrameHash.put("RunDescriptionGenerator", prerna.sablecc2.reactor.frame.r.analytics.RunDescriptionGeneratorReactor.class);
		rFrameHash.put("RunDocCosSimilarity", prerna.sablecc2.reactor.frame.r.analytics.RunDocCosSimilarityReactor.class);
		rFrameHash.put("RunDocumentSummarization", prerna.sablecc2.reactor.algorithms.RunDocumentSummarizationReactor.class);
		rFrameHash.put("RunLOF", prerna.sablecc2.reactor.frame.r.analytics.RunLOFReactor.class);
		rFrameHash.put("RunMatrixRegression", prerna.sablecc2.reactor.frame.r.analytics.RunMatrixRegressionReactor.class);
		rFrameHash.put("RunNumericalColumnSimilarity", prerna.sablecc2.reactor.frame.r.analytics.RunNumericalColumnSimilarityReactor.class);
		rFrameHash.put("RunNumericalCorrelation", prerna.sablecc2.reactor.frame.r.analytics.RunNumericalCorrelationReactor.class);
		rFrameHash.put("RunNumericalModel", prerna.sablecc2.reactor.frame.r.analytics.RunNumericalModelReactor.class);
		rFrameHash.put("RunOutlier", prerna.sablecc2.reactor.frame.r.analytics.RunOutlierReactor.class);
		rFrameHash.put("RunRandomForest", prerna.sablecc2.reactor.frame.r.analytics.RunRandomForestReactor.class);
		rFrameHash.put("GetRFResults", prerna.sablecc2.reactor.frame.r.analytics.GetRFResultsReactor.class);
		rFrameHash.put("RunSimilarity", prerna.sablecc2.reactor.frame.r.analytics.RunSimilarityReactor.class);
		rFrameHash.put("RunSimilarityHeat", prerna.sablecc2.reactor.frame.r.analytics.RunSimilarityHeatReactor.class);
		rFrameHash.put("MatchColumnValues", prerna.sablecc2.reactor.frame.r.MatchColumnValuesReactor.class);
		rFrameHash.put("UpdateMatchColumnValues", prerna.sablecc2.reactor.frame.r.UpdateMatchColumnValuesReactor.class);
//		rFrameHash.put("MetaSemanticSimilarity", prerna.sablecc2.reactor.frame.r.MetaSemanticSimilarityReactor.class);

		// data quality rectors
		rFrameHash.put("RunDataQuality",RunDataQualityReactor.class);
		rFrameHash.put("GetDQRules", GetDQRulesReactor.class);
	}

	private static void populateTinkerFrameHash(Map<String, Class> tinkerFrameHash) {
		tinkerFrameHash.put("ConnectedNodes", ConnectedNodesReactor.class);
		tinkerFrameHash.put("RemoveIntermediaryNode", RemoveIntermediaryNodeReactor.class);
		tinkerFrameHash.put("FindPathsConnectingNodes", FindPathsConnectingNodesReactor.class);
		tinkerFrameHash.put("FindPathsConnectingGroups", FindPathsConnectingGroupsReactor.class);
		// require r
		tinkerFrameHash.put("ChangeGraphLayout", ChangeGraphLayoutReactor.class);
		tinkerFrameHash.put("ClusterGraph", ClusterGraphReactor.class);
		tinkerFrameHash.put("NodeDetails", NodeDetailsReactor.class);
	}
	
	private static void populatePandasFrameHash(Map<String, Class> pandasFrameHash) {
		pandasFrameHash.put("ToUpperCase", prerna.sablecc2.reactor.frame.py.ToUpperCaseReactor.class);
		pandasFrameHash.put("ToLowerCase", prerna.sablecc2.reactor.frame.py.ToLowerCaseReactor.class);
		pandasFrameHash.put("ToProperCase", prerna.sablecc2.reactor.frame.py.ToProperCaseReactor.class);
		pandasFrameHash.put("ReplaceColumnValue", prerna.sablecc2.reactor.frame.py.ReplaceColumnValueReactor.class);
		pandasFrameHash.put("RenameColumn", prerna.sablecc2.reactor.frame.py.RenameColumnReactor.class);
		pandasFrameHash.put("Pivot", prerna.sablecc2.reactor.frame.py.PivotReactor.class);
		pandasFrameHash.put("ChangeColumnType", prerna.sablecc2.reactor.frame.py.ChangeColumnTypeReactor.class);
		pandasFrameHash.put("DropRows", prerna.sablecc2.reactor.frame.py.DropRowsReactor.class);
		pandasFrameHash.put("DropColumn", prerna.sablecc2.reactor.frame.py.DropColumnReactor.class);
		pandasFrameHash.put("DuplicateColumn", prerna.sablecc2.reactor.frame.py.DuplicateColumnReactor.class);
		pandasFrameHash.put("ExtractLetters", prerna.sablecc2.reactor.frame.py.ExtractLettersReactor.class);
		pandasFrameHash.put("ExtractNumbers", prerna.sablecc2.reactor.frame.py.ExtractNumbersReactor.class);
		pandasFrameHash.put("CountIf", prerna.sablecc2.reactor.frame.py.CountIfReactor.class);
		pandasFrameHash.put("SplitColumns", prerna.sablecc2.reactor.frame.py.SplitColumnsReactor.class);
		pandasFrameHash.put("UpdateRowValues", prerna.sablecc2.reactor.frame.py.UpdateRowValuesReactor.class);
		pandasFrameHash.put("SplitUnpivot", prerna.sablecc2.reactor.frame.py.SplitUnpivotReactor.class);
		pandasFrameHash.put("TrimColumns", prerna.sablecc2.reactor.frame.py.TrimColumnsReactor.class);
		pandasFrameHash.put("MatchColumnValues", prerna.sablecc2.reactor.frame.py.MatchColumnValuesReactor.class);
		pandasFrameHash.put("Collapse", prerna.sablecc2.reactor.frame.py.CollapseReactor.class);
		pandasFrameHash.put("JoinColumns", prerna.sablecc2.reactor.frame.py.JoinColumnsReactor.class);
		
		// frame stats
		pandasFrameHash.put("ColumnCount", prerna.sablecc2.reactor.frame.py.ColumnCountReactor.class);
		pandasFrameHash.put("Histogram", prerna.sablecc2.reactor.frame.py.ColumnCountReactor.class);
		pandasFrameHash.put("DescriptiveStats", prerna.sablecc2.reactor.frame.py.DescriptiveStatsReactor.class);
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
		expressionHash.put("ISOBJECTEMPTY", OpIsObjectEmpty.class);
		expressionHash.put("ASSTRING", OpAsString.class);
		expressionHash.put("CONCAT", OpConcat.class);
		
		// none excel functions
		// If is in its own category
		expressionHash.put("IF", IfReactor.class);
		expressionHash.put("LIST", OpList.class);
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
				if (frame instanceof AbstractRdbmsFrame) {
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
					if (pandasFrameHash.containsKey(reactorId)) {
						reactor = (IReactor) pandasFrameHash.get(reactorId).newInstance();
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
		if (parentReactor instanceof AbstractQueryStructReactor || parentReactor instanceof QuerySelectorExpressionAssimilator) {
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
	
	public static List recommend(String reactorName)
	{

		//List <ExtractedResult> resList = FuzzySearch.extractTop(data, list, 3);
		// run reduction loop
		int weight = 100;
		List <ExtractedResult> resList = null;
		
		List <String> retList = new ArrayList();
		do
		{
			resList = FuzzySearch.extractAll(reactorName, nmList, weight);
			weight = weight -1;
			System.out.print(weight);
		}while (resList.size() == 0 && weight > 50);
		
		
		for(int listIndex = 0;listIndex < resList.size();listIndex++)
		{
			ExtractedResult thisRes = resList.get(listIndex);
			int index = thisRes.getIndex();
			Class retClass = classList.get(index);
			System.out.println(thisRes.getString() + "<>" + thisRes.getScore());
			System.out.println("Class >>  " + retClass);
			if(!Modifier.isAbstract( retClass.getModifiers() ))
			{
				retList.add(thisRes.getString());
			}
		}
		
		return retList;
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
