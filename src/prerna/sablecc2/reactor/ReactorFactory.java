package prerna.sablecc2.reactor;

import java.util.HashMap;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc2.reactor.export.CollectReactor;
import prerna.sablecc2.reactor.export.JobMetaCollectorReactor;
import prerna.sablecc2.reactor.export.job.FormatReactor;
import prerna.sablecc2.reactor.export.job.ViewOptionsReactor;
import prerna.sablecc2.reactor.expression.DefaultOpReactor;
import prerna.sablecc2.reactor.expression.IfError;
import prerna.sablecc2.reactor.expression.OpAbsolute;
import prerna.sablecc2.reactor.expression.OpAnd;
import prerna.sablecc2.reactor.expression.OpLarge;
import prerna.sablecc2.reactor.expression.OpMatch;
import prerna.sablecc2.reactor.expression.OpMax;
import prerna.sablecc2.reactor.expression.OpMean;
import prerna.sablecc2.reactor.expression.OpMedian;
import prerna.sablecc2.reactor.expression.OpMin;
import prerna.sablecc2.reactor.expression.OpOr;
import prerna.sablecc2.reactor.expression.OpPower;
import prerna.sablecc2.reactor.expression.OpRound;
import prerna.sablecc2.reactor.expression.OpSmall;
import prerna.sablecc2.reactor.expression.OpSum;
import prerna.sablecc2.reactor.expression.OpSumIf;
import prerna.sablecc2.reactor.expression.OpSumIfs;
import prerna.sablecc2.reactor.expression.OpSumProduct;
import prerna.sablecc2.reactor.imports.ImportDataReactor;
import prerna.sablecc2.reactor.imports.MergeDataReactor;
import prerna.sablecc2.reactor.masterdatabase.ConnectedConceptsReactor;
import prerna.sablecc2.reactor.masterdatabase.DatabaseConceptPropertiesReactors;
import prerna.sablecc2.reactor.masterdatabase.DatabaseConceptsReactors;
import prerna.sablecc2.reactor.masterdatabase.DatabaseListReactor;
import prerna.sablecc2.reactor.masterdatabase.DatabaseMetamodelReactor;
import prerna.sablecc2.reactor.panel.AddPanelOrnamentsReactor;
import prerna.sablecc2.reactor.panel.AddPanelReactor;
import prerna.sablecc2.reactor.panel.ClosePanelReactor;
import prerna.sablecc2.reactor.panel.GetInsightPanelsReactor;
import prerna.sablecc2.reactor.panel.PanelCloneReactor;
import prerna.sablecc2.reactor.panel.PanelReactor;
import prerna.sablecc2.reactor.panel.ResetPanelOrnamentsReactor;
import prerna.sablecc2.reactor.panel.RetrievePanelOrnamentsReactor;
import prerna.sablecc2.reactor.panel.SetPanelLabel;
import prerna.sablecc2.reactor.planner.GraphPlanReactor;
import prerna.sablecc2.reactor.planner.graph.ExecuteJavaGraphPlannerReactor;
import prerna.sablecc2.reactor.planner.graph.LoadGraphClient;
import prerna.sablecc2.reactor.planner.graph.UpdateGraphPlannerReactor2;
import prerna.sablecc2.reactor.qs.AverageReactor;
import prerna.sablecc2.reactor.qs.DatabaseReactor;
import prerna.sablecc2.reactor.qs.FrameReactor;
import prerna.sablecc2.reactor.qs.GroupByReactor;
import prerna.sablecc2.reactor.qs.JoinReactor;
import prerna.sablecc2.reactor.qs.LimitReactor;
import prerna.sablecc2.reactor.qs.OffsetReactor;
import prerna.sablecc2.reactor.qs.QueryFilterReactor;
import prerna.sablecc2.reactor.qs.QueryReactor;
import prerna.sablecc2.reactor.qs.SelectReactor;
import prerna.sablecc2.reactor.qs.SumReactor;
import prerna.sablecc2.reactor.storage.RetrieveValue;
import prerna.sablecc2.reactor.storage.StoreValue;
import prerna.sablecc2.reactor.storage.TaxRetrieveValue2;
import prerna.sablecc2.reactor.storage.TaxSaveScenarioReactor;
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

	static {
		reactorHash = new HashMap<>();
		createReactorHash(reactorHash);

		expressionHash = new HashMap<>();
		populateExpressionSet(expressionHash);
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
	}

	// populates the frame agnostic reactors used by pixel
	private static void createReactorHash(Map<String, Class> reactorHash) {
		// Import Reactors
		// takes in a query struct and imports data to a new frame
		reactorHash.put("Import", ImportDataReactor.class); 
		// takes in a query struct and merges data to an  existing frame
		reactorHash.put("Merge", MergeDataReactor.class); 

		// Query Struct Reactors
		 // builds the select portion of the QS
		reactorHash.put("Select", SelectReactor.class);
		reactorHash.put("Average", AverageReactor.class);
		reactorHash.put("Sum", SumReactor.class);
		reactorHash.put("Group", GroupByReactor.class);
		reactorHash.put("Limit", LimitReactor.class);
		reactorHash.put("Offset", OffsetReactor.class);
		reactorHash.put("Join", JoinReactor.class);
		reactorHash.put("Filter", QueryFilterReactor.class);
		reactorHash.put("Query", QueryReactor.class);

		// Data Source Reactors
		// specifies that our pksl operations after this point are dealing with the specified database
		reactorHash.put("Database", DatabaseReactor.class); 
		reactorHash.put("Datasource", DatabaseReactor.class); 

		// specifies that our pksl operations after this point are dealing with the specified frame
		reactorHash.put("Frame", FrameReactor.class); 
		reactorHash.put("CreateFrame", CreateFrame.class);

		// Reducers
		// this takes in a query struct and produces an iterator
		reactorHash.put("Iterate", IterateReactor.class); 

		// Exporting Reactors
		reactorHash.put("Job", JobReactor.class); // defines the job
		reactorHash.put("Collect", CollectReactor.class); // collect
		reactorHash.put("CollectMeta", JobMetaCollectorReactor.class); // collect
		reactorHash.put("Format", FormatReactor.class); // set formats
		reactorHash.put("ViewOptions", ViewOptionsReactor.class); // set options

		// If is in its own category
		reactorHash.put("if", IfReactor.class);

		// In mem storage of data
		reactorHash.put("StoreValue", StoreValue.class);
		reactorHash.put("RetrieveValue", RetrieveValue.class);
		reactorHash.put("GraphPlan", GraphPlanReactor.class);
//		reactorHash.put("CreateStore", CreateStoreReactor.class);

		// Tax specific handles
		reactorHash.put("LoadClient", LoadGraphClient.class);
		reactorHash.put("RunPlan", ExecuteJavaGraphPlannerReactor.class);
		reactorHash.put("UpdatePlan", UpdateGraphPlannerReactor2.class);
		reactorHash.put("TaxRetrieveValue", TaxRetrieveValue2.class);
		reactorHash.put("RunAliasMatch", AliasMatchTestReactor.class);
		reactorHash.put("SaveTaxScenario", TaxSaveScenarioReactor.class);

		// Local Master Reactors
		reactorHash.put("GetDatabaseList", DatabaseListReactor.class);
		reactorHash.put("GetDatabaseConcepts", DatabaseConceptsReactors.class);
		reactorHash.put("GetConnectedConcepts", ConnectedConceptsReactor.class);
		reactorHash.put("GetConceptProperties", DatabaseConceptPropertiesReactors.class);
		reactorHash.put("GetEngineMetamodel", DatabaseMetamodelReactor.class);
		
		// Panel Reactors
		reactorHash.put("InsightPanelIds", GetInsightPanelsReactor.class);
		reactorHash.put("Panel", PanelReactor.class);
		reactorHash.put("Clone", PanelCloneReactor.class);
		reactorHash.put("AddPanel", AddPanelReactor.class);
		reactorHash.put("ClosePanel", ClosePanelReactor.class);
		reactorHash.put("AddPanelOrnaments", AddPanelOrnamentsReactor.class);
		reactorHash.put("ResetPanelOrnaments", ResetPanelOrnamentsReactor.class);
		reactorHash.put("RetrievePanelOrnaments", RetrievePanelOrnamentsReactor.class);
		reactorHash.put("SetPanelLabel", SetPanelLabel.class);
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
				if (!(parentReactor instanceof SelectReactor)) {
					reactor = (IReactor) expressionHash.get(reactorId.toUpperCase()).newInstance();
					reactor.setPKSL(reactorId, nodeString);
					return reactor;
				}
			}
			// if not an expression
			// search in the normal reactor hash
			if (reactorHash.containsKey(reactorId)) {
				reactor = (IReactor) reactorHash.get(reactorId).newInstance();
				reactor.setPKSL(reactorId, nodeString);
				return reactor;
			}
		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
		}

		// reactor = new SamplReactor();
		reactor = new DefaultOpReactor();
		reactor.setPKSL(reactorId, nodeString);
		return reactor;
	}

	public static boolean hasReactor(String reactorId) {
		return reactorHash.containsKey(reactorId) || expressionHash.containsKey(reactorId.toUpperCase());
	}
}
