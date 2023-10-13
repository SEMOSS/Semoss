package prerna.reactor.algorithms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ibm.icu.text.DecimalFormat;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.usertracking.AnalyticsTrackerHelper;
import prerna.util.usertracking.UserTrackerFactory;
import weka.associations.Apriori;
import weka.associations.AssociationRule;
import weka.associations.AssociationRules;
import weka.associations.Item;
import weka.core.Attribute;
import weka.core.Instances;

public class RunAssociatedLearningReactor extends AbstractFrameReactor {

	private static final Logger classLogger = LogManager.getLogger(RunAssociatedLearningReactor.class);

	private static final String CLASS_NAME = RunAssociatedLearningReactor.class.getName();

	private static final String X_AXIS_NAME = "Confidence";
	private static final String Z_AXIS_NAME = "Count";

	private static final String NUM_RULES = "numRules";
	private static final String CONFIDENCE_LEVEL = "confPer";
	private static final String MIN_SUPPORT = "minSupport";
	private static final String MAX_SUPPORT = "maxSupport";

	private Instances instancesData;

	private Map<Integer, Collection<Item>> premises;
	private Map<Integer, Collection<Item>> consequences;
	private Map<Integer, Integer> counts;
	private Map<Integer, Double> confidenceIntervals;

	private boolean[] isNumeric;
	private int numRules; // number of rules to output
	private double confPer; // min confidence lvl (percentage)
	private double minSupport; // min number of rows required for rule
								// (percentage of total rows of data)
	private double maxSupport; // max number of rows required for rule
								// (percentage of total rows of data)
	private List<String> attributesList;

	/**
	 * RunAssociatedLearning(numRules = [numRules], confPer = [confidenceValue],
	 * minSupport = [minSupport], maxSupport = [maxSupport], skipAttributes =
	 * [skipAttributes]);
	 */
	
	public RunAssociatedLearningReactor() {
		this.keysToGet = new String[]{NUM_RULES, CONFIDENCE_LEVEL, MIN_SUPPORT, MAX_SUPPORT, ReactorKeysEnum.ATTRIBUTES.getKey(), ReactorKeysEnum.PANEL.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = this.getLogger(CLASS_NAME);
		ITableDataFrame dataFrame = getFrame();
		dataFrame.setLogger(logger);
		
		// get inputs from pixel command
		this.numRules = getNumRules();
		this.confPer = getConf();
		this.minSupport = getMinSupport();
		this.maxSupport = getMaxSupport();
		this.attributesList = getAttributesList();
		if(attributesList.size() == 0) {
			String errorString = "No columns were passed as attributes for the association learning routine.";
			logger.info(errorString);
			throw new IllegalArgumentException(errorString);
		}

		int numCols = this.attributesList.size();
		String[] retHeaders = new String[numCols];
		SelectQueryStruct qs = new SelectQueryStruct();
		this.isNumeric = new boolean[numCols];

		// create a qs with selected columns
		// need to check the format
		for (int i = 0; i < numCols; i++) {
			String header = this.attributesList.get(i);
			QueryColumnSelector qsHead = new QueryColumnSelector();
			if (header.contains("__")) {
				String[] split = header.split("__");
				qsHead.setTable(split[0]);
				qsHead.setColumn(split[1]);
				retHeaders[i] = split[1];
			} else {
				qsHead.setTable(header);
				retHeaders[i] = header;
			}
			this.isNumeric[i] = dataFrame.isNumeric(header);
			qs.addSelector(qsHead);
		}

		// re-add frame filters
		qs.mergeImplicitFilters(dataFrame.getFrameFilters());

		int numRows = getNumRows(dataFrame, (QueryColumnSelector) qs.getSelectors().get(0));
		IRawSelectWrapper it = null;
		try {
			it = dataFrame.query(qs);
			logger.info("Start converting frame into WEKA Instacnes data structure");
			this.instancesData = WekaReactorHelper.genInstances(retHeaders, isNumeric, numRows);
			this.instancesData = WekaReactorHelper.fillInstances(this.instancesData, it, this.isNumeric, logger);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(it != null) {
				try {
					it.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		// associated learning only takes categorical values, so we need to
		// discretize all numerical fields
		this.instancesData = WekaReactorHelper.discretizeAllNumericField(this.instancesData);
		logger.info("Done converting frame into WEKA Instacnes data structure");

		premises = new HashMap<Integer, Collection<Item>>();
		consequences = new HashMap<Integer, Collection<Item>>();
		counts = new HashMap<Integer, Integer>();
		confidenceIntervals = new HashMap<Integer, Double>();

		Apriori apriori = new Apriori();
		apriori.setNumRules(numRules);
		apriori.setMinMetric(confPer);
		apriori.setLowerBoundMinSupport(minSupport);
		apriori.setUpperBoundMinSupport(maxSupport);

		logger.info("Running Apriori Algorithm...");
		try {
			apriori.buildAssociations(this.instancesData);
		} catch (Exception e) {
			e.printStackTrace();
		}
		logger.info("Finished Running Algorithm...");
		
		// track GA data
//		UserTrackerFactory.getInstance().trackAnalyticsPixel(this.insight, "AssociatedLearning");
		
		// NEW TRACKING
		UserTrackerFactory.getInstance().trackAnalyticsWidget(
				this.insight, 
				dataFrame, 
				"AssociatedLearning", 
				AnalyticsTrackerHelper.getHashInputs(this.store, this.keysToGet));
		
		// get and store rules
		AssociationRules rules = apriori.getAssociationRules();
		List<AssociationRule> ruleList = rules.getRules();
		if (ruleList.isEmpty()) {
			throw new IllegalArgumentException("Assocation Learning Algorithm ran successfully, but no results were found.");
		}
		int numRule = 0;
		for (AssociationRule rule : ruleList) {
			premises.put(numRule, rule.getPremise());
			consequences.put(numRule, rule.getConsequence());
			counts.put(numRule, rule.getTotalSupport());
			confidenceIntervals.put(numRule, rule.getPrimaryMetricValue());
			numRule++;
		}

		logger.info("Generating Decision Viz Data...");
		return new NounMetadata(getAlgorithmOutput(), PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.VIZ_OUTPUT);
	}

	public Object getAlgorithmOutput() {
		Map<String, Object> allHash = new HashMap<String, Object>();
		allHash.putAll(generateDecisionRuleVizualization());
		allHash.put("layout", "SingleAxisCluster");
		allHash.put("panelId", getPanelId());
		allHash.put("dataTableAlign", getDataTableAlign());
		return allHash;
	}

	public Map<String, String> getDataTableAlign() {
		Map<String, String> dataTableAlign = new HashMap<String, String>();
		dataTableAlign.put("x", X_AXIS_NAME);
		dataTableAlign.put("size", Z_AXIS_NAME);
		return dataTableAlign;
	}

	public Map<String, Object> generateDecisionRuleVizualization() {
		// return if no rules found
		if (premises.isEmpty() && consequences.isEmpty() && counts.isEmpty()) {
			return new Hashtable<String, Object>();
		}

		DecimalFormat format = new DecimalFormat("0.00");

		List<List<Object>> retItemList = new ArrayList<List<Object>>();
		for (Integer numRule : premises.keySet()) {
			Collection<Item> premise = premises.get(numRule);
			Collection<Item> consequence = consequences.get(numRule);
			int count = counts.get(numRule);
			double confidence = confidenceIntervals.get(numRule);

			List<Object> item = new ArrayList<Object>();
			item.add(count);
			item.add(format.format(confidence));
			item.add(getConcatedItems(premise));
			item.add(getConcatedItems(consequence));

			retItemList.add(item);
		}

		String[] headers = new String[] { Z_AXIS_NAME, X_AXIS_NAME, "Premises", "Consequence" };
		Map<String, Object> retHash = new Hashtable<String, Object>();
		retHash.put("headers", headers);
		retHash.put("data", retItemList);

		return retHash;
	}

	private String getConcatedItems(Collection<Item> values) {
		String retVal = "";
		for (Item item : values) {
			Attribute category = item.getAttribute();
			String name = category.name().trim();
			String value = item.getItemValueAsString().trim();
			if (retVal.equals("")) {
				retVal = name + " = " + value;
			} else {
				retVal += retVal + " & " + name + " = " + value + " ";
			}
		}

		return retVal;
	}

	///////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////
	///////////////////////   PIXEL INPUTS   //////////////////////////////
	
	private int getNumRules() {
		GenRowStruct numRulesGrs = this.store.getNoun(NUM_RULES);
		int numRules = -1;
		NounMetadata numRulesNoun;
		if (numRulesGrs != null) {
			numRulesNoun = numRulesGrs.getNoun(0);
			numRules = ((Number) numRulesNoun.getValue()).intValue();
		} else {
			// else, we assume it is the zero index in the current row -->
			// runAssociatedLearning(numRules = [numRules], confPer =
			// [confidenceValue], minSupport = [minSupport], maxSupport =
			// [maxSupport], skipAttributes = [skipAttributes]);
			numRulesNoun = this.curRow.getNoun(0);
			numRules = ((Number) numRulesNoun.getValue()).intValue();
		}
		return numRules;

	}

	private double getConf() {
		GenRowStruct confGrs = this.store.getNoun(CONFIDENCE_LEVEL);
		double conf = 0.0;
		NounMetadata confNoun;
		if (confGrs != null) {
			confNoun = confGrs.getNoun(0);
			conf = ((Number) confNoun.getValue()).doubleValue();
		} else {
			// else, we assume it is the first index in the current row -->
			// runAssociatedLearning(numRules = [numRules], confPer =
			// [confidenceValue], minSupport = [minSupport], maxSupport =
			// [maxSupport], skipAttributes = [skipAttributes]);
			confNoun = this.curRow.getNoun(1);
			conf = ((Number) confNoun.getValue()).doubleValue();
		}
		return conf;

	}

	private double getMinSupport() {
		GenRowStruct minGrs = this.store.getNoun(MIN_SUPPORT);
		double minSupport = 0.0;
		NounMetadata minNoun;
		if (minGrs != null) {
			minNoun = minGrs.getNoun(0);
			minSupport = ((Number) minNoun.getValue()).doubleValue();
		} else {
			// else, we assume it is the second index in the current row -->
			// runAssociatedLearning(numRules = [numRules], confPer =
			// [confidenceValue], minSupport = [minSupport], maxSupport =
			// [maxSupport], skipAttributes = [skipAttributes]);
			minNoun = this.curRow.getNoun(2);
			minSupport = ((Number) minNoun.getValue()).doubleValue();
		}
		return minSupport;

	}

	private double getMaxSupport() {
		GenRowStruct maxGrs = this.store.getNoun(MAX_SUPPORT);
		double maxSupport = 0.0;
		NounMetadata maxNoun;
		if (maxGrs != null) {
			maxNoun = maxGrs.getNoun(0);
			maxSupport = ((Number) maxNoun.getValue()).doubleValue();
		} else {
			// else, we assume it is the third index in the current row -->
			// runAssociatedLearning(numRules = [numRules], confPer =
			// [confidenceValue], minSupport = [minSupport], maxSupport =
			// [maxSupport], skipAttributes = [skipAttributes]);
			maxNoun = this.curRow.getNoun(3);
			maxSupport = ((Number) maxNoun.getValue()).doubleValue();
		}
		return maxSupport;
	}

	private List<String> getAttributesList() {
		List<String> retList = new ArrayList<String>();

		// check if attributeList was entered with key or not
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[4]);
		if (columnGrs != null) {
			for (NounMetadata noun : columnGrs.vector) {
				retList.add(noun.getValue().toString());
			}
		} else {
			// grab lengths 4-> end columns
			// runAssociatedLearning(numRules = [numRules], confPer =
			// [confidenceValue], minSupport = [minSupport], maxSupport =
			// [maxSupport], skipAttributes = [skipAttributes]);
			int rowLength = this.curRow.size();
			for (int i = 4; i < rowLength; i++) {
				NounMetadata colNoun = this.curRow.getNoun(i);
				retList.add(colNoun.getValue().toString());
			}
		}

		return retList;
	}
	
	private String getPanelId() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[5]);
		if(columnGrs != null) {
			if(columnGrs.size() > 0) {
				return columnGrs.get(0).toString();
			}
		}
		return null;
	}

	private int getNumRows(ITableDataFrame frame, QueryColumnSelector predictorCol) {
		SelectQueryStruct qs = new SelectQueryStruct();
		QueryFunctionSelector math = new QueryFunctionSelector();
		math.addInnerSelector(predictorCol);
		math.setFunction(QueryFunctionHelper.COUNT);
		qs.addSelector(math);

		IRawSelectWrapper countIt = null;
		try {
			countIt = frame.query(qs);
			while (countIt.hasNext()) {
				return ((Number) countIt.next().getValues()[0]).intValue();
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(countIt != null) {
				try {
					countIt.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return 0;
	}
	
	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(NUM_RULES)) {
			return "The number of rules to output";
		} else if (key.equals(CONFIDENCE_LEVEL)) {
			return "The minimum confidence level";
		} else if (key.equals(MIN_SUPPORT)) {
			return "The minimum number of rows required for rule (percentage of total rows of data)";
		} else if (key.equals(MAX_SUPPORT)) {
			return "The maximum number of rows required for rule (percentage of total rows of data)";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
