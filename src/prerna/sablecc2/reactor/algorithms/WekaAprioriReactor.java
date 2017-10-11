package prerna.sablecc2.reactor.algorithms;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.ibm.icu.text.DecimalFormat;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.selectors.QueryAggregationEnum;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryMathSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;
import weka.associations.Apriori;
import weka.associations.AssociationRule;
import weka.associations.AssociationRules;
import weka.associations.Item;
import weka.core.Attribute;
import weka.core.Instances;

public class WekaAprioriReactor extends AbstractReactor {

	private static final Logger LOGGER = LogManager.getLogger(WekaAprioriReactor.class.getName());

	private static final String X_AXIS_NAME = "Confidence";
	private static final String Z_AXIS_NAME = "Count";

	private static final String NUM_RULES = "numRules";
	private static final String CONFIDENCE_LEVEL = "confPer";
	private static final String MIN_SUPPORT = "minSupport";
	private static final String MAX_SUPPORT = "maxSupport";
	private static final String ATTRIBUTES = "attributes";
	private static final String PANEL = "panel";

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

	@Override
	public NounMetadata execute() {
		// get inputs from pixel command
		this.numRules = getNumRules();
		this.confPer = getConf();
		this.minSupport = getMinSupport();
		this.maxSupport = getMaxSupport();
		this.attributesList = getAttributesList();
		if(attributesList.size() == 0) {
			String errorString = "No columns were passed as attributes for the association learning routine.";
			LOGGER.info(errorString);
			throw new IllegalArgumentException(errorString);
		}
		ITableDataFrame dataFrame = (ITableDataFrame) this.insight.getDataMaker();

		int numCols = this.attributesList.size();
		String[] retHeaders = new String[numCols];
		QueryStruct2 qs = new QueryStruct2();
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
				qsHead.setColumn(header);
				retHeaders[i] = header;
			}
			this.isNumeric[i] = dataFrame.isNumeric(header);
			qs.addSelector(qsHead);
		}

		// re-add frame filters
		qs.setFilters(dataFrame.getFrameFilters());

		int numRows = getNumRows(dataFrame, (QueryColumnSelector) qs.getSelectors().get(0));
		Iterator<IHeadersDataRow> it = dataFrame.query(qs);
		this.instancesData = WekaReactorHelper.genInstances(retHeaders, isNumeric, numRows);
		this.instancesData = WekaReactorHelper.fillInstances(this.instancesData, it, this.isNumeric);
		// associated learning only takes categorical values, so we need to
		// discretize all numerical fields
		this.instancesData = WekaReactorHelper.discretizeAllNumericField(this.instancesData);

		premises = new HashMap<Integer, Collection<Item>>();
		consequences = new HashMap<Integer, Collection<Item>>();
		counts = new HashMap<Integer, Integer>();
		confidenceIntervals = new HashMap<Integer, Double>();

		Apriori apriori = new Apriori();
		apriori.setNumRules(numRules);
		apriori.setMinMetric(confPer);
		apriori.setLowerBoundMinSupport(minSupport);
		apriori.setUpperBoundMinSupport(maxSupport);

		LOGGER.info("Running Apriori Algorithm...");
		try {
			apriori.buildAssociations(this.instancesData);
		} catch (Exception e) {
			e.printStackTrace();
		}
		LOGGER.info("Finished Running Algorithm...");

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

		LOGGER.info("Generating Decision Viz Data...");
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
		GenRowStruct columnGrs = this.store.getNoun(ATTRIBUTES);
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
		GenRowStruct columnGrs = this.store.getNoun(PANEL);
		if(columnGrs != null) {
			if(columnGrs.size() > 0) {
				return columnGrs.get(0).toString();
			}
		}
		return null;
	}

	private int getNumRows(ITableDataFrame frame, QueryColumnSelector predictorCol) {
		QueryStruct2 qs = new QueryStruct2();
		QueryMathSelector math = new QueryMathSelector();
		math.setInnerSelector(predictorCol);
		math.setMath(QueryAggregationEnum.COUNT);
		qs.addSelector(math);

		Iterator<IHeadersDataRow> countIt = frame.query(qs);
		while (countIt.hasNext()) {
			return ((Number) countIt.next().getValues()[0]).intValue();
		}
		return 0;
	}

}
