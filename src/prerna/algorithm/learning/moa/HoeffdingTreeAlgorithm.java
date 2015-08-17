package prerna.algorithm.learning.moa;

/**
 * This code creates a Hoeffding Decision Tree, which is a probabilistic decision tree especially for large datasets.
 * 
 * For a regular decision tree, we want to make sure that each new node we add is the best possible one. For this 
 * decision tree, we want to be pretty sure but not certain. How certain we are is largely a function of our
 * split confidence value, (our default is set to 90%), and the grace period value, which is the number of instances
 * to look at before we start to consider whether we should create a new branch.
 * 
 * For more information, look at:
 * http://sourceforge.net/projects/moa-datastream/files/documentation/StreamMining.pdf/download
 *  
 *  
 *  @author Jason Adleberg: jadleberg@deloitte.com
 *  @version 1.1
 *  @date 8/7/2015
 */

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import moa.classifiers.Classifier;
import moa.classifiers.trees.HoeffdingAdaptiveTree;
import moa.core.InstancesHeader;
import moa.options.Options;
import moa.streams.CachedInstancesStream;
import prerna.algorithm.api.IAnalyticRoutine;
import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.weka.WekaUtilityMethods;
import prerna.om.SEMOSSParam;
import weka.core.Instance;
import weka.core.Instances;

public class HoeffdingTreeAlgorithm implements IAnalyticRoutine {

	private static final String SPLIT_CONFIDENCE = "splitConfidence";
	private static final String GRACE_PERIOD = "gracePeriod";
	private static final String SKIP_ATTRIBUTES	= "skipAttributes";
	private static final String CLASSIFIER_INDEX = "classifierIndex";
	private static final String TIE_THRESHOLD = "tieThreshold";

	private static final int SPACE_INDENT = 3;

	private List<SEMOSSParam> options;

	private Options treeOptions;
	private int classifierIndex = -1;
	private List<String> skipAttributes;
	private String splitConfidence; 
	private String gracePeriod;
	private String tieThreshold;

	private double accuracy;
	private int numNodes;
	private int numLeaves;
	private StringBuilder treeAsSB;

	public HoeffdingTreeAlgorithm() {
		options = new ArrayList<SEMOSSParam>();

		SEMOSSParam p1 = new SEMOSSParam();
		p1.setName(CLASSIFIER_INDEX);
		options.add(0, p1);
		
		SEMOSSParam p2 = new SEMOSSParam();
		p2.setName(SPLIT_CONFIDENCE);
		options.add(1, p2);

		SEMOSSParam p3 = new SEMOSSParam();
		p3.setName(GRACE_PERIOD);
		options.add(2, p3);
		
		SEMOSSParam p4 = new SEMOSSParam();
		p4.setName(TIE_THRESHOLD);
		options.add(3, p4);

		SEMOSSParam p5 = new SEMOSSParam();
		p5.setName(SKIP_ATTRIBUTES);
		options.add(4, p5);
	}

	/**
	 * Create MOA instances from ITableDataFrame, and set algorithm's properties. 
	 * Then, send over to computeTree(instances).
	 * 
	 * @param data                  data, from BTree
	 * @return						null (for now).
	 */
	@SuppressWarnings("unchecked")
	@Override
	public ITableDataFrame runAlgorithm(ITableDataFrame... data) {

		ITableDataFrame dataFrame = data[0];
		int numInstances = dataFrame.getNumRows();

		// set options
		this.classifierIndex = (int) options.get(0).getSelected();
		double invConfidence = (double) options.get(1).getSelected();
		double gracePercentage = (double) options.get(2).getSelected();
		double tieThresholdPercent = (double) options.get(3).getSelected();
		this.skipAttributes = (List<String>) options.get(4).getSelected();

		// confidenceValue is a value between 0 and 1: .1 represents 90% confidence.
		// gracePeriod refers to the number of rows to look at before computing a tree. 
		// It should be whichever is less: 200, or 10% of the dataset.
		// MOA's method require that both are passed in as Strings.

		this.splitConfidence = Double.toString(1.0-invConfidence);
		int rowsToLookAt = (int)(gracePercentage * numInstances); 
		if (rowsToLookAt > 200) { 
			this.gracePeriod = "200"; 
		} else { 
			this.gracePeriod = Integer.toString(rowsToLookAt); 
		}
		this.tieThreshold = Double.toString(tieThresholdPercent);
		dataFrame.setColumnsToSkip(skipAttributes);
		// Create Instances and a datastream from our query: these are data objects that MOA requires.
		Instances instanceData = WekaUtilityMethods.createInstancesFromQuery("DataSet", dataFrame.getData(), dataFrame.getColumnHeaders(), classifierIndex);

		// compute Tree
		computeTree(instanceData);

		return null;
	}

	/**
	 * Create MOA instances from ITableDataFrame, and set algorithm's properties. 
	 * Then, send over to computeTree(instances).
	 * 
	 * @param data                  data, from BTree
	 * @return						null (for now).
	 */
	public void computeTree(Instances instanceData) {

		// create tree and stream
		Classifier learner = new HoeffdingAdaptiveTree();    
		this.treeOptions = learner.getOptions();

		int numInstances = instanceData.size();
		int numAttributes = instanceData.numAttributes();

		if(classifierIndex < 0 || classifierIndex >= numAttributes) {
			throw new IllegalArgumentException("Classification variable not found");
		}

		instanceData.setClassIndex(classifierIndex);
		CachedInstancesStream stream = new CachedInstancesStream(instanceData);

		// set static options for Hoeffding Tree.
		// the following are defaults and should not be changed.
		set("maxByteSize", "33554432");									  // max memory it can use
		set("numericEstimator", "GaussianNumericAttributeClassObserver"); // which numeric estimator to use
		set("nominalEstimator", "NominalAttributeClassObserver");		  // which nominal estimator to use
		set("memoryEstimatePeriod", "1000000");							  // instances to input before checking memory usage
		set("splitCriterion", "InfoGainSplitCriterion");				  // use entropy to determine if split 
		set("binarySplits", "false");									  // only use binary splits. non-binary splits appears to be broken. 
		set("stopMemManagement", "false");								  // dont stop when memory limit is hit
		set("removePoorAtts", "false");									  // dont remove any attributes
		set("noPrePrune", "false");										  // dont pre-prune anything
		set("leafprediction", "MC");							  		  // score tree normally [IMPORTANT]
		set("nbThreshold", "0");										  // threshold for naive bayes [not applicable]
		// these options are important and passed in by the user.
		set("splitConfidence", splitConfidence);						  // how confident we are that a leaf is the best one
		set("gracePeriod", gracePeriod);								  // how many instances to look at before training
		set("tieThreshold", tieThreshold);								  // threshold below which a split must break ties
		
		// set training dimension (assumed to be last column), and prepare stream for analysis.
		InstancesHeader metadata = new InstancesHeader(instanceData);
		learner.setModelContext(metadata);
		learner.prepareForUse();
		
		// build the tree
		int numSampleCorrect = 0;
		int numberSamples = 0;
		while (stream.hasMoreInstances() && numberSamples < numInstances) {
			Instance trainInst = stream.nextInstance();
			learner.trainOnInstance(trainInst);
			numberSamples++;
			if(learner.correctlyClassifies(trainInst)) {
				numSampleCorrect++;
			}
		}

		/*
		// reiterate through instances and score them on our tree
		stream = new CachedInstancesStream(instanceData);
		numberSamples = 0;
		int numSampleCorrect = 0;
		while (stream.hasMoreInstances() && numberSamples < numInstances) {
			Instance trainInst = stream.nextInstance();
			// we can use this to debug if it is not scoring correctly:
			if(learner.correctlyClassifies(trainInst)) {
				numSampleCorrect++;
			}
			numberSamples++;
		}
		*/

		// Output accuracy
		accuracy = 100.0 * (double)numSampleCorrect / (double)numberSamples;

		// Create string representation of tree
		treeAsSB = new StringBuilder();
		learner.getDescription(treeAsSB, 0);
	}

	@SuppressWarnings("rawtypes")
	public Map<String, Map> processTreeString() {
		// Output number of nodes
		String[] descriptionSplit = treeAsSB.toString().split("\n");
		String[] hardlyReadableTree = Arrays.copyOfRange(descriptionSplit, 11, descriptionSplit.length);
		String[] humanReadableTree = createHumanReadableTree(hardlyReadableTree);

		String nodes = descriptionSplit[3].replaceAll("tree[ ]*size[ ]*\\(nodes\\)[ ]*=\\s","").replaceAll("\\r","");
		String leaves = descriptionSplit[4].replaceAll("tree[ ]*size[ ]*\\(leaves\\)[ ]*=\\s","").replaceAll("\\r","");
		
		numNodes = Integer.parseInt(nodes);
		numLeaves = Integer.parseInt(leaves);

		return getDictionary(humanReadableTree);
	}

	/**
	 * Shorthand for setting MOA's Hoeffding Tree Options.
	 * 
	 * @param option				HTree option to change
	 * @param value					value to change to
	 */
	public void set(String option, String value) {
		this.treeOptions.getOption(option).setValueViaCLIString(value);
	}

	@Override
	public String getName() {
		return "MOA Hoeffding Tree";
	}

	public void setGracePd(String set) {
		this.gracePeriod = set;
	}

	public void setConfidence(String set) {
		this.splitConfidence = set;
	}

	public void setClassifierIndex(String set) {
		this.classifierIndex = Integer.parseInt(set);
	}
	
	public void setTieThreshold(String set) {
		this.tieThreshold = set;
	}

	@Override
	public String getResultDescription() {
		return null;
	}

	@Override
	public void setSelectedOptions(Map<String, Object> selected) {
		Set<String> keySet = selected.keySet();
		for(String key : keySet) {
			for(SEMOSSParam param : options) {
				if(param.getName().equals(key)){
					param.setSelected(selected.get(key));
					break;
				}
			}
		}
	}

	@Override
	public List<SEMOSSParam> getOptions() {
		return this.options;
	}

	@Override
	public String getDefaultViz() {
		return "prerna.ui.components.playsheets.MOAClassificationPlaySheet";
	}

	@Override
	public List<String> getChangedColumns() {
		return null;
	}

	@Override
	public Map<String, Object> getResultMetadata() {
		Map<String, Object> resultData = new HashMap<String, Object>();
		resultData.put("accuracy", accuracy);
		resultData.put("numNodes", numNodes);
		resultData.put("numLeaves", numLeaves);
		return resultData;
	}

	/**
	 * Currently, Moa's Hoeffding Tree code returns the tree as a string,
	 * formatted in a particular way. For a node at the end of a tree (a "leaf"), it displays
	 * something like this:
	 * 
	 * 		Leaf [class:Genre] = <class 1:Comedy-Musical> weights: {96|70|32|57|0}
	 * 
	 * We want to simplify this to:
	 * 
	 *      Genre = Comedy-Musical.
	 * 
	 * For a node in the middle of a tree, it displays something like this:
	 * 
	 * 		if [att 2:Studio] != {val 11:Focus}:
	 * 
	 * We want to simplify this to: 
	 * 
	 *      Studio != Focus.
	 * 
	 * In order to do this, we will use regex. Annotations are included at the end 
	 * of each line. Please e-mail jadleberg@deloitte.com if you have any questions!
	 */
	public String[] createHumanReadableTree(String[] strArray) {

		String[] returnArray = new String[strArray.length];

		for (int i=0;i<strArray.length;i++) {
			String s = strArray[i];

			// remove extra line
			s = s.replaceAll("\r","");
			// three space indent
			s = s.replaceAll("  ","   ");

			// if string looks like:
			// "if [att 2:Studio] != {val 11:Focus}:"
			if (s.contains("if")) {
				String removeString = "if[^:]*:";	 
				s = s.replaceAll(removeString, ""); // -cut-if [att 2:-cut-Studio] != {val 11:Focus}:
				removeString = "]";
				s = s.replaceAll(removeString, ""); // Studio-cut-]-cut- != {val 11:Focus}:
				// if attribute
				if (s.contains("{")) {
					removeString = "\\{[^:]*:";
					s = s.replaceAll(removeString, ""); // Studio != -cut-{val 11:-cut-Focus}:
					removeString = "}:";
					s = s.replaceAll(removeString, ""); // Studio != Focus -cut-}:-cut-
				}
				// if numerical
				else {
					removeString = ":";
					s = s.replaceAll(removeString, ""); // Studio != Focus -cut-}:-cut-
				}
			}

			else if (s.contains("Leaf")) {
				// if string looks like: 
				// "Leaf [class:Genre] = <class 1:Comedy-Musical> weights: {96|70|32|57|0}"
				String removeString = "Leaf[^:]*:";
				s = s.replaceAll(removeString, ""); // -cut-Leaf [class:-cut-Genre] = <class 1:Comedy-Musical> weights: {96|70|32|57|0}"
				removeString = "]";
				s = s.replaceAll(removeString, ""); // Genre-cut-]-cut = <class 1:Comedy-Musical> weights: {96|70|32|57|0}"
				removeString = "<[^:]*:";
				s = s.replaceAll(removeString, ""); // Genre = -cut-<class 1:-cut-Comedy-Musical> weights: {96|70|32|57|0}"
				removeString = ">.*";
				s = s.replaceAll(removeString, ""); // Genre = Comedy-Musical-cut-> weights: {96|70|32|57|0}-cut-"
			}

			else {
				System.out.println("We encountered a strange output from MOA's Hoeffding Tree:");
				System.out.println(s);
				System.out.println("Please e-mail jadleberg@deloitte.com!");
			}

			returnArray[i] = s;
		}
		return returnArray;
	}


	/**
	 * This method recursively converts a string representation of a tree into a hashtable
	 * of hashtables (or JSON) representation. Each key in the hashtable is a node in the tree,
	 * and an array of its children trees are stored in the values.
	 * 
	 * @param strArray				string representation of tree
	 * @return						hashtable representation of tree.
	 */
	public Map<String, Map> getDictionary(String[] strArray) {
		Map<String, Map> dictionary = new HashMap<String, Map>();

		for (int i = 0; i<strArray.length; i++) {
			String node = strArray[i];
			// get indentation level of node.
			// 3 spaces [SPACE_INDENT] correspond to one level, but this value could be changed.
			int indentLevel = (node.length() - node.replaceAll("^\\s+", "").length());
			// start by looking for nodes with indent 0 (root nodes) 
			if (indentLevel == 0) {
			// recursively add their children to the value
				dictionary.put(node.trim(), recurse(i, strArray, indentLevel));
			}
		}
		return dictionary;
		//return null;
	}

	/**
	 * Recursively construct a node's subtree.
	 * 
	 * @param i						current node being examined
	 * @param strArray				string representation of tree
	 * @param indentLevel			level of tree (3 spaces = one level)
	 * @return						hashtable representation of tree.
	 */
	public HashMap recurse(int i, String[] strArray, int indentLevel) {	
		//ArrayList<Hashtable> innerArray = new ArrayList<Hashtable>();
		HashMap<String, HashMap> innerDict = new HashMap<String, HashMap>();

		// get indentation levels of the entries listed under a node.
		for (int j = i; j<strArray.length; j++) {
			String innerNode = strArray[j];
			int innerLevel = (innerNode.length() - innerNode.replaceAll("^\\s+", "").length());
			if (innerLevel - indentLevel == SPACE_INDENT) {
				// if indentation level is one more, recursively add to dictionary.
				innerDict.put(innerNode.trim(), recurse(j, strArray, innerLevel));
			}
			// break recursion if entry is not more fully indented.
			if (innerLevel <= indentLevel && i != j)  {
				return innerDict; 
			};
		}
		// break recursion if at last row in string's representation.
		return innerDict;
	}
}