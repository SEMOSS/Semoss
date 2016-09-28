/**
 *  This code computes a Local Outlier Factor score for each point in the dataset.
 *  This score refers to how much a point deviates from others in the dataset,
 *     and can be a function of as many dimensions as is desired.
 *     
 *  Technically, this goes a step further, and computes a LOoP, or probability of 
 *  Local Outlier, which is just a measure between 0 and 1 of how much something is
 *  an outlier. 0 is in a cluster, and 1 is an outlier.
 *     
 *  For more documentation about how it works, see the original paper here:
 *  http://www.dbs.ifi.lmu.de/Publikationen/Papers/LOF.pdf
 *  
 *  This algorithm uses a KD-Tree to speed up computations.
 *  
 *  
 *  @author Jason Adleberg, Rishi Luthar, Maher Ashraf Khalil
 */

package prerna.algorithm.learning.unsupervised.outliers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import prerna.algorithm.api.IAnalyticTransformationRoutine;
import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.util.DuplicationReconciliation;
import prerna.algorithm.learning.util.DuplicationReconciliation.ReconciliationMode;
import prerna.algorithm.learning.util.InstanceSimilarity;
import prerna.om.SEMOSSParam;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;

public class FastOutlierDetection implements IAnalyticTransformationRoutine {

	public static final String NUM_SAMPLE_SIZE = "numSampleSize";
	public static final String INSTANCE_INDEX = "instanceIndex";
	public static final String SKIP_ATTRIBUTES	= "skipAttributes";
	public static final String NUMBER_OF_RUNS	= "numRuns";
	public static final String DUPLICATION_RECONCILIATION	= "dupReconciliation";

	private static Random random = new Random();

	private List<SEMOSSParam> options;

	private ITableDataFrame dataFrame;
	private String[] attributeNames;

	private int instanceIndex;
	private Map<String, DuplicationReconciliation> dups;

	private String changedColumn;
	private List<String> skipAttributes;

	private int numSubsetSize;                  // How many neighbors to examine?
	private HashMap<Object, Double> results;    // This stores the FastDistance for each point in dataset
	int numRuns;								// number of Runs to make

	public FastOutlierDetection() {
		options = new ArrayList<SEMOSSParam>();

		SEMOSSParam p1 = new SEMOSSParam();
		p1.setName(INSTANCE_INDEX);
		options.add(0, p1);

		SEMOSSParam p2 = new SEMOSSParam();
		p2.setName(NUM_SAMPLE_SIZE);
		options.add(1, p2);
		
		SEMOSSParam p3 = new SEMOSSParam();
		p3.setName(NUMBER_OF_RUNS);
		options.add(2, p3);
		
		SEMOSSParam p4 = new SEMOSSParam();
		p4.setName(SKIP_ATTRIBUTES);
		options.add(3, p4);

		SEMOSSParam p5 = new SEMOSSParam();
		p5.setName(DUPLICATION_RECONCILIATION);
		options.add(4, p5);
	}

	/**
	 * Reformat data, initialize a LOF object and then calculate a list of LOF scores
	 * for each point in dataset by calling score(k).
	 * @param data                  data, from BTree
	 * @return						double array of LOF scores for each point in array
	 */
	@Override
	public ITableDataFrame runAlgorithm(ITableDataFrame... data) {
		this.instanceIndex = ((Number) options.get(0).getSelected()).intValue();
		this.numSubsetSize = ((Number) options.get(1).getSelected()).intValue();
		this.numRuns = ((Number) options.get(2).getSelected()).intValue();
		this.skipAttributes = (List<String>) options.get(3).getSelected();
		this.dups = (Map<String, DuplicationReconciliation>) options.get(4).getSelected();

		// get number of rows and cols
		this.dataFrame = data[0];
		this.attributeNames = dataFrame.getColumnHeaders();

		int numInstances = dataFrame.getUniqueInstanceCount(attributeNames[instanceIndex]); 
		boolean[] isNumeric = dataFrame.isNumeric();
		if(skipAttributes == null) {
			skipAttributes = new ArrayList<String>();
		}
		
		if(numSubsetSize > numInstances) {
			throw new IllegalArgumentException("Subset size is larger than the number of instances.");
		}
		
		if(dups == null) {
			dups = new HashMap<String, DuplicationReconciliation>();
			for(int i = 0; i < attributeNames.length; i++) {
				if(isNumeric[i])
					dups.put(attributeNames[i], new DuplicationReconciliation(ReconciliationMode.MEAN));
			}
		}

		// make sure numSampleSize isn't too big.
		if (numSubsetSize > numInstances/3) {
			System.out.println("R is too big!");
			numSubsetSize = numInstances/3;
		}

		int random_skip = numInstances/numSubsetSize; // won't be smaller than 3.

		results = new HashMap<Object, Double>();
		
		for (int k = 0; k < numRuns; k++) {
			// grab R random rows
			Iterator<List<Object[]>> it = dataFrame.scaledUniqueIterator(attributeNames[instanceIndex], null);
			List<List<Object[]>> rSubset = new ArrayList<List<Object[]>>();
			for (int i = 0; i < numSubsetSize; i++) {
				// skip over a number between 0 and random_skip rows
				int skip = random.nextInt(random_skip);
				for (int j = 0; j < skip - 1; j++) {
					it.next();
				}
				rSubset.add(it.next());
			}
	
			// for row in dataTable, grab R random rows
			it = dataFrame.scaledUniqueIterator(attributeNames[instanceIndex], null);
			while(it.hasNext()) {
				List<Object[]> instance = it.next();
				Object instanceName = instance.get(0)[instanceIndex];
				double maxSim = 0;
				for(int i= 0; i < numSubsetSize; i++) {
					List<Object[]> subsetInstance = rSubset.get(i);
					if(subsetInstance.get(0)[instanceIndex].equals(instance.get(0)[instanceIndex])) {
						continue;
					}
					double sim = InstanceSimilarity.getInstanceSimilarity(instance, subsetInstance, isNumeric, attributeNames, dups);
					if(maxSim < sim) {
						maxSim = sim;
					}
				}
				if (results.get(instanceName) == null) {
					results.put(instanceName, maxSim);
				}
				else {
					double oldVal = results.get(instanceName);
					results.put(instanceName, oldVal+maxSim);
				}
			}
		}

		String attributeName = attributeNames[instanceIndex];
		// to avoid adding columns with same name
		int counter = 0;
		this.changedColumn = attributeName + "_FastOutlier_" + counter;
		while(ArrayUtilityMethods.arrayContainsValue(attributeNames, changedColumn)) {
			counter++;
			this.changedColumn = attributeName + "_FastOutlier_" + counter;
		}

		Map<String, String> dataType = new HashMap<String, String>();
		dataType.put(changedColumn, "DOUBLE");
		dataFrame.connectTypes(attributeName, changedColumn, dataType);
		for(Object instance : results.keySet()) {
			Double val = (numRuns-results.get(instance))/numRuns;

			Map<String, Object> clean = new HashMap<String, Object>();
			if(instance.toString().startsWith("http://semoss.org/ontologies/Concept/")) {
				instance = Utility.getInstanceName(instance.toString());
			}
			clean.put(attributeName, instance);
			clean.put(changedColumn, val);

			dataFrame.addRelationship(clean);
		}

		return null;
	}

	public HashMap<Object, Double> getResults() {
		return results;
	}

	@Override
	public String getName() {
		return "Local Outlier Factor Algorithm";
	}

	@Override
	public String getResultDescription() {
		// TODO Auto-generated method stub
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
		return "prerna.ui.components.playsheets.LocalOutlierVizPlaySheet";
	}

	@Override
	public List<String> getChangedColumns() {
		List<String> changedCols = new ArrayList<String>();
		changedCols.add(changedColumn);
		return changedCols;
	}

	@Override
	public Map<String, Object> getResultMetadata() {
		// TODO Auto-generated method stub
		return null;
	}

}
