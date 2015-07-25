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

import prerna.algorithm.api.IAnalyticRoutine;
import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.util.DuplicationReconciliation;
import prerna.algorithm.learning.util.DuplicationReconciliation.ReconciliationMode;
import prerna.algorithm.learning.util.InstanceSimilarity;
import prerna.ds.BTreeDataFrame;
import prerna.om.SEMOSSParam;

public class FastOutlierDetection implements IAnalyticRoutine {

	private static final String NUM_SAMPLE_SIZE = "numSampleSize";
	private static final String INSTANCE_INDEX = "instanceIndex";
	private static final String SKIP_ATTRIBUTES	= "skipAttributes";
	private static final String DUPLICAITON_RECONCILIATION	= "dupReconciliation";

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

	public FastOutlierDetection() {
		options = new ArrayList<SEMOSSParam>();

		SEMOSSParam p1 = new SEMOSSParam();
		p1.setName(INSTANCE_INDEX);
		options.add(0, p1);

		SEMOSSParam p2 = new SEMOSSParam();
		p2.setName(SKIP_ATTRIBUTES);
		options.add(1, p2);

		SEMOSSParam p3 = new SEMOSSParam();
		p3.setName(NUM_SAMPLE_SIZE);
		options.add(2, p3);

		SEMOSSParam p4 = new SEMOSSParam();
		p4.setName(DUPLICAITON_RECONCILIATION);
		options.add(3, p4);
	}

	/**
	 * Reformat data, initialize a LOF object and then calculate a list of LOF scores
	 * for each point in dataset by calling score(k).
	 * @param data                  data, from BTree
	 * @return						double array of LOF scores for each point in array
	 */
	@Override
	public ITableDataFrame runAlgorithm(ITableDataFrame... data) {
		this.instanceIndex = 0;//(int) options.get(0).getSelected();
		this.numSubsetSize = 10;//(int) options.get(1).getSelected();
		this.skipAttributes = null;//(List<String>) options.get(2).getSelected();
		this.dups = (Map<String, DuplicationReconciliation>) options.get(2).getSelected();

		// get number of rows and cols
		this.dataFrame = data[0];
		this.attributeNames = dataFrame.getColumnHeaders();

		int numInstances = dataFrame.getUniqueInstanceCount(attributeNames[instanceIndex]); 
		boolean[] isNumeric = dataFrame.isNumeric();
		if(skipAttributes == null) {
			skipAttributes = new ArrayList<String>();
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
		// grab R random rows
		Iterator<List<Object[]>> it = dataFrame.scaledUniqueIterator(attributeNames[instanceIndex], false);
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
		it = dataFrame.scaledUniqueIterator(attributeNames[instanceIndex], false);
		while(it.hasNext()) {
			List<Object[]> instance = it.next();
			Object instanceName = instance.get(0)[instanceIndex];
			double minSim = 2;
			for(int i= 0; i < numSubsetSize; i++) {
				List<Object[]> subsetInstance = rSubset.get(i);
				if(subsetInstance.get(0)[instanceIndex].equals(instance.get(0)[instanceIndex])) {
					continue;
				}
				double sim = InstanceSimilarity.getInstanceSimilarity(instance, subsetInstance, isNumeric, attributeNames, dups);
				if(minSim > sim) {
					minSim = sim;
				}
			}
			results.put(instanceName, minSim);
		}

		String attributeName = attributeNames[instanceIndex];
		this.changedColumn = attributeName + "_FastOutlier";
		ITableDataFrame returnTable = new BTreeDataFrame(new String[]{attributeName, changedColumn});
		for(Object instance : results.keySet()) {
			Map<String, Object> row = new HashMap<String, Object>();
			row.put(attributeName, instance);
			row.put(changedColumn, results.get(instance));
			returnTable.addRow(row, row);
		}

		System.out.println(results);
		return returnTable;
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
