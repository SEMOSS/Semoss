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
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math3.special.Erf;

import prerna.algorithm.api.IAnalyticRoutine;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.BTreeDataFrame;
import prerna.math.StatisticsUtilityMethods;
import prerna.om.SEMOSSParam;

public class LOF implements IAnalyticRoutine {

	private static final String K_NEIGHBORS = "k";
	private static final String INSTANCE_INDEX = "instanceIndex";
	private static final String SKIP_ATTRIBUTES	= "skipAttributes";

	private List<SEMOSSParam> options;

	private ITableDataFrame dataFrame;
	private String[] attributeNames;

	private int instanceIndex;
	private int numInstances; 
	private	int dimensions;

	private String changedColumn;

	private List<String> skipAttributes;

	private Object[] index;
	private int k;                    // How many neighbors to examine?
	public KDTree Tree;               // Points in dataset are put into KDTree to help find nearest neighbors
	private double[][] reachDistance; // This stores the reachDistance between 2 points. Not symmetrical! 
	private double[] kDistance;       // This stores the k-Distance for each point in dataset
	private double[] LRD;             // This stores the LRD for each point in dataset
	private double[] LOF;             // This stores the LOF for each point in dataset
	private double[] LOP;             // This stores the LOP for each point in dataset
	private double[][] dataFormatted; // This stores the formatted data from dataTable

	public LOF() {
		options = new ArrayList<SEMOSSParam>();

		SEMOSSParam p1 = new SEMOSSParam();
		p1.setName(INSTANCE_INDEX);
		options.add(0, p1);

		SEMOSSParam p2 = new SEMOSSParam();
		p2.setName(K_NEIGHBORS);
		options.add(1, p2);

		SEMOSSParam p3 = new SEMOSSParam();
		p3.setName(SKIP_ATTRIBUTES);
		options.add(2, p3);
	}

	/**
	 * Reformat data, initialize a LOF object and then calculate a list of LOF scores
	 * for each point in dataset by calling score(k).
	 * @param data                  data, from BTree
	 * @return						double array of LOF scores for each point in array
	 */
	@Override
	public ITableDataFrame runAlgorithm(ITableDataFrame... data) {
		this.instanceIndex = (int) options.get(0).getSelected();
		this.k = (int) options.get(1).getSelected();
		this.skipAttributes = (List<String>) options.get(2).getSelected();

		// get number of rows and cols
		this.dataFrame = data[0];
		this.dataFrame.setColumnsToSkip(skipAttributes);
		this.attributeNames = dataFrame.getColumnHeaders();
		this.numInstances = dataFrame.getUniqueInstanceCount(attributeNames[instanceIndex]); 
		if(skipAttributes == null) {
			skipAttributes = new ArrayList<String>();
		}
		this.dimensions = dataFrame.getNumCols() - 1;// - skipAttributes.size();

		// double check that all info is numerical
		boolean[] isNumeric = dataFrame.isNumeric();
		for(int i = 0; i < isNumeric.length; i++) {
			if(i != instanceIndex && !isNumeric[i] && !skipAttributes.contains(attributeNames[i])) {
				throw new IllegalArgumentException("All columns must be numbers! \n"
						+ "Column "+ attributeNames[i] + " is not all numbers!");
			};
		}

		// Initialize arrays
		kDistance = new double[numInstances];
		LRD = new double[numInstances];
		LOF = new double[numInstances];
		LOP = new double[numInstances];
		reachDistance = new double[numInstances][numInstances];
		index = new Object[numInstances];
		dataFormatted = new double[numInstances][dimensions];

		// This code flattens out instances, incase there are repeat appearances of an identifier
		Iterator<List<Object[]>> it = dataFrame.scaledUniqueIterator(attributeNames[instanceIndex], false);
		int counter = 0;
		while(it.hasNext()) {
			List<Object[]> instance = it.next();
			double[] instanceValues = new double[dimensions];
			int already_crossed = 0;
			for(int i = 0; i < instance.size(); i++) {
				Object[] row = instance.get(i);
				for(int j = 0; j < dimensions+1; j++) {
					if (j != instanceIndex && !row.toString().isEmpty())
						instanceValues[j-already_crossed] += ((Number) row[j]).doubleValue();
					else already_crossed = 1;
				}
			}
			dataFormatted[counter] = instanceValues;
			index[counter] = instance.get(0)[instanceIndex];
			counter++;
		}

		// add to tree!
		this.Tree = new KDTree(dimensions);
		for (int i = 0; i < numInstances; i++) {
			Tree.add(dataFormatted[i], i); 
		}
		
		// run scoring algorithm
		Hashtable<Object, Double> results = score(k);

		String attributeName = attributeNames[instanceIndex];
		this.changedColumn = attributeName + "_LOP";
		ITableDataFrame returnTable = new BTreeDataFrame(new String[]{attributeName, changedColumn});
		for(Object instance : results.keySet()) {
			Map<String, Object> row = new HashMap<String, Object>();
			row.put(attributeName, instance);
			row.put(changedColumn, results.get(instance));
			returnTable.addRow(row, row);
		}

		return returnTable;
	}

	/**
	 * Calculate a list of LOP scores for each point in dataset
	 * @param k                     how many neighbors to examine
	 * @return						hashtable of Title and LOP scores
	 */
	public Hashtable<Object, Double> score(int k) {
		// 1. Get the K-distance for each point in our array 
		//    (using KDTree's nearest k-neighbors method)
		for (int i=0; i<numInstances; i++) {
			double kdistance = 0;
			// returns IDs for each of k nearest neighbors
			Object[] neighborIDs = Tree.getNearestNeighbors(dataFormatted[i], k).returnData();
			// calculate distances to nearest neighbors, and avg them
			for (int j=0; j<neighborIDs.length; j++)
				kdistance += dist(dataFormatted[i], dataFormatted[(int)neighborIDs[j]]);

			kDistance[i] = kdistance/k;
		}

		// 2. Fill out ReachDistance[][]. 
		// ReachDistance from a->b is defined as the maximum of:
		//     A. Point a's kdistance OR
		//     B. The actual distance from a->b.
		for (int i=0; i<numInstances; i++) {
			double kdistance = kDistance[i];
			for (int j=0; j<this.numInstances; j++) {
				double distance = dist(dataFormatted[i], dataFormatted[j]);
				if (kdistance > distance) {this.reachDistance[i][j] = kdistance;}
				else                      {this.reachDistance[i][j] = distance; };
			}
		}

		// 3. Fill out LRD array. 
		//    For a point A, LRD equals k divided by the sum of kdistances of point A's nearest k neighbors.
		//    

		for (int i=0; i<this.numInstances; i++) {
			// grab k nearest neighbors
			Object[] neighborIDs = Tree.getNearestNeighbors(dataFormatted[i], k).returnData();
			// sum up reachDistance for each of them
			double sum_reachDistance = 0;
			for (int j=0; j<neighborIDs.length; j++) {
				sum_reachDistance += reachDistance[i][(int)neighborIDs[j]];
			}

			LRD[i] = k/sum_reachDistance;
		}

		// 4. Fill out LOF array.
		//    For a point A, LOF equals the average LRD value of A's k nearest neighbors,
		//    divided by A's LRD value.
		for (int i=0; i<this.numInstances; i++) {
			// grab k nearest neighbors
			Object[] neighborIDs = Tree.getNearestNeighbors(dataFormatted[i], k).returnData();
			// for each, sum up the ratio of LRD of the point to LRD of the neighbor
			double sum_ratioLRDs = 0;
			for (int j=0; j<neighborIDs.length; j++) {
				sum_ratioLRDs += LRD[(int)neighborIDs[j]]/LRD[i];
			}
			// LRD = k/sum_reachDistance
			LOF[i] = sum_ratioLRDs/k;
		}

		// 5. Calculate LOP
		//    This transforms a LOF to a value between 0 and 1.
		//    See here for more information: http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.439.2035&rep=rep1&type=pdf
		double[] ploof = new double[numInstances];
		for (int i=0; i<this.numInstances; i++)
			ploof[i] = LOF[i] - 1;

		double stdev = StatisticsUtilityMethods.getSampleStandardDeviationIgnoringInfinity(ploof);
		double squareRoot2 = Math.sqrt(2);
		if (stdev == 0) {
			for (int i = 0; i < this.numInstances; i++) {
				if (Double.isInfinite(LOF[i])) { 
					LOP[i] = 1; 
				} else { 
					LOP[i] = 0; 
				}
			}
		} else {
			for(int i = 0; i < numInstances; i++) {
				LOP[i] = Math.max(0, Erf.erf(ploof[i] / (stdev * squareRoot2)));
			}
		}

		// 6. Insert all LOP values into a hashtable, with the unique ID as the ID.
		Hashtable<Object, Double> results = new Hashtable<Object, Double>();
		for (int i=0; i<this.numInstances; i++) {
			results.put(index[i], LOP[i]);
		}

		return results;
	}

	/**
	 * Calculate distance between two n-dimensional coordinates.
	 * 
	 * @param p1, p2                two points in n dimensions.
	 * @return						the scalar distance between them.
	 */
	private static final double dist(double[] p1, double[] p2) {
		double d = 0;
		double q = 0;
		for (int i = 0; i < p1.length; ++i) {
			d += (q=(p1[i] - p2[i]))*q;
		}
		return Math.sqrt(d);
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

	public double[][] getreach() {
		return reachDistance;
	}

}
