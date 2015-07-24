/**
 *  This code looks for a specified number of outliers within a categorical dataset.
 *  
 *  It is used strictly for categorical data, and can be thought of as LOF for
 *     categorical data.
 *     
 *  For more documentation about how it works, see the original paper here:
 *  http://arxiv.org/ftp/cs/papers/0503/0503081.pdf
 *  
 *  @author Jason Adleberg, Rishi Luthar, Maher Ashraf Khalil
 */
package prerna.algorithm.learning.unsupervised.outliers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.algorithm.api.IAnalyticRoutine;
import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.impl.ExactStringMatcher;
import prerna.ds.BTreeDataFrame;
import prerna.math.BarChart;
import prerna.math.StatisticsUtilityMethods;
import prerna.om.SEMOSSParam;
import prerna.util.ArrayUtilityMethods;

public class EntropyDensityStatistic implements IAnalyticRoutine {

	private static final String INSTANCE_INDEX = "instanceIndex";
	private static final String SKIP_ATTRIBUTES = "skipAttributes";

	private static final String BIN_COL_NAME_ADDED = "_BINS";
	
	private List<SEMOSSParam> options;
	private List<String> skipAttributes;

	private int instanceIndex;
	private String[] attributeNames;
	private ITableDataFrame dataFrame;

	private List<String> addedCols = new ArrayList<String>(); 
	private String outlierVal;
	
	public EntropyDensityStatistic() {
		options = new ArrayList<SEMOSSParam>();

		SEMOSSParam p1 = new SEMOSSParam();
		p1.setName(INSTANCE_INDEX);
		options.add(0, p1);

		SEMOSSParam p2 = new SEMOSSParam();
		p2.setName(SKIP_ATTRIBUTES);
		options.add(1, p1);
	}

	@Override
	public ITableDataFrame runAlgorithm(ITableDataFrame... data) {
		this.instanceIndex = 0; //(int) options.get(0).getSelected();
		this.skipAttributes = new ArrayList<String>(); //(List<String>) options.get(1).getSelected();
		this.dataFrame = data[0];
		this.attributeNames = dataFrame.getColumnHeaders();

		// double check that all info in dataTable is categorical
		boolean[] isNumeric = dataFrame.isNumeric();
		for(int i = 0; i < isNumeric.length; i++) {
			if(i == instanceIndex) {
				continue;
			}
			if (isNumeric[i]) {
				String[] names = new String[2];
				names[0] = attributeNames[instanceIndex];
				names[1] = attributeNames[i] + BIN_COL_NAME_ADDED;
				
				//search if binning has already occurred and in data frame
				if(ArrayUtilityMethods.arrayContainsValue(attributeNames, names[1])) {
					continue; // already created bin
				}
				
				int numInstances = dataFrame.getNumRows();
				Object[] instances = new Object[numInstances];
				Double[] values = new Double[numInstances];
				Iterator<Object[]> it = dataFrame.iterator(false);
				int counter = 0;
				while(it.hasNext()) {
					Object[] row = it.next();
					instances[counter] = row[instanceIndex];
					try {
						values[counter] = ((Number) row[i]).doubleValue();
					} catch (ClassCastException e) {
						values[counter] = null;
					}
					counter++;
				}
				
				BarChart chart = new BarChart(values);
				String[] binValues = chart.getAssignmentForEachObject();
				ITableDataFrame table = new BTreeDataFrame(names);
				Map<String, Object> addBinValues = new HashMap<String, Object>();
				for(int j = 0; j < values.length; j++) {
					addBinValues.put(names[0], instances[j]);
					addBinValues.put(names[1], binValues[j]);
					table.addRow(addBinValues, addBinValues);
				}
				addedCols.add(names[1]);
				
				this.dataFrame.join(table, attributeNames[instanceIndex], attributeNames[instanceIndex], 1.0, new ExactStringMatcher() );
				skipAttributes.add(attributeNames[i]);

				if(i < instanceIndex) {
					instanceIndex--;
				}
			}
		}
		dataFrame.setColumnsToSkip(skipAttributes);
		attributeNames = dataFrame.getColumnHeaders();

		Map<String, Map<String, Integer>> frequencyCounts = dataFrame.getUniqueColumnValuesAndCount();
		Map<Object, Double> results = score(frequencyCounts);

		String attributeName = attributeNames[instanceIndex];
		// to avoid adding columns with same name
		int counter = 0;
		this.outlierVal = attributeName + "_EntropyBasedOutlier_" + counter;
		while(ArrayUtilityMethods.arrayContainsValue(attributeNames, outlierVal)) {
			counter++;
			this.outlierVal = attributeName + "_CLUSTER_" + counter;
		}
		ITableDataFrame returnTable = new BTreeDataFrame(new String[]{attributeName, outlierVal});
		for(Object instance : results.keySet()) {
			Map<String, Object> row = new HashMap<String, Object>();
			row.put(attributeName, instance);
			row.put(outlierVal, results.get(instance));
			returnTable.addRow(row, row);
		}
		
		return returnTable;
	}


	public Map<Object, Double> score(Map<String, Map<String, Integer>> frequencyCounts) {
		Map<Object, Double> retHash = new HashMap<Object, Double>();
		
		Iterator<List<Object[]>> it = dataFrame.uniqueIterator(attributeNames[instanceIndex], false);
		int numInstances = dataFrame.getNumRows();
		while(it.hasNext()) {
			List<Object[]> instance = it.next();
			Object instanceName = instance.get(0)[instanceIndex];
			double entropyVal = 0;
			for(int i = 0; i < attributeNames.length; i++) {
				if(i == instanceIndex) {
					continue;
				}
				Map<String, Integer> attributeCounts = frequencyCounts.get(attributeNames[i]);
				int numUniqueVals = attributeCounts.values().size();
				double averageProb = 0;
				for(int j = 0; j < instance.size(); j++) {
					Object[] instanceRow = instance.get(j);
					int occurance = attributeCounts.get(instanceRow[i]);
					averageProb += (double) occurance / numInstances;
				}
				averageProb /= instance.size();
				double rowEntropy = -1.0 * StatisticsUtilityMethods.logBase2(averageProb) / StatisticsUtilityMethods.logBase2(numUniqueVals);
				entropyVal += rowEntropy;
			}
			
			retHash.put(instanceName, entropyVal);
		}
		
		return retHash;
	}

	@Override
	public String getName() {
		return "Entropy Density Outlier Statistic";
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
		addedCols.add(this.outlierVal);
		return addedCols;
	}

	@Override
	public Map<String, Object> getResultMetadata() {
		// TODO Auto-generated method stub
		return null;
	}
}
