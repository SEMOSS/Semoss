package prerna.ui.components.playsheets;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.impl.ArrayUtilityMethods;
import prerna.algorithm.impl.ClusteringAlgorithm;
import prerna.algorithm.impl.StatisticsUtilityMethods;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.GridFilterData;
import prerna.ui.components.GridTableModel;
import prerna.ui.components.GridTableRowSorter;
import prerna.ui.components.NewScrollBarUI;
import prerna.util.Constants;
import prerna.util.DIHelper;

@SuppressWarnings("serial")
public class ClusteringVizPlaySheet extends BrowserPlaySheet{

	private static final Logger logger = LogManager.getLogger(ClusteringVizPlaySheet.class.getName());
	private int numClusters;
	private ArrayList<Object[]> clusterInfo;

	//indexing used for bar graph visualizations
	private int[] numericalPropIndices;
	private Integer[] categoryPropIndices; 

	public ClusteringVizPlaySheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/cluster.html";
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void createView() {
		super.createView();
		JPanel panel = new JPanel();
		table = new JTable();
		panel.add(table);
		GridBagLayout gbl_mainPanel = new GridBagLayout();
		gbl_mainPanel.columnWidths = new int[]{0, 0};
		gbl_mainPanel.rowHeights = new int[]{0, 0};
		gbl_mainPanel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_mainPanel.rowWeights = new double[]{1.0, Double.MIN_VALUE};
		panel.setLayout(gbl_mainPanel);
		addScrollPanel(panel);
		GridFilterData gfd = new GridFilterData();
		list.addAll(0, clusterInfo);
		gfd.setColumnNames(names);
		//append cluster information to list data
		gfd.setDataList(list);
		GridTableModel model = new GridTableModel(gfd);
		table.setModel(model);
		table.setRowSorter(new GridTableRowSorter(model));

		jTab.addTab("Raw Data", panel);
	}


	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public Hashtable processQueryData()
	{
		Hashtable allHash = new Hashtable();
		ArrayList<Hashtable<String, Object>> dataList = new ArrayList<Hashtable<String, Object>>(list.size());
		ArrayList<Hashtable<String, Object[]>> clusterInformation = new ArrayList<Hashtable<String, Object[]>>(numClusters);
		//initialize cluster information
		for(int i = 0; i < numClusters; i++) {
			Hashtable<String, Object[]> innerHash = new Hashtable<String, Object[]>();
			clusterInformation.add(innerHash);
		}

		for(Object[] dataRow : list) {
			Hashtable<String, Object> instanceHash = new Hashtable<String, Object>();
			//add name and cluster under special names first
			int clusterID = (int) dataRow[dataRow.length - 1];
			instanceHash.put("ClusterID", clusterID);
			instanceHash.put("NodeName", dataRow[0]);
			Hashtable<String,Object[]> clusterHash = clusterInformation.get(clusterID);
			//loop through properties and add to innerHash
			for(int i = 1; i < dataRow.length - 1; i++) {
				Object value = dataRow[i];
				String propName = names[i];
				instanceHash.put(propName, value);
				// add properties to cluster hash
				updateClusterHash(clusterHash, propName, value);
			}
			dataList.add(instanceHash);
		}
		
		Hashtable<String, Hashtable<String, Object>>[] barData = new Hashtable[numClusters];
		for(int i = 0; i < numClusters; i++) {
			Hashtable<String, Object[]> allClusterInfo = clusterInformation.get(i);
			Hashtable<String, Hashtable<String, Object>> clusterData = new Hashtable<String, Hashtable<String, Object>>(allClusterInfo.keySet().size());
			for(String propName : allClusterInfo.keySet()) {
				int idx = ArrayUtilityMethods.calculateIndexOfArray(names, propName);
				Object[] values = allClusterInfo.get(propName);
				values = ArrayUtilityMethods.removeAllNulls(values);
				if(values != null) {
					if (ArrayUtilityMethods.arrayContainsValue(numericalPropIndices, idx) & values.length > 5) {					
						// dealing with numerical prop - determine range, calculate IQR, determine bin-size, group
						Arrays.sort(values);
						double[] numValues = ArrayUtilityMethods.convertObjArrToDoubleArr(values);
						Hashtable<String, Object>[] propBins = calculateNumericBins(numValues);
						Hashtable<String, Object> innerHash = new Hashtable<String, Object>();
						// cause JS is dumb
						Object[] propBinsArr = new Object[]{propBins};
						innerHash.put("dataSeries", propBinsArr);
						innerHash.put("names", new String[]{propName, "Distribution"});
						clusterData.put(propName, innerHash);
					} else {
						String[] stringValues = ArrayUtilityMethods.convertObjArrToStringArr(values);
						String[] uniqueValues = ArrayUtilityMethods.getUniqueArray(stringValues);
						Hashtable<String, Object>[] propBins = calculateCategoricalBins(stringValues, uniqueValues);
						Hashtable<String, Object> innerHash = new Hashtable<String, Object>();
						// cause JS is dumb
						Object[] propBinsArr = new Object[]{propBins};
						innerHash.put("dataSeries", propBinsArr);
						innerHash.put("names", new String[]{propName, "Frequency"});
						// need to create outerHash since bar chart takes in weird format - since it is set up to conver to stacked bar chart
						clusterData.put(propName, innerHash);
					}
				}
			}
			barData[i] = clusterData;
		}
		allHash.put("dataSeries", dataList);
		allHash.put("barData", barData);
		
		return allHash;
	}

	@SuppressWarnings("unchecked")
	private Hashtable<String, Object>[] calculateCategoricalBins(String[] values, String[] uniqueValues) {
		int numOccurrences = values.length;
		int uniqueSize = uniqueValues.length;
		int[] uniqueCounts = new int[uniqueSize];
		Hashtable<String, Object>[] retBins = new Hashtable[uniqueSize];
		// find counts for unique variables
		int i;
		for(i = 0; i < numOccurrences; i++) {
			INNER : for(int j = 0; j < uniqueSize; j++) {
				if(uniqueValues[j].equals(values[i])) {
					uniqueCounts[j]++;
					break INNER;
				}
			}
		}
		// sort the unique counts from smallest to largest
		for(i = 0; i < uniqueSize - 1; i++) {
			int j;
			// if first index is larger than second index, switch positions in both count and name arrays
			for(j = i+1; j < uniqueSize; j++) {
				if(uniqueCounts[i] > uniqueCounts[j]) {
					int largerVal = uniqueCounts[i];
					int smallerVal = uniqueCounts[j];
					String largerPropName = uniqueValues[i];
					String smallerPropName = uniqueValues[j];
					uniqueCounts[j] = largerVal;
					uniqueCounts[i] = smallerVal;
					uniqueValues[j] = largerPropName;
					uniqueValues[i] = smallerPropName;
				}
			}
		}		
		// order the values to look the normal distribution
		String[] sortedValues = new String[uniqueSize];
		int[] sortedCounts = new int[uniqueSize];
		int center = (int) Math.ceil(uniqueSize / 2.0);
		int sgn;
		for(i = 1, sgn = -1; i <= uniqueSize; i++, sgn *= -1) {
			sortedCounts[center - 1 + (sgn*i/2)] = uniqueCounts[uniqueSize - i];
			sortedValues[center - 1 + (sgn*i/2)] = uniqueValues[uniqueSize - i];
		}
		for(i = 0; i < uniqueSize; i++) {
			Hashtable<String, Object> innerHash = new Hashtable<String, Object>();
			innerHash.put("seriesName", "Frequency");
			innerHash.put("y0", "0");
			innerHash.put("x", sortedValues[i]);
			innerHash.put("y", sortedCounts[i]);
			retBins[i] = innerHash;
		}
		return retBins;
	}

	@SuppressWarnings("unchecked")
	public Hashtable<String, Object>[] calculateNumericBins(double[] numValues) {
		int numOccurances = numValues.length;
		double min = numValues[0];
		double max = numValues[numOccurances -1];
		double range = max - min;
		double IQR = StatisticsUtilityMethods.quartile(numValues, 75, true) - StatisticsUtilityMethods.quartile(numValues, 25, true);
		double binSize = 2 * IQR * Math.pow(numOccurances, -1.0/3.0);
		int numBins = (int) Math.ceil(range/binSize);
		Hashtable<String, Object>[] retBins = new Hashtable[numBins];

		int i;
		int currBin = 0;
		int counter = 0;
		for(i = 0; i < numOccurances; i++) {
			if(numValues[i] > min+(binSize*currBin)) {
				Hashtable<String, Object> innerHash = new Hashtable<String, Object>();
				innerHash.put("seriesName", "Distribution");
				innerHash.put("y0", "0");
				innerHash.put("x", (min + currBin*binSize) + "-" + (min + (currBin+1) * binSize));
				innerHash.put("y", counter);
				retBins[currBin] = innerHash;
				currBin++;
				counter = 0;
			}
			counter++;
		}
		//trim in case last hashtable in array is null
		return (Hashtable<String, Object>[]) ArrayUtilityMethods.trimEmptyValues(retBins);
	}

	public void updateClusterHash(Hashtable<String, Object[]> clusterHash, String propName, Object value) {
		Object[] allValuesOfPropInCluster;
		if(!clusterHash.containsKey(propName)) {
			allValuesOfPropInCluster = new Object[10];
			allValuesOfPropInCluster[0] = value;
			clusterHash.put(propName, allValuesOfPropInCluster);
		} else {
			allValuesOfPropInCluster = clusterHash.get(propName);
			int lastNonEmptyValIdx = ArrayUtilityMethods.determineLastNonNullValue(allValuesOfPropInCluster);
			if(lastNonEmptyValIdx == allValuesOfPropInCluster.length - 1) {
				// object array is full, resize it to double the size
				allValuesOfPropInCluster = ArrayUtilityMethods.resizeArray(allValuesOfPropInCluster, 2);
				clusterHash.put(propName, allValuesOfPropInCluster);
			} else {
				allValuesOfPropInCluster[lastNonEmptyValIdx+1] = value;
			}
		}
	}

	@Override
	public void createData() {
		processQuery();
		ClusteringAlgorithm clusterAlg = new ClusteringAlgorithm(list,names);
		clusterAlg.setNumClusters(numClusters);
		clusterAlg.execute();
		numericalPropIndices = clusterAlg.getNumericalPropIndices();
		categoryPropIndices = clusterAlg.getCategoryPropIndices();
		int[] clusterAssigned = clusterAlg.getClustersAssigned();
		Hashtable<String, Integer> instanceIndexHash = clusterAlg.getInstanceIndexHash();
		ArrayList<Object[]> newList = new ArrayList<Object[]>();
		//store cluster final state information
		clusterInfo = new ArrayList<Object[]>(numClusters);
		clusterInfo = clusterAlg.getClusterRows();
		//iterate through query return
		for(Object[] dataRow : list) {
			Object[] newDataRow = new Object[dataRow.length + 1];
			String instance = "";
			for(int i = 0; i < dataRow.length; i++) {
				if(i == 0) {
					instance = dataRow[i].toString();
				}
				newDataRow[i] = dataRow[i];
			}
			int clusterNumber = clusterAssigned[instanceIndexHash.get(instance)];
			newDataRow[newDataRow.length - 1] = clusterNumber;
			newList.add(newDataRow);
			//add to matrix
		}
		list = newList;
		String[] newNames = new String[names.length + 1];
		for(int i = 0; i < names.length; i++) {
			newNames[i] = names[i];
		}
		newNames[newNames.length - 1] = "CluserID";
		names = newNames;

		dataHash = processQueryData();
	}

	private void processQuery() 
	{
		SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		//run the query against the engine provided
		sjsw.setEngine(engine);
		sjsw.setQuery(query);
		sjsw.executeQuery();	
		names = sjsw.getVariables();
		list = new ArrayList<Object[]>();
		while(sjsw.hasNext()) {
			SesameJenaSelectStatement sjss = sjsw.next();
			Object[] dataRow = new Object[names.length];
			for(int i = 0; i < names.length; i++) {
				dataRow[i] = sjss.getVar(names[i]);
			}
			list.add(dataRow);
		}
	}

	/**
	 * Sets the string version of the SPARQL query on the playsheet.
	 * Pulls out the number of clusters and stores them in the numClusters
	 * @param query String
	 */
	@Override
	public void setQuery(String query) {
		if(query.startsWith("SELECT") || query.startsWith("CONSTRUCT"))
			this.query=query;
		else{
			logger.info("New Query " + query);
			int semi=query.indexOf(";");
			numClusters = Integer.parseInt(query.substring(0,semi));
			this.query = query.substring(semi+1);
		}
	}

	public void addScrollPanel(JPanel rawDataPanel) {
		JScrollPane scrollPane = new JScrollPane(table);
		scrollPane.getVerticalScrollBar().setUI(new NewScrollBarUI());
		scrollPane.setAutoscrolls(true);

		GridBagConstraints gbc_scrollPane = new GridBagConstraints();
		gbc_scrollPane.fill = GridBagConstraints.BOTH;
		gbc_scrollPane.gridx = 0;
		gbc_scrollPane.gridy = 0;
		rawDataPanel.add(scrollPane, gbc_scrollPane);
	}
}
