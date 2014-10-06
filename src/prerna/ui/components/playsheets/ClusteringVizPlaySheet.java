package prerna.ui.components.playsheets;

import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.cluster.AbstractClusteringAlgorithm;
import prerna.algorithm.cluster.ClusterRemoveDuplicates;
import prerna.algorithm.cluster.ClusteringAlgorithm;
import prerna.algorithm.cluster.ClusteringOptimization;
import prerna.algorithm.cluster.GenerateEntropyDensity;
import prerna.math.BarChart;
import prerna.math.StatisticsUtilityMethods;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.GridFilterData;
import prerna.ui.components.GridTableModel;
import prerna.ui.components.GridTableRowSorter;
import prerna.ui.components.NewScrollBarUI;
import prerna.ui.main.listener.impl.ClusteringRefreshParamListener;
import prerna.ui.swing.custom.CustomButton;
import prerna.util.ArrayUtilityMethods;
import prerna.util.CSSApplication;
import prerna.util.Constants;
import prerna.util.DIHelper;
import aurelienribon.ui.css.Style;

@SuppressWarnings("serial")
public class ClusteringVizPlaySheet extends BrowserPlaySheet{

	private static final Logger LOGGER = LogManager.getLogger(ClusteringVizPlaySheet.class.getName());
	private int numClusters;
//	private double n;
//	private String type = "";
	private ArrayList<Object[]> clusterInfo;
	private ArrayList<JCheckBox> paramCheckboxes;
	private ArrayList<Object []> masterList;
	private String[] masterNames;

	//indexing used for bar graph visualizations
	private int[] numericalPropIndices;

	public ClusteringVizPlaySheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/cluster.html";
	}

	@Override
	public void createView() {
		super.createView();
		if(jTab.getTabCount()==1) {
			addSelectorTab();
			addGridTab();
		} else {
			jTab.remove(2);
			addGridTab();
		}

		new CSSApplication(getContentPane());
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void addGridTab() {
		JPanel panel = new JPanel();
		table = new JTable();
		panel.add(table);
		GridBagLayout gbl_mainPanel = new GridBagLayout();
		gbl_mainPanel.columnWidths = new int[]{0, 0};
		gbl_mainPanel.rowHeights = new int[]{0, 0};
		gbl_mainPanel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_mainPanel.rowWeights = new double[]{1.0, Double.MIN_VALUE};
		panel.setLayout(gbl_mainPanel);
		addScrollPanel(panel, table);
		GridFilterData gfd = new GridFilterData();
		if(clusterInfo != null) {
			list.addAll(0, clusterInfo);
		}
		gfd.setColumnNames(names);
		//append cluster information to list data
		gfd.setDataList(list);
		GridTableModel model = new GridTableModel(gfd);
		table.setModel(model);
		table.setRowSorter(new GridTableRowSorter(model));

		jTab.addTab("Raw Data", panel);
	}
	public void addSelectorTab() {
		GenerateEntropyDensity test = new GenerateEntropyDensity(list);
		double[] testVals = test.generateEntropy();
		DecimalFormat formatter = new DecimalFormat("0.####E0");
		JPanel panel = new JPanel();

		GridBagLayout gbl_panel = new GridBagLayout();
		gbl_panel.columnWidths = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
		gbl_panel.rowHeights = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
		gbl_panel.columnWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
		gbl_panel.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
		panel.setLayout(gbl_panel);
		
		JLabel lblParamSelect = new JLabel("Select Parameters:");
		lblParamSelect.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_lblParamSelect = new GridBagConstraints();
		gbc_lblParamSelect.anchor = GridBagConstraints.WEST;
		gbc_lblParamSelect.insets = new Insets(10, 5, 0, 0);
		gbc_lblParamSelect.gridx = 0;
		gbc_lblParamSelect.gridy = 0;
		panel.add(lblParamSelect, gbc_lblParamSelect);
		paramCheckboxes = new ArrayList<JCheckBox>();
		
		JLabel entropyDensityLabel = new JLabel("Entropy Density For Parameter:");
		entropyDensityLabel.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_entropyDensityLabel = new GridBagConstraints();
		gbc_entropyDensityLabel.anchor = GridBagConstraints.NORTHWEST;
		gbc_entropyDensityLabel.fill = GridBagConstraints.NONE;
		gbc_entropyDensityLabel.insets = new Insets(10, 5, 0, 0);
		gbc_entropyDensityLabel.gridx = 1;
		gbc_entropyDensityLabel.gridy = 0;
		panel.add(entropyDensityLabel, gbc_entropyDensityLabel);
		
		for(int i=1;i<masterNames.length;i++) {
			String checkboxLabel = masterNames[i];
			JCheckBox checkbox = new JCheckBox(checkboxLabel);
			checkbox.setName(checkboxLabel+"checkBox");
			checkbox.setSelected(true);
			GridBagConstraints gbc_checkbox = new GridBagConstraints();
			gbc_checkbox.anchor = GridBagConstraints.NORTHWEST;
			gbc_checkbox.fill = GridBagConstraints.NONE;
			gbc_checkbox.insets = new Insets(5, 10, 0, 0);
			gbc_checkbox.gridx = 0;
			gbc_checkbox.gridy = i;
			panel.add(checkbox, gbc_checkbox);
			paramCheckboxes.add(checkbox);
			
			JLabel entropyDensityVal = new JLabel();
			entropyDensityVal.setText(formatter.format(testVals[i-1]));
			GridBagConstraints gbc_entropyDensityVal = new GridBagConstraints();
			gbc_entropyDensityVal.anchor = GridBagConstraints.NORTHWEST;
			gbc_entropyDensityVal.fill = GridBagConstraints.NONE;
			gbc_entropyDensityVal.insets = new Insets(5, 10, 0, 0);
			gbc_entropyDensityVal.gridx = 1;
			gbc_entropyDensityVal.gridy = i;
			panel.add(entropyDensityVal, gbc_entropyDensityVal);
		}
		
		JButton btnRefreshParam = new CustomButton("Refresh Parameters");
		btnRefreshParam.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_btnRefreshParam = new GridBagConstraints();
		gbc_btnRefreshParam.insets = new Insets(10, 5, 0, 0);
		gbc_btnRefreshParam.anchor = GridBagConstraints.NORTH;
		gbc_btnRefreshParam.fill = GridBagConstraints.VERTICAL;
		gbc_btnRefreshParam.gridx = 0;
		gbc_btnRefreshParam.gridy = names.length + 1;
		panel.add(btnRefreshParam, gbc_btnRefreshParam);
		Style.registerTargetClassName(btnRefreshParam,  ".createBtn");

		ClusteringRefreshParamListener refListener = new ClusteringRefreshParamListener();
		refListener.setPlaySheet(this);
		refListener.setCheckBoxes(paramCheckboxes);
		refListener.setMasterData(masterNames, masterList);
		btnRefreshParam.addActionListener(refListener);
		
		JScrollPane scroll = new JScrollPane(panel);
		jTab.insertTab("Param Selector", null, scroll, null, 0);
		jTab.setSelectedIndex(0);
	}

	public void filterData(String[] filteredNames,ArrayList<Object []> filteredList) {
		this.names = filteredNames;
		this.list = filteredList;
		createData();
		createView();
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
						BarChart chart = null;
						try {
							chart = new BarChart(numValues);
						} catch(OutOfMemoryError e) {
							System.out.println("");
							chart = new BarChart(numValues);
						}
						Hashtable<String, Object>[] propBins = chart.getRetHashForJSON();
						Hashtable<String, Object> innerHash = new Hashtable<String, Object>();
						String[] zScore = StatisticsUtilityMethods.getZScoreRangeAsString(numValues, true);
						// cause JS is dumb
						Object[] propBinsArr = new Object[]{propBins};
						innerHash.put("dataSeries", propBinsArr);
						innerHash.put("names", new String[]{propName, "Distribution"});
						innerHash.put("zScore", zScore);
						clusterData.put(propName, innerHash);
					} else {
						String[] stringValues = ArrayUtilityMethods.convertObjArrToStringArr(values);
						BarChart chart = new BarChart(stringValues);
						Hashtable<String, Object>[] propBins = chart.getRetHashForJSON();
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
		if(masterList==null) {
			processQuery();
			ClusterRemoveDuplicates formatter = new ClusterRemoveDuplicates(list, names);
			masterList = formatter.getRetMasterTable();
			masterNames = formatter.getRetVarNames();
		}
		//For testing purposes
//		PrintWriter writer = null;
//		try {
//			writer = new PrintWriter("Clustering_Algorithm_Optimization.txt");
//			writer.println("Clusters\t\t\tInstanceToCluster\t\t\tClusterToCluster\t\t\tItems\t\t\tAverage");
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		}
//		for(int i = 2; i < 40; i++) {
//			AbstractClusteringAlgorithm clusterAlg = new ClusteringAlgorithm(list, names);
//			clusterAlg.setNumClusters(i);
//			clusterAlg.execute();
//			double instanceToClusterSim = clusterAlg.calculateFinalInstancesToClusterSimilarity();
//			double clusterToClusterSim = clusterAlg.calculateFinalTotalClusterToClusterSimilarity();
//			double sum = instanceToClusterSim + clusterToClusterSim;
//			double items = list.size() + (double) (i * (i-1) /2);
//			double average = sum/items;
//			writer.println(i + "\t\t\t" + instanceToClusterSim + "\t\t\t" + clusterToClusterSim + "\t\t\t" + items + "\t\t\t" + average);
//		}
//		writer.close();
		
		AbstractClusteringAlgorithm clusterAlg;
//		if(type.equalsIgnoreCase("agglomerative")){
//			clusterAlg = new AgglomerativeClusteringAlgorithm(list,names);
//			clusterAlg.setNumClusters(numClusters);
//			((AgglomerativeClusteringAlgorithm) clusterAlg).setN(n);
		if(numClusters > 2){
			clusterAlg = new ClusteringAlgorithm(masterList, masterNames);
			clusterAlg.setNumClusters(numClusters);
		} else{
			clusterAlg = new ClusteringOptimization(masterList, masterNames);
			((ClusteringOptimization) clusterAlg).determineOptimalCluster();
			numClusters = ((ClusteringOptimization) clusterAlg).getNumClusters();
		}
		clusterAlg.execute();
		//store cluster final state information
		clusterInfo = new ArrayList<Object[]>(numClusters);
		clusterInfo = clusterAlg.getClusterRows();
		
		numericalPropIndices = clusterAlg.getNumericalPropIndices();
		
		int[] clusterAssigned = clusterAlg.getClustersAssigned();
		Hashtable<String, Integer> instanceIndexHash = clusterAlg.getInstanceIndexHash();
		ArrayList<Object[]> newList = new ArrayList<Object[]>();
		//iterate through query return
		for(Object[] dataRow : masterList) {
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
		String[] newNames = new String[masterNames.length + 1];
		for(int i = 0; i < masterNames.length; i++) {
			newNames[i] = masterNames[i];
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
		LOGGER.info("New Query " + query);
		String[] querySplit = query.split("\\+\\+\\+");
		if(querySplit.length == 1) {
			this.query = query;
		} else if(querySplit.length == 2) {
			this.query = querySplit[0];
			this.numClusters = Integer.parseInt(querySplit[1]);
		} 
//		else if(querySplit.length == 4) {
//			this.query = querySplit[0];
//			this.numClusters = Integer.parseInt(querySplit[1]);
//			this.n = Double.parseDouble(querySplit[2]);
//			this.type = querySplit[3];
//		}
	}

	public void addScrollPanel(JPanel panel, JComponent obj) {
		JScrollPane scrollPane = new JScrollPane(obj);
		scrollPane.getVerticalScrollBar().setUI(new NewScrollBarUI());
		scrollPane.setAutoscrolls(true);

		GridBagConstraints gbc_scrollPane = new GridBagConstraints();
		gbc_scrollPane.fill = GridBagConstraints.BOTH;
		gbc_scrollPane.gridx = 0;
		gbc_scrollPane.gridy = 0;
		panel.add(scrollPane, gbc_scrollPane);
	}
}
