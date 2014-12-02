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

import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import edu.stanford.nlp.util.ArrayUtils;
import prerna.algorithm.cluster.AbstractClusteringAlgorithm;
import prerna.algorithm.cluster.ClusterRemoveDuplicates;
import prerna.algorithm.cluster.ClusteringAlgorithm;
import prerna.algorithm.cluster.ClusteringOptimization;
import prerna.algorithm.cluster.DatasetSimilarity;
import prerna.algorithm.cluster.GenerateEntropyDensity;
import prerna.math.BarChart;
import prerna.math.StatisticsUtilityMethods;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.GridFilterData;
import prerna.ui.components.GridTableModel;
import prerna.ui.components.GridTableRowSorter;
import prerna.ui.components.NewScrollBarUI;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.main.listener.impl.ClusteringDrillDownListener;
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
	
	private int inputNumClusters;
	private int numClusters;
	private String fullQuery;
	
	private ArrayList<Object[]> clusterInfo;
	private ArrayList<JCheckBox> paramCheckboxes;
	private ArrayList<JCheckBox> clusterCheckboxes;
	private ArrayList<JCheckBox> paramsToCheck;
	private JPanel clusterSelectorPanel;
	private ArrayList<Object []> masterList;
	private String[] masterNames;
	private String paramSelectorTabName = "Param Selector";
	private String rawDataTabName = "Raw Data";

	private Hashtable<String, IPlaySheet> playSheetHash;
	private JComboBox<String> drillDownTabSelectorComboBox;

	//indexing used for bar graph visualizations
	private int[] numericalPropIndices;

	private ArrayList<Object[]> rawDataList;
	private String[] rawDataNames;
	
	private Hashtable<String, Integer> instanceIndexHash;
	private int[] clusterAssigned;
	private Boolean addAsTab = false;//determines whether to add this playsheet as a tab to the jTab or to create a new playsheet
	
	public ClusteringVizPlaySheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/cluster.html";
	}

	@Override
	public void createView() {
		super.createView();
	}
	
	/**
	 * Method addPanel. Creates a panel and adds the table to the panel.
	 */
	@Override
	public void addPanel()
	{
		//if this is to be a separate playsheet, create the tab in a new window
		//otherwise, if this is to be just a new tab in an existing playsheet,
		if(!addAsTab) {
			super.addPanel();
			if(jTab.indexOfTab(paramSelectorTabName)<0) {//if param selecctor tab does not exist, add it
				addSelectorTab();
			}
			if(jTab.indexOfTab(rawDataTabName)>0){//there is already a grid tab, remove it
				jTab.remove(jTab.indexOfTab(rawDataTabName));
			}
			addGridTab();
		}else {
			String lastTabName = jTab.getTitleAt(jTab.getTabCount()-1);
			LOGGER.info("Parsing integer out of last tab name");
			int count = 1;
			if(jTab.getTabCount()>1)
				count = Integer.parseInt(lastTabName.substring(0,lastTabName.indexOf(".")))+1;
			addPanelAsTab(count+". Clustering");
			rawDataTabName = count+". Clustering Raw Data";
			addGridTab();
			
			if(playSheetHash!=null) {
				playSheetHash.put(rawDataTabName,this);
			}
			
			//if there is a drop down, update it to include tab name
			if(drillDownTabSelectorComboBox!=null){
				DefaultComboBoxModel<String> model = (DefaultComboBoxModel<String>) drillDownTabSelectorComboBox.getModel();
				model.addElement(rawDataTabName);
				drillDownTabSelectorComboBox.repaint();
			}

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
		if(names.length != rawDataNames.length) {
			if(names.length > 15) {
				gfd.setColumnNames(rawDataNames);
				//append cluster information to list data
				gfd.setDataList(rawDataList);
				GridTableModel model = new GridTableModel(gfd);
				table.setModel(model);
				table.setRowSorter(new GridTableRowSorter(model));
			} else {
				gfd.setColumnNames(names);
				//append cluster information to list data
				gfd.setDataList(list);
				GridTableModel model = new GridTableModel(gfd);
				table.setModel(model);
				table.setRowSorter(new GridTableRowSorter(model));
			}
		} else {
			list.addAll(0, clusterInfo);
			gfd.setColumnNames(names);
			//append cluster information to list data
			gfd.setDataList(list);
			GridTableModel model = new GridTableModel(gfd);
			table.setModel(model);
			table.setRowSorter(new GridTableRowSorter(model));

		}
		jTab.addTab(rawDataTabName, panel);
	}

	public void addSelectorTab() {
		GenerateEntropyDensity test;
		if(masterList!=null) {
			test = new GenerateEntropyDensity(masterList,true);
		}
		else
			test = new GenerateEntropyDensity(list);
		double[] testVals = test.generateEntropy();
		DecimalFormat formatter = new DecimalFormat("0.000E0");
		JPanel panel = new JPanel();

		GridBagLayout gbl_panel = new GridBagLayout();
		gbl_panel.columnWidths = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
		gbl_panel.rowHeights = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
		gbl_panel.columnWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
		gbl_panel.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
		panel.setLayout(gbl_panel);
	
		JPanel paramSelectorPanel = new JPanel();
		GridBagConstraints gbc_paramSelectorPanel = new GridBagConstraints();
		gbc_paramSelectorPanel.anchor = GridBagConstraints.WEST;
		gbc_paramSelectorPanel.insets = new Insets(10, 5, 0, 0);
		gbc_paramSelectorPanel.gridx = 0;
		gbc_paramSelectorPanel.gridy = 0;
		panel.add(paramSelectorPanel, gbc_paramSelectorPanel);
		
		GridBagLayout gbl_paramSelectorPanel = new GridBagLayout();
		gbl_paramSelectorPanel.columnWidths = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
		gbl_paramSelectorPanel.rowHeights = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
		gbl_paramSelectorPanel.columnWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
		gbl_paramSelectorPanel.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
		paramSelectorPanel.setLayout(gbl_paramSelectorPanel);
				
		JLabel lblParamSelect = new JLabel("Select Parameters:");
		lblParamSelect.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_lblParamSelect = new GridBagConstraints();
		gbc_lblParamSelect.anchor = GridBagConstraints.WEST;
		gbc_lblParamSelect.insets = new Insets(10, 5, 0, 0);
		gbc_lblParamSelect.gridx = 0;
		gbc_lblParamSelect.gridy = 0;
		paramSelectorPanel.add(lblParamSelect, gbc_lblParamSelect);
		paramCheckboxes = new ArrayList<JCheckBox>();
		
		JLabel entropyDensityLabel = new JLabel("Entropy Density For Parameter:");
		entropyDensityLabel.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_entropyDensityLabel = new GridBagConstraints();
		gbc_entropyDensityLabel.anchor = GridBagConstraints.NORTHWEST;
		gbc_entropyDensityLabel.fill = GridBagConstraints.NONE;
		gbc_entropyDensityLabel.insets = new Insets(10, 5, 0, 0);
		gbc_entropyDensityLabel.gridx = 1;
		gbc_entropyDensityLabel.gridy = 0;
		paramSelectorPanel.add(entropyDensityLabel, gbc_entropyDensityLabel);
		for(int i=1;i<masterNames.length;i++) {
			
			JLabel entropyDensityVal = new JLabel();
			entropyDensityVal.setText(formatter.format(testVals[i-1]));
			GridBagConstraints gbc_entropyDensityVal = new GridBagConstraints();
			gbc_entropyDensityVal.anchor = GridBagConstraints.NORTHWEST;
			gbc_entropyDensityVal.fill = GridBagConstraints.NONE;
			gbc_entropyDensityVal.insets = new Insets(5, 10, 0, 0);
			gbc_entropyDensityVal.gridx = 0;
			gbc_entropyDensityVal.gridy = i;
			paramSelectorPanel.add(entropyDensityVal, gbc_entropyDensityVal);
			
			String checkboxLabel = "";
			checkboxLabel = masterNames[i];
			JCheckBox checkbox = new JCheckBox(checkboxLabel);
			checkbox.setName(checkboxLabel+"checkBox");
			checkbox.setSelected(true);
			if(paramsToCheck!=null) {
				boolean selected = paramsToCheck.get(i-1).isSelected();
				checkbox.setSelected(selected);
			}
			GridBagConstraints gbc_checkbox = new GridBagConstraints();
			gbc_checkbox.anchor = GridBagConstraints.NORTHWEST;
			gbc_checkbox.fill = GridBagConstraints.NONE;
			gbc_checkbox.insets = new Insets(5, 10, 0, 0);
			gbc_checkbox.gridx = 1;
			gbc_checkbox.gridy = i;
			paramSelectorPanel.add(checkbox, gbc_checkbox);
			paramCheckboxes.add(checkbox);
		}
		
		JButton btnRefreshParam = new CustomButton("Refresh Parameters");
		btnRefreshParam.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_btnRefreshParam = new GridBagConstraints();
		gbc_btnRefreshParam.insets = new Insets(10, 5, 0, 0);
		gbc_btnRefreshParam.anchor = GridBagConstraints.NORTH;
		gbc_btnRefreshParam.fill = GridBagConstraints.VERTICAL;
		gbc_btnRefreshParam.gridx = 0;
		gbc_btnRefreshParam.gridy = 2;
		panel.add(btnRefreshParam, gbc_btnRefreshParam);
		Style.registerTargetClassName(btnRefreshParam,  ".createBtn");

		ClusteringRefreshParamListener refListener = new ClusteringRefreshParamListener();
		refListener.setPlaySheet(this);
		refListener.setCheckBoxes(paramCheckboxes);
		refListener.setMasterData(masterNames, masterList);
		btnRefreshParam.addActionListener(refListener);
		
		clusterSelectorPanel = new JPanel();
		GridBagConstraints gbc_clusterSelectorPanel = new GridBagConstraints();
		gbc_clusterSelectorPanel.anchor = GridBagConstraints.WEST;
		gbc_clusterSelectorPanel.insets = new Insets(10, 5, 0, 0);
		gbc_clusterSelectorPanel.gridx = 1;
		gbc_clusterSelectorPanel.gridy = 0;
		panel.add(clusterSelectorPanel, gbc_clusterSelectorPanel);
		
		GridBagLayout gbl_clusterSelectorPanel = new GridBagLayout();
		gbl_clusterSelectorPanel.columnWidths = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
		gbl_clusterSelectorPanel.rowHeights = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
		gbl_clusterSelectorPanel.columnWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
		gbl_clusterSelectorPanel.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
		clusterSelectorPanel.setLayout(gbl_clusterSelectorPanel);
		
		JLabel lblClusterSelect = new JLabel("Select Clusters to drill down:");
		lblClusterSelect.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_lblClusterSelect = new GridBagConstraints();
		gbc_lblClusterSelect.anchor = GridBagConstraints.NORTHWEST;
		gbc_lblClusterSelect.fill = GridBagConstraints.NONE;
		gbc_lblClusterSelect.insets = new Insets(10, 5, 0, 0);
		gbc_lblClusterSelect.gridx = 0;
		gbc_lblClusterSelect.gridy = 0;
		clusterSelectorPanel.add(lblClusterSelect, gbc_lblClusterSelect);
		clusterCheckboxes = new ArrayList<JCheckBox>();
		
		//need to iterate through the cluster list
		updateClusterCheckboxes();

		JButton btnDrillDownCluster = new CustomButton("Drill Down on Selected Clusters");
		btnDrillDownCluster.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_btnDrillDownCluster = new GridBagConstraints();
		gbc_btnDrillDownCluster.insets = new Insets(10, 5, 0, 0);
		gbc_btnDrillDownCluster.anchor = GridBagConstraints.NORTH;
		gbc_btnDrillDownCluster.fill = GridBagConstraints.VERTICAL;
		gbc_btnDrillDownCluster.gridx = 1;
		gbc_btnDrillDownCluster.gridy = 2;
		panel.add(btnDrillDownCluster, gbc_btnDrillDownCluster);
		Style.registerTargetClassName(btnDrillDownCluster,  ".createBtn");

		ClusteringDrillDownListener drillDownListener = new ClusteringDrillDownListener();
		drillDownListener.setCheckBoxes(paramCheckboxes);
		drillDownListener.setMasterData(masterNames, masterList);
		drillDownListener.setPlaySheet(this);
		btnDrillDownCluster.addActionListener(drillDownListener);
		
		JScrollPane scroll = new JScrollPane(panel);
		jTab.insertTab(paramSelectorTabName, null, scroll, null, 0);
		jTab.setSelectedIndex(0);
	}
	
	private void updateClusterCheckboxes() {
		//remove old clusters
		for(JCheckBox checkbox : clusterCheckboxes) {
			clusterSelectorPanel.remove(checkbox);
		}
		clusterCheckboxes = new ArrayList<JCheckBox>();
		for(int i=0;i<numClusters;i++) {		
			String checkboxLabel = "" + i;
			JCheckBox checkbox = new JCheckBox(checkboxLabel);
			checkbox.setName("Cluster "+checkboxLabel+"checkBox");
			checkbox.setSelected(true);
			GridBagConstraints gbc_checkbox = new GridBagConstraints();
			gbc_checkbox.anchor = GridBagConstraints.NORTHWEST;
			gbc_checkbox.fill = GridBagConstraints.NONE;
			gbc_checkbox.insets = new Insets(5, 10, 0, 0);
			gbc_checkbox.gridx = 0;
			gbc_checkbox.gridy = i+1;
			clusterSelectorPanel.add(checkbox, gbc_checkbox);
			clusterCheckboxes.add(checkbox);
		}
	}
	
	public void setSelectedParams(ArrayList<JCheckBox> paramCheckboxes) {
		this.paramsToCheck = paramCheckboxes;
	}
	
	public void drillDownData(String[] masterNames,String[] filteredNames,ArrayList<Object []> masterList,ArrayList<Object []> filteredList) {
		this.masterNames = masterNames;
		this.names = filteredNames;
		this.masterList = masterList;
		this.list = filteredList;
		numClusters = inputNumClusters;
	}

	public void filterData(String[] filteredNames,ArrayList<Object []> filteredList) {
		this.names = filteredNames;
		this.list = filteredList;
		numClusters = inputNumClusters;
		createData();
		createView();
		updateClusterCheckboxes();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public Hashtable processQueryData()
	{
		Hashtable allHash = new Hashtable();
		ArrayList<Hashtable<String, Object>> dataList = new ArrayList<Hashtable<String, Object>>(list.size());
		ArrayList<Hashtable<String, Object[]>> clusterInformation = new ArrayList<Hashtable<String, Object[]>>(numClusters);
		ArrayList<ArrayList<Object[]>> storeInstanceDataInCluster = new ArrayList<ArrayList<Object[]>>();
		//initialize cluster information
		for(int i = 0; i < numClusters; i++) {
			Hashtable<String, Object[]> innerHash = new Hashtable<String, Object[]>();
			clusterInformation.add(innerHash);
			storeInstanceDataInCluster.add(new ArrayList<Object[]>());
		}

		//original names
		for(Object[] dataRow : list) {
			//add name and cluster under special names first
			int clusterID = (int) dataRow[dataRow.length - 1];
			
			//split up instances based on cluster
			ArrayList<Object[]> instancesInCluster = storeInstanceDataInCluster.get(clusterID);
			instancesInCluster.add(dataRow);
			
			Hashtable<String, Object> instanceHash = new Hashtable<String, Object>();
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
						Double[] numValues = ArrayUtilityMethods.convertObjArrToDoubleWrapperArr(values);
						numValues = ArrayUtilityMethods.sortDoubleWrapperArr(numValues);
						BarChart chart = new BarChart(numValues);
						Hashtable<String, Object>[] propBins = chart.getRetHashForJSON();
						Hashtable<String, Object> innerHash = new Hashtable<String, Object>();
						String[] zScore = StatisticsUtilityMethods.getZScoreRangeAsStringIgnoringNull(numValues, true);
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
			
			// add in similarity for cluster
			String[] origArr = names;
			origArr = Arrays.copyOfRange(origArr, 0, origArr.length - 1);
			DatasetSimilarity alg = new DatasetSimilarity(storeInstanceDataInCluster.get(i), origArr);
			alg.generateClusterCenters();
			double[] simValues = alg.getSimilarityValuesForInstances();
			BarChart chart = new BarChart(simValues, "");
			Hashtable<String, Object>[] bins = chart.getRetHashForJSON();
			String[] zScore = StatisticsUtilityMethods.getZScoreRangeAsString(simValues, false);

			Hashtable<String, Object> innerHash = new Hashtable<String, Object>();
			Object[] binArr = new Object[]{bins};
			innerHash.put("dataSeries", binArr);
			innerHash.put("names", new String[]{names[0].concat(" Similarity Distribution to Dataset Center"), "Distribution"});
			innerHash.put("zScore", zScore);
			clusterData.put("Similarity Value to Dataset Center", innerHash);
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
		//if you dont have a list, then run the query
		if(list==null) {
			processQuery();
			ClusterRemoveDuplicates formatter = new ClusterRemoveDuplicates(list, names);
			list = formatter.getRetMasterTable();
			names = formatter.getRetVarNames();
			masterList =  new ArrayList<Object[]>(list);
			masterNames = names.clone();//TODO make sure this writes properly
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
		if(numClusters >= 2){
			clusterAlg = new ClusteringAlgorithm(list, names);
			clusterAlg.setNumClusters(numClusters);
		} else{
			clusterAlg = new ClusteringOptimization(list, names);
			((ClusteringOptimization) clusterAlg).determineOptimalCluster();
			numClusters = ((ClusteringOptimization) clusterAlg).getNumClusters();
		}
		clusterAlg.execute();
		//store cluster final state information
		clusterInfo = new ArrayList<Object[]>(numClusters);
		clusterInfo = clusterAlg.getSummaryClusterRows();
		
		numericalPropIndices = clusterAlg.getNumericalPropIndices();
		clusterAssigned = clusterAlg.getClustersAssigned();
		instanceIndexHash = clusterAlg.getInstanceIndexHash();
		
		//updating our list and names to include the cluster assigned in the last column
		ArrayList<Object[]> listWithCluster = new ArrayList<Object[]>();
		for(Object[] dataRow : list) {
			Object[] newDataRow = new Object[dataRow.length + 1];
			String instance="";
			if(dataRow.length>0)
				instance = dataRow[0].toString();
			for(int i = 0; i < dataRow.length; i++) {
				newDataRow[i] = dataRow[i];
			}
			int clusterNumber = clusterAssigned[instanceIndexHash.get(instance)];
			newDataRow[newDataRow.length - 1] = clusterNumber;
			listWithCluster.add(newDataRow);
		}
		list = listWithCluster;
		String[] namesWithCluster = new String[names.length + 1];
		for(int i = 0; i < names.length; i++) {
			namesWithCluster[i] = names[i];
		}
		namesWithCluster[namesWithCluster.length - 1] = "ClusterID";
		names = namesWithCluster;
		rawDataList =  new ArrayList<Object[]>(list);
		rawDataNames = names.clone();//TODO make sure this writes properly

		dataHash = processQueryData();
	}

	//TODO why arent we just calling super create data here?
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
		fullQuery = query;
		LOGGER.info("New Query " + query);
		String[] querySplit = query.split("\\+\\+\\+");
		if(querySplit.length == 1) {
			this.query = query;
		} else if(querySplit.length == 2) {
			this.query = querySplit[0];
			this.inputNumClusters = Integer.parseInt(querySplit[1]);
			this.numClusters = inputNumClusters;
		} 
//		else if(querySplit.length == 4) {
//			this.query = querySplit[0];
//			this.numClusters = Integer.parseInt(querySplit[1]);
//			this.n = Double.parseDouble(querySplit[2]);
//			this.type = querySplit[3];
//		}
	}
	
//	public void setList(ArrayList<Object []> list){
//		this.list = list;	
//	}
//	public void setNames(String[] names){
//		this.names = names;	
//	}
	
	public String getFullQuery() {
		return fullQuery;
	}
	public ArrayList<JCheckBox> getClusterCheckboxes() {
		return clusterCheckboxes;
	}
	public Hashtable<String, Integer> getInstanceIndexHash() {
		return instanceIndexHash;
	}
	public int[] getClusterAssigned() {
		return clusterAssigned;
	}
	public void setAddAsTab(Boolean addAsTab) {
		this.addAsTab = addAsTab;
	}
	public void setPlaySheetHash(Hashtable<String, IPlaySheet> playSheetHash) {
		this.playSheetHash = playSheetHash;
	}
	public void setDrillDownTabSelectorComboBox(JComboBox<String> drillDownTabSelectorComboBox){
		this.drillDownTabSelectorComboBox = drillDownTabSelectorComboBox;
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
	public void setInputNumClusters(int inputNumClusters) {
		this.inputNumClusters = inputNumClusters;
		this.numClusters = inputNumClusters;
	}
	public int getNumClusters() {
		return numClusters;
	}
	public ArrayList<Object []> getMasterList() {
		return masterList;
	}
	public String[] getMasterNames() {
		return masterNames;
	}
}