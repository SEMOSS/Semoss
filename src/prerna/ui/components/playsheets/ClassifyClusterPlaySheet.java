package prerna.ui.components.playsheets;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.beans.PropertyVetoException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Hashtable;

import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JSlider;
import javax.swing.JTabbedPane;
import javax.swing.JTextField;
import javax.swing.JToggleButton;
import javax.swing.border.BevelBorder;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.cluster.DatasetSimilarity;
import prerna.algorithm.cluster.GenerateEntropyDensity;
import prerna.algorithm.weka.impl.WekaClassification;
import prerna.math.BarChart;
import prerna.ui.components.BrowserGraphPanel;
import prerna.ui.components.NewScrollBarUI;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.main.listener.impl.ClassificationSelectionListener;
import prerna.ui.main.listener.impl.ClusterTabSelectionListener;
import prerna.ui.main.listener.impl.NumberOfClustersSelectionListener;
import prerna.ui.main.listener.impl.RunAlgorithmListener;
import prerna.ui.main.listener.impl.RunDrillDownListener;
import prerna.ui.main.listener.impl.SelectAlgorithmListener;
import prerna.ui.main.listener.impl.SelectCheckboxesListener;
import prerna.ui.main.listener.impl.ShowDrillDownListener;
import prerna.ui.swing.custom.CustomButton;
import prerna.ui.swing.custom.ToggleButton;
import prerna.util.CSSApplication;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.QuestionPlaySheetStore;
import prerna.util.Utility;
import aurelienribon.ui.css.Style;

public class ClassifyClusterPlaySheet extends BasicProcessingPlaySheet{

	private static final Logger LOGGER = LogManager.getLogger(ClassifyClusterPlaySheet.class.getName());
	
	private JTabbedPane jTab;
	private Hashtable<String, IPlaySheet> playSheetHash = new Hashtable<String,IPlaySheet>();
	private JPanel variableSelectorPanel;

	//independent variable panel
	private double[] entropyArr;
	private double[] accuracyArr;
	private double[] precisionArr;
	private ArrayList<JCheckBox> ivCheckboxes;
	private ArrayList<JLabel> entropyLabels;
	private ArrayList<JLabel> accuracyLabels;
	private ArrayList<JLabel> precisionLabels;
	//independent variable panel - select all
	private JCheckBox checkboxSelectAllIVs;
	private SelectCheckboxesListener selectAllIVsList;
	public JPanel indVariablesPanel;
	
	//data set similarity chart
	private BrowserGraphPanel simBarChartPanel;
	private Hashtable<String, Object> simBarChartHash;
	
	private String checkboxName = "checkBox";
	
	//cluster/classify/outlier selection drop down
	private JComboBox<String> algorithmComboBox;
	//run buttons
	private JButton runAlgorithm, runDrillDown;
	
	//cluster panel components
	private JPanel clusterPanel;
	private JLabel lblSelectNumClusters;
	private JComboBox<String> selectNumClustersComboBox;
	private final String automaticallySelectNumClustersText = "Automatically select number of clusters";
	private final String manuallySelectNumClustersText = "Manually set number of clusters";
	private JTextField selectNumClustersTextField;
	
	//classification panel components
	private JPanel classifyPanel;
	private JComboBox<String> classificationMethodComboBox;
	private JComboBox<String> classComboBox;
	private JLabel lblSelectClass, lblSelectClassMethod;
	
	//outlier panel components
	private JPanel outlierPanel;
	private JLabel lblEnterKNeighbors;
	private JSlider enterKNeighborsSlider;
	
	//drill down panel components
	private JToggleButton showDrillDownBtn;
	private JPanel drillDownPanel;
	private JLabel lblDrillDownSelectTab;
	private JComboBox<String> drillDownTabSelectorComboBox;
	//drill down panel cluster checkboxes
	private JPanel clusterCheckBoxPanel;
	private JCheckBox checkboxSelectAllClusters;
	private SelectCheckboxesListener selectAllClustersList;
	private ArrayList<JCheckBox> clusterCheckboxes = new ArrayList<JCheckBox>();
		
	public void setData(String[] names, ArrayList<Object[]> list) {
		this.list = list;
		this.names=names;
	}
	@Override
	public void createData() {
		if(list==null)
			super.createData();
	}
	@Override
	public void runAnalytics() {
		if(list==null)
			return;
		
		GenerateEntropyDensity test = new GenerateEntropyDensity(list,true);
		entropyArr = test.generateEntropy();
		fillSimBarChartHash(names,list);
	}
	
	public void fillSimBarChartHash(String[] names, ArrayList<Object[]> list) {
		//run the algorithms for similarity bar chart to create hash.
		DatasetSimilarity alg = new DatasetSimilarity(list, names);
		alg.generateClusterCenters();
		double[] simBarChartValues = alg.getSimilarityValuesForInstances();	
		
		BarChart chart = new BarChart(simBarChartValues, names[0]);
		Hashtable<String, Object>[] bins = chart.getRetHashForJSON();
		
		simBarChartHash = new Hashtable<String, Object>();
		Object[] binArr = new Object[]{bins};
		simBarChartHash.put("dataSeries", binArr);
		simBarChartHash.put("names", new String[]{names[0].concat(" Similarity Distribution to Dataset Center")});
		
	}

	@Override
	public void createView() {	
		if(list!=null && list.isEmpty()){
			String questionID = getQuestionID();
			// fill the nodetype list so that they can choose from
			// remove from store
			// this will also clear out active sheet
			QuestionPlaySheetStore.getInstance().remove(questionID);
			if(QuestionPlaySheetStore.getInstance().isEmpty())
			{
				JButton btnShowPlaySheetsList = (JButton) DIHelper.getInstance().getLocalProp(Constants.SHOW_PLAYSHEETS_LIST);
				btnShowPlaySheetsList.setEnabled(false);
			}
			Utility.showError("Query returned no results.");
			return;		
		}
		addPanel();

		if(list!=null){
			gfd.setColumnNames(names);
			gfd.setDataList(list);
		}

		updateProgressBar("100%...Selector Panel Complete", 100);
	}
	
	@Override
	public void addPanel() {//TODO add control panel?
		setWindow();
		updateProgressBar("0%...Preprocessing", 0);
		resetProgressBar();
		
		JPanel mainPanel = new JPanel();
		this.setContentPane(mainPanel);
		GridBagLayout gbl_mainPanel = new GridBagLayout();
		gbl_mainPanel.columnWidths = new int[]{0, 0};
		gbl_mainPanel.rowHeights = new int[]{0, 0};
		gbl_mainPanel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_mainPanel.rowWeights = new double[]{1.0, Double.MIN_VALUE};
		mainPanel.setLayout(gbl_mainPanel);
		
		JPanel barPanel = new JPanel();
		GridBagConstraints gbc_barPanel = new GridBagConstraints();
		gbc_barPanel.fill = GridBagConstraints.BOTH;
		gbc_barPanel.gridx = 0;
		gbc_barPanel.gridy = 1;
		mainPanel.add(barPanel, gbc_barPanel);
		barPanel.setLayout(new BorderLayout(0, 0));
		barPanel.add(jBar, BorderLayout.CENTER);
		
		//Panel that will be the first tab to select variables
		variableSelectorPanel = new JPanel();
		JScrollPane scrollPane = new JScrollPane(variableSelectorPanel);
		scrollPane.getVerticalScrollBar().setUI(new NewScrollBarUI());
		scrollPane.setAutoscrolls(true);
		
		jTab = new JTabbedPane();
		jTab.add("Variable Selector", scrollPane);
		GridBagConstraints gbc_jTab = new GridBagConstraints();
		gbc_jTab.fill = GridBagConstraints.BOTH;
		gbc_jTab.gridx = 0;
		gbc_jTab.gridy = 0;
		mainPanel.add(jTab, gbc_jTab);

		GridBagLayout gbl_panel = new GridBagLayout();
		gbl_panel.columnWidths = new int[]{0, 0, 0};
		gbl_panel.rowHeights = new int[]{0, 0, 0};
		gbl_panel.columnWeights = new double[]{0.0, 0.0, 1.0};
		gbl_panel.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 1.0};//, 0.0, 0.0, 0.0, 0.0, 1.0};
		variableSelectorPanel.setLayout(gbl_panel);

		JLabel lblDataSetSimilarity = new JLabel();
		lblDataSetSimilarity.setText("Dataset similarity distribution");
		lblDataSetSimilarity.setFont(new Font("Tahoma", Font.BOLD, 16));
		GridBagConstraints gbc_lblDataSetSimilarity = new GridBagConstraints();
		gbc_lblDataSetSimilarity.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_lblDataSetSimilarity.fill = GridBagConstraints.NONE;
		gbc_lblDataSetSimilarity.gridwidth = 3;
		gbc_lblDataSetSimilarity.insets = new Insets(10, 5, 0, 0);
		gbc_lblDataSetSimilarity.gridx = 0;
		gbc_lblDataSetSimilarity.gridy = 0;
		variableSelectorPanel.add(lblDataSetSimilarity, gbc_lblDataSetSimilarity);
		
		addSimBarChart();
		
		JLabel indVariablesLabel = new JLabel();
		indVariablesLabel.setText("Filter parameters");
		indVariablesLabel.setFont(new Font("Tahoma", Font.BOLD, 16));
		GridBagConstraints gbc_indVariablesLabel = new GridBagConstraints();
		gbc_indVariablesLabel.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_indVariablesLabel.fill = GridBagConstraints.NONE;
		gbc_indVariablesLabel.insets = new Insets(10, 5, 0, 0);
		gbc_indVariablesLabel.gridx = 0;
		gbc_indVariablesLabel.gridy = 2;
		variableSelectorPanel.add(indVariablesLabel, gbc_indVariablesLabel);
		
		indVariablesPanel = new JPanel();
		GridBagConstraints gbc_indVariablesPanel = new GridBagConstraints();
		gbc_indVariablesPanel.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_indVariablesPanel.fill = GridBagConstraints.NONE;
		gbc_indVariablesPanel.gridheight = 3;
		gbc_indVariablesPanel.insets = new Insets(10, 5, 0, 0);
		gbc_indVariablesPanel.gridx = 0;
		gbc_indVariablesPanel.gridy = 3;
		variableSelectorPanel.add(indVariablesPanel, gbc_indVariablesPanel);
		
		fillIndependentVariablePanel(indVariablesPanel);

		JLabel lblSelectAnalysis = new JLabel("Select analysis to perform:");
		lblSelectAnalysis.setFont(new Font("Tahoma", Font.BOLD, 16));
		GridBagConstraints gbc_lblSelectAnalysis = new GridBagConstraints();
		gbc_lblSelectAnalysis.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_lblSelectAnalysis.fill = GridBagConstraints.NONE;
		gbc_lblSelectAnalysis.insets = new Insets(10, 15, 0, 0);
		gbc_lblSelectAnalysis.gridx = 1;
		gbc_lblSelectAnalysis.gridy = 2;
		variableSelectorPanel.add(lblSelectAnalysis, gbc_lblSelectAnalysis);

		algorithmComboBox = new JComboBox<String>();
		algorithmComboBox.setFont(new Font("Tahoma", Font.PLAIN, 11));
		algorithmComboBox.setBackground(Color.GRAY);
		algorithmComboBox.setPreferredSize(new Dimension(100, 25));
		algorithmComboBox.setModel(new DefaultComboBoxModel<String>(new String[] {"Cluster", "Classify","Outliers","Similarity","Predictability"}));
		GridBagConstraints gbc_algorithmComboBox = new GridBagConstraints();
		gbc_algorithmComboBox.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_algorithmComboBox.fill = GridBagConstraints.NONE;
		gbc_algorithmComboBox.insets = new Insets(10, 5, 0, 0);
		gbc_algorithmComboBox.gridx = 2;
		gbc_algorithmComboBox.gridy = 2;
		variableSelectorPanel.add(algorithmComboBox, gbc_algorithmComboBox);
		SelectAlgorithmListener algSelectList = new SelectAlgorithmListener();
		algSelectList.setView(this);
		algorithmComboBox.addActionListener(algSelectList);
		
		//add in cluster / classify / outlier panels
		//show the cluster as default
		clusterPanel = new JPanel();
		GridBagConstraints gbc_clusterPanel = new GridBagConstraints();
		gbc_clusterPanel.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_clusterPanel.fill = GridBagConstraints.NONE;
		gbc_clusterPanel.gridwidth = 3;
		gbc_clusterPanel.insets = new Insets(5, 15, 0, 0);
		gbc_clusterPanel.gridx = 1;
		gbc_clusterPanel.gridy = 3;
		variableSelectorPanel.add(clusterPanel, gbc_clusterPanel);
		
		classifyPanel = new JPanel();
		GridBagConstraints gbc_classifyPanel = new GridBagConstraints();
		gbc_classifyPanel.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_classifyPanel.fill = GridBagConstraints.NONE;
		gbc_classifyPanel.gridwidth = 3;
		gbc_classifyPanel.insets = new Insets(5, 15, 0, 0);
		gbc_classifyPanel.gridx = 1;
		gbc_classifyPanel.gridy = 3;
		variableSelectorPanel.add(classifyPanel, gbc_classifyPanel);
		classifyPanel.setVisible(false);
		
		outlierPanel = new JPanel();
		GridBagConstraints gbc_outlierPanel = new GridBagConstraints();
		gbc_outlierPanel.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_outlierPanel.fill = GridBagConstraints.NONE;
		gbc_outlierPanel.gridwidth = 3;
		gbc_outlierPanel.insets = new Insets(5, 15, 0, 0);
		gbc_outlierPanel.gridx = 1;
		gbc_outlierPanel.gridy = 3;
		variableSelectorPanel.add(outlierPanel, gbc_outlierPanel);
		outlierPanel.setVisible(false);
		
		fillClusterPanel(clusterPanel);
		fillClassifyPanel(classifyPanel);
		fillOutlierPanel(outlierPanel);
		
		runAlgorithm = new CustomButton("Run Algorithm");
		runAlgorithm.setFont(new Font("Tahoma", Font.BOLD, 11));
		runAlgorithm.setPreferredSize(new Dimension(150, 25));
		GridBagConstraints gbc_runAlgorithm = new GridBagConstraints();
		gbc_runAlgorithm.insets = new Insets(10, 15, 0, 0);
		gbc_runAlgorithm.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_runAlgorithm.fill = GridBagConstraints.NONE;
		gbc_runAlgorithm.gridx = 1;
		gbc_runAlgorithm.gridy = 4;
		variableSelectorPanel.add(runAlgorithm, gbc_runAlgorithm);
		Style.registerTargetClassName(runAlgorithm,  ".createBtn");

		showDrillDownBtn = new ToggleButton("Drill Down on Specific Clusters");
		showDrillDownBtn.setName("showDrillDownBtn");
		showDrillDownBtn.setFont(new Font("Tahoma", Font.BOLD, 11));
		showDrillDownBtn.setPreferredSize(new Dimension(150, 25));
		showDrillDownBtn.setVisible(false);
		Style.registerTargetClassName(showDrillDownBtn,  ".toggleButton");
		
		GridBagConstraints gbc_showDrillDownBtn = new GridBagConstraints();
		gbc_showDrillDownBtn.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_showDrillDownBtn.fill = GridBagConstraints.NONE;
		gbc_showDrillDownBtn.insets = new Insets(10, 15, 0, 0);
		gbc_showDrillDownBtn.gridx = 2;
		gbc_showDrillDownBtn.gridy = 4;
		variableSelectorPanel.add(showDrillDownBtn, gbc_showDrillDownBtn);
		
		drillDownPanel = new JPanel();
		drillDownPanel.setBorder(new BevelBorder(BevelBorder.LOWERED, null, null, null, null));
		
		GridBagConstraints gbc_drillDownPanel = new GridBagConstraints();
		gbc_drillDownPanel.anchor = GridBagConstraints.FIRST_LINE_START;
		//gbc_drillDownPanel.fill = GridBagConstraints.BOTH;
		gbc_drillDownPanel.gridwidth = 2;
		gbc_drillDownPanel.insets = new Insets(15, 5, 0, 0);
		gbc_drillDownPanel.gridx = 1;
		gbc_drillDownPanel.gridy = 5;
		variableSelectorPanel.add(drillDownPanel, gbc_drillDownPanel);
		drillDownPanel.setVisible(false);

		fillDrillDownPanel(drillDownPanel);
		
		RunAlgorithmListener runListener = new RunAlgorithmListener();
		runListener.setView(this);
		runListener.setEntropyArr(entropyArr);
		runAlgorithm.addActionListener(runListener);
		
		ShowDrillDownListener drillDownListener = new ShowDrillDownListener();
		drillDownListener.setView(this);
		showDrillDownBtn.addActionListener(drillDownListener);
		
		ClusterTabSelectionListener drillDownSelectTabListener = new ClusterTabSelectionListener();
		drillDownSelectTabListener.setView(this);
		drillDownTabSelectorComboBox.addActionListener(drillDownSelectTabListener);
		
		RunDrillDownListener runDrillDownListener = new RunDrillDownListener();
		runDrillDownListener.setView(this);
		runDrillDown.addActionListener(runDrillDownListener);

		this.setPreferredSize(new Dimension(1000,750));
		new CSSApplication(getContentPane());
		pane.add(this);
		try {
			this.pack();
			this.setVisible(true);
			simBarChartPanel.callIt(simBarChartHash);			
			this.setSelected(false);
			this.setSelected(true);
		} catch (PropertyVetoException e) {
			LOGGER.error("Exception creating view");
		}
	
	}
	
	public void addSimBarChart() {
		simBarChartPanel = new BrowserGraphPanel("/html/MHS-RDFSemossCharts/app/columnchart.html");
		simBarChartPanel.setPreferredSize(new Dimension(500, 300));
		GridBagConstraints gbc_simBarChartPanel = new GridBagConstraints();
		gbc_simBarChartPanel.fill = GridBagConstraints.BOTH;
		gbc_simBarChartPanel.gridwidth = 3;
		gbc_simBarChartPanel.insets = new Insets(10, 5, 0, 0);
		gbc_simBarChartPanel.gridx = 0;
		gbc_simBarChartPanel.gridy = 1;
		variableSelectorPanel.add(simBarChartPanel, gbc_simBarChartPanel);
	}
	
	public void recreateSimBarChart(String[] names, ArrayList<Object[]> list) {
		BrowserGraphPanel oldSimBarChartPanel = simBarChartPanel;
		fillSimBarChartHash(names, list);
		addSimBarChart();
		variableSelectorPanel.remove(oldSimBarChartPanel);
		
		this.pack();
		this.setVisible(true);
		simBarChartPanel.callIt(simBarChartHash);
		try{
			this.setSelected(false);
			this.setSelected(true);
		} catch (PropertyVetoException e) {
			LOGGER.error("Exception creating similarity bar chart");
		}
	}
	
	private void fillIndependentVariablePanel(JPanel indVariablesPanel) {
		GridBagLayout gbl_indVariablesPanel = new GridBagLayout();
		gbl_indVariablesPanel.columnWidths = new int[]{0, 0, 0};
		gbl_indVariablesPanel.rowHeights = new int[]{0, 0, 0};
		gbl_indVariablesPanel.columnWeights = new double[]{1.0, 0.0, Double.MIN_VALUE};
		gbl_indVariablesPanel.rowWeights = new double[]{1.0, 0.0, Double.MIN_VALUE};
		indVariablesPanel.setLayout(gbl_indVariablesPanel);
		
		//select all, deselect all independent variables
		String checkboxSelectLabel = "Select All Variables";
		checkboxSelectAllIVs = new JCheckBox(checkboxSelectLabel);
		checkboxSelectAllIVs.setName(checkboxSelectLabel+"checkBox");
		checkboxSelectAllIVs.setSelected(true);
		GridBagConstraints gbc_checkboxSelectAllIVs = new GridBagConstraints();
		gbc_checkboxSelectAllIVs.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_checkboxSelectAllIVs.fill = GridBagConstraints.NONE;
		gbc_checkboxSelectAllIVs.insets = new Insets(5, 20, 0, 0);
		gbc_checkboxSelectAllIVs.gridx = 0;
		gbc_checkboxSelectAllIVs.gridy = 0;
		indVariablesPanel.add(checkboxSelectAllIVs, gbc_checkboxSelectAllIVs);

		JLabel includeLabel = new JLabel();
		includeLabel.setText("Variables");
		includeLabel.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_includeLabel = new GridBagConstraints();
		gbc_includeLabel.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_includeLabel.fill = GridBagConstraints.NONE;
		gbc_includeLabel.gridwidth = 2;
		gbc_includeLabel.insets = new Insets(5, 20, 0, 0);
		gbc_includeLabel.gridx = 0;
		gbc_includeLabel.gridy = 1;
		indVariablesPanel.add(includeLabel, gbc_includeLabel);
		
		JLabel entropyLabel = new JLabel();
		entropyLabel.setText("Entropy");
		entropyLabel.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_entropyLabel = new GridBagConstraints();
		gbc_entropyLabel.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_entropyLabel.fill = GridBagConstraints.NONE;
		gbc_entropyLabel.insets = new Insets(5, 20, 0, 0);
		gbc_entropyLabel.gridx = 2;
		gbc_entropyLabel.gridy = 1;
		indVariablesPanel.add(entropyLabel, gbc_entropyLabel);
		
		JLabel accuracyLabel = new JLabel();
		accuracyLabel.setText("Accuracy");
		accuracyLabel.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_accuracyLabel = new GridBagConstraints();
		gbc_accuracyLabel.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_accuracyLabel.fill = GridBagConstraints.NONE;
		gbc_accuracyLabel.insets = new Insets(5, 20, 0, 0);
		gbc_accuracyLabel.gridx = 3;
		gbc_accuracyLabel.gridy = 1;
		indVariablesPanel.add(accuracyLabel, gbc_accuracyLabel);
		
		JLabel precisionLabel = new JLabel();
		precisionLabel.setText("Precision");
		precisionLabel.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_precisionLabel = new GridBagConstraints();
		gbc_precisionLabel.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_precisionLabel.fill = GridBagConstraints.NONE;
		gbc_precisionLabel.insets = new Insets(5, 20, 0, 0);
		gbc_precisionLabel.gridx = 4;
		gbc_precisionLabel.gridy = 1;
		indVariablesPanel.add(precisionLabel, gbc_precisionLabel);
		
		ivCheckboxes = new ArrayList<JCheckBox>();
		entropyLabels = new ArrayList<JLabel>();

		DecimalFormat formatter = new DecimalFormat("#0.00");
		
		for(int i=1;i<names.length;i++) {
			String checkboxLabel = names[i];
			JCheckBox checkbox = new JCheckBox(checkboxLabel);
			checkbox.setName(checkboxLabel+checkboxName);
			checkbox.setSelected(true);
			GridBagConstraints gbc_checkbox = new GridBagConstraints();
			gbc_checkbox.anchor = GridBagConstraints.FIRST_LINE_START;
			gbc_checkbox.gridwidth = 2;
			gbc_checkbox.fill = GridBagConstraints.NONE;
			gbc_checkbox.insets = new Insets(5, 20, 0, 0);
			gbc_checkbox.gridx = 0;
			gbc_checkbox.gridy = i+1;
			indVariablesPanel.add(checkbox, gbc_checkbox);
			ivCheckboxes.add(checkbox);
				
			JLabel entropyVal = new JLabel();
			entropyVal.setText(formatter.format(entropyArr[i-1]));
			GridBagConstraints gbc_entropyVal = new GridBagConstraints();
			gbc_entropyVal.anchor = GridBagConstraints.FIRST_LINE_START;
			gbc_entropyVal.fill = GridBagConstraints.NONE;
			gbc_entropyVal.insets = new Insets(5, 20, 0, 0);
			gbc_entropyVal.gridx = 2;
			gbc_entropyVal.gridy = i+1;
			indVariablesPanel.add(entropyVal, gbc_entropyVal);
			entropyLabels.add(entropyVal);
		}
		
		selectAllIVsList = new SelectCheckboxesListener();
		selectAllIVsList.setCheckboxes(ivCheckboxes);
		checkboxSelectAllIVs.addActionListener(selectAllIVsList);
		
		accuracyLabels = new ArrayList<JLabel>(names.length);
		precisionLabels = new ArrayList<JLabel>(names.length);
	}
	
	public void fillAccuracyAndPrecision(double[] accuracyArr, double[] precisionArr) {
		DecimalFormat formatter = new DecimalFormat("#0.00");
		
		boolean[] includeColArr = new boolean[ivCheckboxes.size()];
		includeColArr[0] = true; //this is the "title" or "name"
		for(int i = 0; i < ivCheckboxes.size(); i++) {
			JCheckBox checkbox = ivCheckboxes.get(i);
			includeColArr[i] = checkbox.isSelected();
		}

		for(int i = 0; i < names.length; i++) {
			if(includeColArr[i]) {
				try {
					// try to remove old values if present
					indVariablesPanel.remove(accuracyLabels.get(i));
					indVariablesPanel.remove(precisionLabels.get(i));
					accuracyLabels.remove(i);
					precisionLabels.remove(i);
				} catch (IndexOutOfBoundsException ex) {
					// do nothing
				}				
				JLabel accuracyVal = new JLabel();
				accuracyVal.setText(formatter.format(accuracyArr[i])+"%");
				GridBagConstraints gbc_accuracyVal = new GridBagConstraints();
				gbc_accuracyVal.anchor = GridBagConstraints.FIRST_LINE_START;
				gbc_accuracyVal.fill = GridBagConstraints.NONE;
				gbc_accuracyVal.insets = new Insets(5, 20, 0, 0);
				gbc_accuracyVal.gridx = 3;
				gbc_accuracyVal.gridy = i+2;
				indVariablesPanel.add(accuracyVal, gbc_accuracyVal);
				accuracyLabels.add(i, accuracyVal);
				
				JLabel precisionVal = new JLabel();
				precisionVal.setText(formatter.format(precisionArr[i])+"%");
				GridBagConstraints gbc_precisionVal = new GridBagConstraints();
				gbc_precisionVal.anchor = GridBagConstraints.FIRST_LINE_START;
				gbc_precisionVal.fill = GridBagConstraints.NONE;
				gbc_precisionVal.insets = new Insets(5, 20, 0, 0);
				gbc_precisionVal.gridx = 4;
				gbc_precisionVal.gridy = i+2;
				indVariablesPanel.add(precisionVal, gbc_precisionVal);
				precisionLabels.add(i, precisionVal);
			} else {
				try {
					accuracyLabels.get(i);
				} catch (IndexOutOfBoundsException ex) {
					// if value isn't present
					JLabel accuracyVal = new JLabel();
					accuracyVal.setText("Not Selected");
					GridBagConstraints gbc_accuracyVal = new GridBagConstraints();
					gbc_accuracyVal.anchor = GridBagConstraints.FIRST_LINE_START;
					gbc_accuracyVal.fill = GridBagConstraints.NONE;
					gbc_accuracyVal.insets = new Insets(5, 20, 0, 0);
					gbc_accuracyVal.gridx = 3;
					gbc_accuracyVal.gridy = i+2;
					indVariablesPanel.add(accuracyVal, gbc_accuracyVal);
					accuracyLabels.add(i, accuracyVal);
					
					JLabel precisionVal = new JLabel();
					precisionVal.setText("Not Selected");
					GridBagConstraints gbc_precisionVal = new GridBagConstraints();
					gbc_precisionVal.anchor = GridBagConstraints.FIRST_LINE_START;
					gbc_precisionVal.fill = GridBagConstraints.NONE;
					gbc_precisionVal.insets = new Insets(5, 20, 0, 0);
					gbc_precisionVal.gridx = 4;
					gbc_precisionVal.gridy = i+2;
					indVariablesPanel.add(precisionVal, gbc_precisionVal);
					precisionLabels.add(i, precisionVal);
				}
			}
		}
		indVariablesPanel.revalidate();
		indVariablesPanel.repaint();
	}
	
	private void fillClusterPanel(JPanel clusterPanel) {
		GridBagLayout gbl_clusterPanel = new GridBagLayout();
		gbl_clusterPanel.columnWidths = new int[]{0, 0, 0};
		gbl_clusterPanel.rowHeights = new int[]{0, 0, 0};
		gbl_clusterPanel.columnWeights = new double[]{1.0, 0.0, Double.MIN_VALUE};
		gbl_clusterPanel.rowWeights = new double[]{1.0, 0.0, Double.MIN_VALUE};
		clusterPanel.setLayout(gbl_clusterPanel);
		
		lblSelectNumClusters = new JLabel("Select number of clusters:");
		lblSelectNumClusters.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_lblSelectNumClusters = new GridBagConstraints();
		gbc_lblSelectNumClusters.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_lblSelectNumClusters.fill = GridBagConstraints.NONE;
		gbc_lblSelectNumClusters.insets = new Insets(10, 5, 0, 0);
		gbc_lblSelectNumClusters.gridx = 0;
		gbc_lblSelectNumClusters.gridy = 0;
		clusterPanel.add(lblSelectNumClusters, gbc_lblSelectNumClusters);

		selectNumClustersComboBox = new JComboBox<String>();
		selectNumClustersComboBox.setFont(new Font("Tahoma", Font.PLAIN, 11));
		selectNumClustersComboBox.setBackground(Color.GRAY);
		selectNumClustersComboBox.setPreferredSize(new Dimension(225, 25));
		String[] dropDownVals = new String[] {automaticallySelectNumClustersText,manuallySelectNumClustersText};
		selectNumClustersComboBox.setModel(new DefaultComboBoxModel<String>(dropDownVals));
		GridBagConstraints gbc_selectNumClustersComboBox = new GridBagConstraints();
		gbc_selectNumClustersComboBox.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_selectNumClustersComboBox.fill = GridBagConstraints.NONE;
		gbc_selectNumClustersComboBox.insets = new Insets(5, 5, 0, 0);
		gbc_selectNumClustersComboBox.gridx = 1;
		gbc_selectNumClustersComboBox.gridy = 0;
		clusterPanel.add(selectNumClustersComboBox, gbc_selectNumClustersComboBox);
		
		selectNumClustersTextField = new JTextField();
		selectNumClustersTextField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		selectNumClustersTextField.setText("2");
		selectNumClustersTextField.setColumns(4);
		selectNumClustersTextField.setVisible(false);
		GridBagConstraints gbc_selectNumClustersTextField = new GridBagConstraints();
		gbc_selectNumClustersTextField.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_selectNumClustersTextField.fill = GridBagConstraints.NONE;
		gbc_selectNumClustersTextField.insets = new Insets(5, 5, 0, 0);
		gbc_selectNumClustersTextField.gridx = 2;
		gbc_selectNumClustersTextField.gridy = 0;
		clusterPanel.add(selectNumClustersTextField, gbc_selectNumClustersTextField);
		
		NumberOfClustersSelectionListener numClustersSelectList = new NumberOfClustersSelectionListener();
		numClustersSelectList.setView(this);
		selectNumClustersComboBox.addActionListener(numClustersSelectList);
	}
	
	private void fillClassifyPanel(JPanel classifyPanel) {
		
		GridBagLayout gbl_classifyPanel = new GridBagLayout();
		gbl_classifyPanel.columnWidths = new int[]{0, 0, 0};
		gbl_classifyPanel.rowHeights = new int[]{0, 0, 0};
		gbl_classifyPanel.columnWeights = new double[]{1.0, 0.0, Double.MIN_VALUE};
		gbl_classifyPanel.rowWeights = new double[]{1.0, 0.0, Double.MIN_VALUE};
		classifyPanel.setLayout(gbl_classifyPanel);
		
		lblSelectClass = new JLabel("Select class:");
		lblSelectClass.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_lblSelectClass = new GridBagConstraints();
		gbc_lblSelectClass.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_lblSelectClass.fill = GridBagConstraints.NONE;
		gbc_lblSelectClass.insets = new Insets(10, 5, 0, 0);
		gbc_lblSelectClass.gridx = 0;
		gbc_lblSelectClass.gridy = 0;
		classifyPanel.add(lblSelectClass, gbc_lblSelectClass);

		classComboBox = new JComboBox<String>();
		classComboBox.setFont(new Font("Tahoma", Font.PLAIN, 11));
		classComboBox.setBackground(Color.GRAY);
		classComboBox.setPreferredSize(new Dimension(250, 25));
		String[] cols = new String[names.length-1];
		for(int i=1;i<names.length;i++)
			cols[i-1] = names[i];
		classComboBox.setModel(new DefaultComboBoxModel<String>(cols));
		GridBagConstraints gbc_classComboBox = new GridBagConstraints();
		gbc_classComboBox.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_classComboBox.fill = GridBagConstraints.NONE;
		gbc_classComboBox.insets = new Insets(5, 5, 0, 0);
		gbc_classComboBox.gridx = 1;
		gbc_classComboBox.gridy = 0;
		classifyPanel.add(classComboBox, gbc_classComboBox);
		ClassificationSelectionListener classSelectList = new ClassificationSelectionListener();
		classSelectList.setView(this);
		classComboBox.addActionListener(classSelectList);
		
		lblSelectClassMethod = new JLabel("Select classification Method:");
		lblSelectClassMethod.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_lblSelectClassMethod = new GridBagConstraints();
		gbc_lblSelectClassMethod.anchor = GridBagConstraints.FIRST_LINE_END;
		gbc_lblSelectClassMethod.fill = GridBagConstraints.NONE;
		gbc_lblSelectClassMethod.insets = new Insets(10, 5, 0, 0);
		gbc_lblSelectClassMethod.gridx = 0;
		gbc_lblSelectClassMethod.gridy = 1;
		classifyPanel.add(lblSelectClassMethod, gbc_lblSelectClassMethod);

		classificationMethodComboBox = new JComboBox<String>();
		classificationMethodComboBox.setFont(new Font("Tahoma", Font.PLAIN, 11));
		classificationMethodComboBox.setBackground(Color.GRAY);
		classificationMethodComboBox.setPreferredSize(new Dimension(250, 25));
		String[] classTypes = new String[] {"J48","J48GRAFT","SIMPLECART","REPTREE","BFTREE"};
		classificationMethodComboBox.setModel(new DefaultComboBoxModel<String>(classTypes));
		GridBagConstraints gbc_classificationMethodComboBox = new GridBagConstraints();
		gbc_classificationMethodComboBox.anchor = GridBagConstraints.FIRST_LINE_END;
		gbc_classificationMethodComboBox.fill = GridBagConstraints.NONE;
		gbc_classificationMethodComboBox.insets = new Insets(5, 5, 0, 0);
		gbc_classificationMethodComboBox.gridx = 1;
		gbc_classificationMethodComboBox.gridy = 1;
		classifyPanel.add(classificationMethodComboBox, gbc_classificationMethodComboBox);

	}
	private void fillOutlierPanel(JPanel outlierPanel) {
	
		GridBagLayout gbl_outlierPanel = new GridBagLayout();
		gbl_outlierPanel.columnWidths = new int[]{0, 0, 0};
		gbl_outlierPanel.rowHeights = new int[]{0, 0, 0};
		gbl_outlierPanel.columnWeights = new double[]{1.0, 0.0, Double.MIN_VALUE};
		gbl_outlierPanel.rowWeights = new double[]{1.0, 0.0, Double.MIN_VALUE};
		outlierPanel.setLayout(gbl_outlierPanel);
		
		lblEnterKNeighbors = new JLabel("Enter K neighbors:");
		lblEnterKNeighbors.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_lblEnterKNeighbors = new GridBagConstraints();
		gbc_lblEnterKNeighbors.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_lblEnterKNeighbors.fill = GridBagConstraints.NONE;
		gbc_lblEnterKNeighbors.insets = new Insets(10, 5, 0, 0);
		gbc_lblEnterKNeighbors.gridx = 0;
		gbc_lblEnterKNeighbors.gridy = 0;
		outlierPanel.add(lblEnterKNeighbors, gbc_lblEnterKNeighbors);
		
		enterKNeighborsSlider = new JSlider(5,45,25);
		enterKNeighborsSlider.setFont(new Font("Tahoma", Font.PLAIN, 11));
		enterKNeighborsSlider.setMajorTickSpacing(5);
		enterKNeighborsSlider.setMinorTickSpacing(1);
		enterKNeighborsSlider.setPaintLabels(true);
		enterKNeighborsSlider.setPaintTicks(true);
		enterKNeighborsSlider.setPreferredSize(new Dimension(400,40));
		//enterKNeighborsSlider.set
//		enterKNeighborsTextField.setText("5");
//		enterKNeighborsTextField.setColumns(4);
		enterKNeighborsSlider.setVisible(false);
		GridBagConstraints gbc_getEnterKNeighborsSlider = new GridBagConstraints();
		gbc_getEnterKNeighborsSlider.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_getEnterKNeighborsSlider.fill = GridBagConstraints.NONE;
		gbc_getEnterKNeighborsSlider.insets = new Insets(5, 5, 0, 0);
		gbc_getEnterKNeighborsSlider.gridx = 0;
		gbc_getEnterKNeighborsSlider.gridy = 1;
		outlierPanel.add(enterKNeighborsSlider, gbc_getEnterKNeighborsSlider);
	}

	public void fillDrillDownPanel(JPanel drillDownPanel) {
		GridBagLayout gbl_drillDownPanel = new GridBagLayout();
		gbl_drillDownPanel.columnWidths = new int[]{0, 0, 0};
		gbl_drillDownPanel.rowHeights = new int[]{0, 0, 0};
		gbl_drillDownPanel.columnWeights = new double[]{0.0, 0.0, 1.0};
		gbl_drillDownPanel.rowWeights = new double[]{0.0, 0.0, 1.0};
		drillDownPanel.setLayout(gbl_drillDownPanel);
		
		lblDrillDownSelectTab = new JLabel("Select clustering run:");
		lblDrillDownSelectTab.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_lblDrillDownSelectTab = new GridBagConstraints();
		gbc_lblDrillDownSelectTab.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_lblDrillDownSelectTab.fill = GridBagConstraints.NONE;
		gbc_lblDrillDownSelectTab.insets = new Insets(10, 5, 0, 0);
		gbc_lblDrillDownSelectTab.gridx = 0;
		gbc_lblDrillDownSelectTab.gridy = 0;
		drillDownPanel.add(lblDrillDownSelectTab, gbc_lblDrillDownSelectTab);
		lblDrillDownSelectTab.setVisible(false);
		
		drillDownTabSelectorComboBox = new JComboBox<String>();
		drillDownTabSelectorComboBox.setFont(new Font("Tahoma", Font.PLAIN, 11));
		drillDownTabSelectorComboBox.setBackground(Color.GRAY);
		drillDownTabSelectorComboBox.setPreferredSize(new Dimension(250, 25));
		drillDownTabSelectorComboBox.setModel(new DefaultComboBoxModel<String>(new String[] {}));
		GridBagConstraints gbc_drillDownTabSelectorComboBox = new GridBagConstraints();
		gbc_drillDownTabSelectorComboBox.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_drillDownTabSelectorComboBox.fill = GridBagConstraints.NONE;
		gbc_drillDownTabSelectorComboBox.insets = new Insets(5, 5, 0, 0);
		gbc_drillDownTabSelectorComboBox.gridx = 1;
		gbc_drillDownTabSelectorComboBox.gridy = 0;
		drillDownPanel.add(drillDownTabSelectorComboBox, gbc_drillDownTabSelectorComboBox);
		drillDownTabSelectorComboBox.setVisible(false);
		
		clusterCheckBoxPanel = new JPanel();
		GridBagConstraints gbc_clusterCheckBoxPanel = new GridBagConstraints();
		gbc_clusterCheckBoxPanel.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_clusterCheckBoxPanel.fill = GridBagConstraints.NONE;
		gbc_clusterCheckBoxPanel.gridwidth = 2;
		gbc_clusterCheckBoxPanel.gridx = 0;
		gbc_clusterCheckBoxPanel.gridy = 1;
		drillDownPanel.add(clusterCheckBoxPanel, gbc_clusterCheckBoxPanel);
		
		GridBagLayout gbl_clusterCheckBoxPanel = new GridBagLayout();
		gbl_clusterCheckBoxPanel.columnWidths = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
		gbl_clusterCheckBoxPanel.rowHeights = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
		gbl_clusterCheckBoxPanel.columnWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
		gbl_clusterCheckBoxPanel.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
		clusterCheckBoxPanel.setLayout(gbl_clusterCheckBoxPanel);
		
		JLabel lblClusterSelect = new JLabel("Select clusters to include:");
		lblClusterSelect.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_lblClusterSelect = new GridBagConstraints();
		gbc_lblClusterSelect.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_lblClusterSelect.fill = GridBagConstraints.NONE;
		//gbc_lblClusterSelect.gridwidth = 6;
		gbc_lblClusterSelect.insets = new Insets(10, 5, 0, 0);
		gbc_lblClusterSelect.gridx = 0;
		gbc_lblClusterSelect.gridy = 0;
		clusterCheckBoxPanel.add(lblClusterSelect, gbc_lblClusterSelect);
		
		//select all, deselect all
		String checkboxSelectLabel = "Select All";
		checkboxSelectAllClusters = new JCheckBox(checkboxSelectLabel);
		checkboxSelectAllClusters.setName(checkboxSelectLabel+"checkBox");
		checkboxSelectAllClusters.setSelected(true);
		GridBagConstraints gbc_checkboxSelect = new GridBagConstraints();
		gbc_checkboxSelect.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_checkboxSelect.fill = GridBagConstraints.NONE;
		gbc_checkboxSelect.gridwidth = 6;
		gbc_checkboxSelect.insets = new Insets(10, 10, 0, 0);
		gbc_checkboxSelect.gridx = 1;
		gbc_checkboxSelect.gridy = 0;
		clusterCheckBoxPanel.add(checkboxSelectAllClusters, gbc_checkboxSelect);

		selectAllClustersList = new SelectCheckboxesListener();
		selectAllClustersList.setCheckboxes(clusterCheckboxes);
		checkboxSelectAllClusters.addActionListener(selectAllClustersList);
		
		runDrillDown = new CustomButton("Drill Down");
		runDrillDown.setFont(new Font("Tahoma", Font.BOLD, 11));
		runDrillDown.setPreferredSize(new Dimension(150, 25));
		GridBagConstraints gbc_runDrillDown = new GridBagConstraints();
		gbc_runDrillDown.insets = new Insets(10, 5, 0, 40);
		gbc_runDrillDown.anchor = GridBagConstraints.FIRST_LINE_END;
		gbc_runDrillDown.fill = GridBagConstraints.NONE;
		gbc_runDrillDown.gridx = 1;
		gbc_runDrillDown.gridy = 2;
		drillDownPanel.add(runDrillDown, gbc_runDrillDown);
		Style.registerTargetClassName(runDrillDown,  ".createBtn");
		
	}
	

	public void showCluster(Boolean show) {
		clusterPanel.setVisible(show);
		lblSelectNumClusters.setVisible(show);
		selectNumClustersComboBox.setVisible(show);
		selectNumClustersTextField.setVisible(show);
		selectNumClustersComboBox.setSelectedItem(automaticallySelectNumClustersText);
	}
	public void showClassify(Boolean show) {
		classifyPanel.setVisible(show);
		lblSelectClass.setVisible(show);
		lblSelectClassMethod.setVisible(show);
		classComboBox.setVisible(show);
		classificationMethodComboBox.setVisible(show);
		if(show) {
			String selection = classComboBox.getSelectedItem() + "";
			setDisabledCheckBox(selection);
		} else
			setDisabledCheckBox("");//all checkboxes will be enabled
	}
	public void showOutlier(Boolean show) {
		outlierPanel.setVisible(show);
		lblEnterKNeighbors.setVisible(show);
		enterKNeighborsSlider.setVisible(show);
	}
	public void enableDrillDown() {
		showDrillDownBtn.setEnabled(true);
	}
	public void showDrillDownPanel(Boolean show) {
		drillDownPanel.setVisible(show);
		lblDrillDownSelectTab.setVisible(show);
		drillDownTabSelectorComboBox.setVisible(show);
		checkboxSelectAllClusters.setVisible(show);
		//button to run drill down
		if(show){
			drillDownTabSelectorComboBox.setSelectedIndex(0);
			String tabName = (String) drillDownTabSelectorComboBox.getSelectedItem();
			ClusteringVizPlaySheet playSheet = (ClusteringVizPlaySheet) playSheetHash.get(tabName);
			int clusters = playSheet.getNumClusters();			
			updateClusterCheckboxes(clusters);
			resetClusterCheckboxesListener();
		} else {
			updateClusterCheckboxes(0); //remove all the checkboxes
		}
		
	}
	
	public void updateClusterCheckboxes(int numClusters) {
		//remove old clusters
		for(JCheckBox checkbox : clusterCheckboxes) {
			clusterCheckBoxPanel.remove(checkbox);
		}
		clusterCheckboxes = new ArrayList<JCheckBox>();
		int numColumns = 6;
		for(int i=0;i<numClusters;i+=numColumns) {
			for(int j=i;j<i+numColumns;j++) {
				if(j<numClusters) {
				String checkboxLabel = "" + j;
				JCheckBox checkbox = new JCheckBox(checkboxLabel);
				checkbox.setName("Cluster "+checkboxLabel+"checkBox");
				checkbox.setSelected(true);
				GridBagConstraints gbc_checkbox = new GridBagConstraints();
				gbc_checkbox.anchor = GridBagConstraints.NORTHWEST;
				gbc_checkbox.fill = GridBagConstraints.NONE;
				gbc_checkbox.insets = new Insets(5, 10, 0, 0);
				gbc_checkbox.gridx = j % numColumns + 1;
				gbc_checkbox.gridy = j / numColumns + 2;
				clusterCheckBoxPanel.add(checkbox, gbc_checkbox);
				clusterCheckboxes.add(checkbox);
				}
			}
		}
	}
	
	public void resetClusterCheckboxesListener() {
		selectAllClustersList.setCheckboxes(clusterCheckboxes);
		checkboxSelectAllClusters.setSelected(true);
	}
	
	public void setSelectedColumns(String[] selectedCols) {
		for(JCheckBox checkbox : ivCheckboxes) {
			checkbox.setSelected(false);
		}
		for(String checkboxToSelect : selectedCols) {
			for(JCheckBox checkbox : ivCheckboxes) {
				String name = checkbox.getName();
				if(name.equals(checkboxToSelect + checkboxName))
					checkbox.setSelected(true);
			}
		}
	}
	public void setDisabledCheckBox(String checkboxToDisable){
		for(JCheckBox checkbox : ivCheckboxes) {
			String name = checkbox.getName();
			if(name.equals(checkboxToDisable + checkboxName)) {
				checkbox.setSelected(true);
				checkbox.setEnabled(false);
			} else {
				checkbox.setEnabled(true);
			}
		}
	}

	public void showSelectNumClustersTextField(Boolean show) {
		selectNumClustersTextField.setVisible(show);
	}
	@Override
	public void setQuery(String query) {
		String[] querySplit = query.split("\\+\\+\\+");
		this.query = querySplit[0];
	}
	public JComboBox<String> getAlgorithmComboBox() {
		return algorithmComboBox;
	}
	public JComboBox<String> getClassificationMethodComboBox() {
		return classificationMethodComboBox;
	}
	public JComboBox<String> getClassComboBox() {
		return classComboBox;
	}
	public ArrayList<JCheckBox> getColumnCheckboxes() {
		return ivCheckboxes;
	}
	public ArrayList<JCheckBox> getClusterCheckboxes() {
		return clusterCheckboxes;
	}
	public JComboBox<String> getDrillDownTabSelectorComboBox() {
		return drillDownTabSelectorComboBox;
	}
	public JComboBox<String> getSelectNumClustersComboBox() {
		return selectNumClustersComboBox;
	}
	public String getAutomaticallySelectNumClustersText() {
		return automaticallySelectNumClustersText;
	}
	public String getManuallySelectNumClustersText() {
		return manuallySelectNumClustersText;
	}
	public JTextField getSelectNumClustersTextField() {
		return selectNumClustersTextField;
	}
	public JSlider getEnterKNeighborsSlider() {
		return enterKNeighborsSlider;
	}
	public JToggleButton getShowDrillDownBtn() {
		return showDrillDownBtn;
	}
	public String[] getNames() {
		return names;
	}
	public ArrayList<Object[]> getList() {
		return list;
	}
	public JTabbedPane getJTab() {
		return jTab;
	}
	public JProgressBar getJBar() {
		return jBar;
	}
	public Hashtable<String, IPlaySheet> getPlaySheetHash(){
		return playSheetHash;
	}
}