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
import javax.swing.JTabbedPane;
import javax.swing.JToggleButton;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.cluster.GenerateEntropyDensity;
import prerna.algorithm.weka.impl.WekaClassification;
import prerna.algorithm.weka.impl.WekaUtilityMethods;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.NewScrollBarUI;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.main.listener.impl.AlgorithmSelectionListener;
import prerna.ui.main.listener.impl.ClassificationSelectionListener;
import prerna.ui.main.listener.impl.ClusterTabSelectionListener;
import prerna.ui.main.listener.impl.RunAlgorithmListener;
import prerna.ui.main.listener.impl.RunDrillDownListener;
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
	private double[] entropyArr;
	private double[] accuracyArr;
	private double[] precisionArr;
	
	private JTabbedPane jTab;
	private String checkboxName = "checkBox";
	private JComboBox<String> algorithmComboBox;
	private JPanel clusterPanel;
	private JPanel classifyPanel;
	private JComboBox<String> classificationMethodComboBox;
	private JComboBox<String> classComboBox;
	private JLabel lblSelectClass, lblSelectClassMethod;
	private JButton runAlgorithm, runDrillDown;
	
	private JToggleButton showDrillDownBtn;
	private JPanel drillDownPanel;
	private JLabel lblDrillDownSelectTab;
	private JComboBox<String> drillDownTabSelectorComboBox;
	private JPanel clusterCheckBoxPanel;
	private ArrayList<JCheckBox> clusterCheckboxes = new ArrayList<JCheckBox>();
	
	Hashtable<String, IPlaySheet> playSheetHash = new Hashtable<String,IPlaySheet>();
	
	private ArrayList<JCheckBox> columnCheckboxes;
	private ArrayList<JLabel> entropyLabels;
	private ArrayList<JLabel> accuracyLabels;
	private ArrayList<JLabel> precisionLabels;
	
	public void setData(String[] names, ArrayList<Object[]> list) {
		this.list = list;
		this.names=names;
	}
	
	@Override
	public void runAnalytics() {
		//if nothing there return? TODO make sure this is the right way to handle this exception
		if(list==null)
			return;
		
		GenerateEntropyDensity test = new GenerateEntropyDensity(list,true);
		entropyArr = test.generateEntropy();
		
		accuracyArr = new double[names.length-1];
		precisionArr = new double[names.length-1];
		for(int i=1; i < names.length; i++) {
			WekaClassification weka = new WekaClassification(list, names, "J48", i);
			try {
				weka.execute();
				double accuracy = WekaUtilityMethods.calculateAccuracy(weka.getAccuracyArr());
				double precision = WekaUtilityMethods.calculatePercision(weka.getPrecisionArr());
				accuracyArr[i-1] = accuracy;
				precisionArr[i-1] = precision;
			} catch (Exception e) {
				LOGGER.error("Could not generate accuracy and precision values from WEKA classification");
			}
		}
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
	public void addPanel() {//TODO add control panel
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
		JPanel variableSelectorPanel = new JPanel();
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
		gbl_panel.columnWeights = new double[]{0.0, 1.0, 0.0};
		gbl_panel.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0};
		variableSelectorPanel.setLayout(gbl_panel);

		JLabel indVariablesLabel = new JLabel();
		indVariablesLabel.setText("Select Independent Variables to include in analysis");
		indVariablesLabel.setFont(new Font("Tahoma", Font.BOLD, 16));
		GridBagConstraints gbc_indVariablesLabel = new GridBagConstraints();
		gbc_indVariablesLabel.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_indVariablesLabel.fill = GridBagConstraints.NONE;
	//	gbc_indVariablesLabel.gridwidth = 2;
		gbc_indVariablesLabel.insets = new Insets(10, 5, 0, 0);
		gbc_indVariablesLabel.gridx = 0;
		gbc_indVariablesLabel.gridy = 0;
		variableSelectorPanel.add(indVariablesLabel, gbc_indVariablesLabel);
		
		JPanel indVariablesPanel = new JPanel();
		GridBagConstraints gbc_indVariablesPanel = new GridBagConstraints();
		gbc_indVariablesPanel.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_indVariablesPanel.fill = GridBagConstraints.NONE;
	//	gbc_indVariablesPanel.gridwidth = 2;
		gbc_indVariablesPanel.insets = new Insets(10, 5, 0, 0);
		gbc_indVariablesPanel.gridx = 0;
		gbc_indVariablesPanel.gridy = 1;
		variableSelectorPanel.add(indVariablesPanel, gbc_indVariablesPanel);
		
		fillIndependentVariablePanel(indVariablesPanel);

		JLabel lblSelectAnalysis = new JLabel("Select analysis to perform:");
		lblSelectAnalysis.setFont(new Font("Tahoma", Font.BOLD, 16));
		GridBagConstraints gbc_lblSelectAnalysis = new GridBagConstraints();
		gbc_lblSelectAnalysis.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_lblSelectAnalysis.fill = GridBagConstraints.NONE;
		gbc_lblSelectAnalysis.insets = new Insets(30, 5, 0, 0);
		gbc_lblSelectAnalysis.gridx = 0;
		gbc_lblSelectAnalysis.gridy = 2;
		variableSelectorPanel.add(lblSelectAnalysis, gbc_lblSelectAnalysis);

		algorithmComboBox = new JComboBox<String>();
		algorithmComboBox.setFont(new Font("Tahoma", Font.PLAIN, 11));
		algorithmComboBox.setBackground(Color.GRAY);
		algorithmComboBox.setPreferredSize(new Dimension(100, 25));
		algorithmComboBox.setModel(new DefaultComboBoxModel<String>(new String[] {"Cluster", "Classify"}));
		GridBagConstraints gbc_algorithmComboBox = new GridBagConstraints();
		gbc_algorithmComboBox.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_algorithmComboBox.fill = GridBagConstraints.NONE;
		gbc_algorithmComboBox.insets = new Insets(10, 5, 0, 0);
		gbc_algorithmComboBox.gridx = 0;
		gbc_algorithmComboBox.gridy = 3;
		variableSelectorPanel.add(algorithmComboBox, gbc_algorithmComboBox);
		AlgorithmSelectionListener algSelectList = new AlgorithmSelectionListener();
		algSelectList.setView(this);
		algorithmComboBox.addActionListener(algSelectList);
		
		//add in cluster and classify panels
		//show the cluster as default
		clusterPanel = new JPanel();
		clusterPanel.setMinimumSize(new Dimension(400, 100));
		GridBagConstraints gbc_clusterPanel = new GridBagConstraints();
		gbc_clusterPanel.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_clusterPanel.fill = GridBagConstraints.NONE;
		gbc_clusterPanel.insets = new Insets(10, 5, 0, 0);
		gbc_clusterPanel.gridx = 0;
		gbc_clusterPanel.gridy = 4;
		variableSelectorPanel.add(clusterPanel, gbc_clusterPanel);
		
		classifyPanel = new JPanel();
		classifyPanel.setMinimumSize(new Dimension(400, 100));
		GridBagConstraints gbc_classifyPanel = new GridBagConstraints();
		gbc_classifyPanel.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_classifyPanel.fill = GridBagConstraints.NONE;
		gbc_classifyPanel.insets = new Insets(10, 5, 0, 0);
		gbc_classifyPanel.gridx = 0;
		gbc_classifyPanel.gridy = 4;
		variableSelectorPanel.add(classifyPanel, gbc_classifyPanel);
		classifyPanel.setVisible(false);
		
		fillClusterPanel(clusterPanel);
		fillClassifyPanel(classifyPanel);
		
		runAlgorithm = new CustomButton("Run Algorithm");
		runAlgorithm.setFont(new Font("Tahoma", Font.BOLD, 11));
		runAlgorithm.setPreferredSize(new Dimension(150, 25));
		GridBagConstraints gbc_runAlgorithm = new GridBagConstraints();
		gbc_runAlgorithm.insets = new Insets(10, 5, 0, 40);
		gbc_runAlgorithm.anchor = GridBagConstraints.FIRST_LINE_END;
		gbc_runAlgorithm.fill = GridBagConstraints.NONE;
		gbc_runAlgorithm.gridx = 0;
		gbc_runAlgorithm.gridy = 5;
		variableSelectorPanel.add(runAlgorithm, gbc_runAlgorithm);
		Style.registerTargetClassName(runAlgorithm,  ".createBtn");

		showDrillDownBtn = new ToggleButton("Drill Down on Clusters");
		showDrillDownBtn.setName("showDrillDownBtn");
		showDrillDownBtn.setFont(new Font("Tahoma", Font.BOLD, 11));
		showDrillDownBtn.setVisible(false);
		Style.registerTargetClassName(showDrillDownBtn,  ".toggleButton");
		
		GridBagConstraints gbc_showDrillDownBtn = new GridBagConstraints();
		gbc_showDrillDownBtn.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_showDrillDownBtn.fill = GridBagConstraints.NONE;
		gbc_showDrillDownBtn.insets = new Insets(10, 5, 0, 0);
		gbc_showDrillDownBtn.gridx = 0;
		gbc_showDrillDownBtn.gridy = 6;
		variableSelectorPanel.add(showDrillDownBtn, gbc_showDrillDownBtn);
		
		drillDownPanel = new JPanel();
		GridBagConstraints gbc_drillDownPanel = new GridBagConstraints();
		gbc_drillDownPanel.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_drillDownPanel.fill = GridBagConstraints.VERTICAL;
		gbc_drillDownPanel.insets = new Insets(10, 5, 0, 0);
		gbc_drillDownPanel.gridx = 0;
		gbc_drillDownPanel.gridy = 7;
		variableSelectorPanel.add(drillDownPanel, gbc_drillDownPanel);
		drillDownPanel.setVisible(false);

		fillDrillDownPanel(drillDownPanel);
		
		RunAlgorithmListener runListener = new RunAlgorithmListener();
		runListener.setView(this);
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
			this.setSelected(false);
			this.setSelected(true);
		} catch (PropertyVetoException e) {
			LOGGER.error("Exception creating view");
		}
		
	}
	
	private void fillIndependentVariablePanel(JPanel indVariablesPanel) {
		GridBagLayout gbl_indVariablesPanel = new GridBagLayout();
		gbl_indVariablesPanel.columnWidths = new int[]{0, 0, 0};
		gbl_indVariablesPanel.rowHeights = new int[]{0, 0, 0};
		gbl_indVariablesPanel.columnWeights = new double[]{1.0, 0.0, Double.MIN_VALUE};
		gbl_indVariablesPanel.rowWeights = new double[]{1.0, 0.0, Double.MIN_VALUE};
		indVariablesPanel.setLayout(gbl_indVariablesPanel);
		
		JLabel includeLabel = new JLabel();
		includeLabel.setText("Independent Variable");
		includeLabel.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_includeLabel = new GridBagConstraints();
		gbc_includeLabel.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_includeLabel.fill = GridBagConstraints.NONE;
		gbc_includeLabel.gridwidth = 2;
		gbc_includeLabel.insets = new Insets(5, 20, 0, 0);
		gbc_includeLabel.gridx = 0;
		gbc_includeLabel.gridy = 0;
		indVariablesPanel.add(includeLabel, gbc_includeLabel);
		
		JLabel entropyLabel = new JLabel();
		entropyLabel.setText("Entropy");
		entropyLabel.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_entropyLabel = new GridBagConstraints();
		gbc_entropyLabel.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_entropyLabel.fill = GridBagConstraints.NONE;
		gbc_entropyLabel.insets = new Insets(5, 20, 0, 0);
		gbc_entropyLabel.gridx = 2;
		gbc_entropyLabel.gridy = 0;
		indVariablesPanel.add(entropyLabel, gbc_entropyLabel);
		
		JLabel accuracyLabel = new JLabel();
		accuracyLabel.setText("Accuracy");
		accuracyLabel.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_accuracyLabel = new GridBagConstraints();
		gbc_accuracyLabel.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_accuracyLabel.fill = GridBagConstraints.NONE;
		gbc_accuracyLabel.insets = new Insets(5, 20, 0, 0);
		gbc_accuracyLabel.gridx = 3;
		gbc_accuracyLabel.gridy = 0;
		indVariablesPanel.add(accuracyLabel, gbc_accuracyLabel);
		
		JLabel precisionLabel = new JLabel();
		precisionLabel.setText("Precision");
		precisionLabel.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_precisionLabel = new GridBagConstraints();
		gbc_precisionLabel.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_precisionLabel.fill = GridBagConstraints.NONE;
		gbc_precisionLabel.insets = new Insets(5, 20, 0, 0);
		gbc_precisionLabel.gridx = 4;
		gbc_precisionLabel.gridy = 0;
		indVariablesPanel.add(precisionLabel, gbc_precisionLabel);
		
		columnCheckboxes = new ArrayList<JCheckBox>();
		entropyLabels = new ArrayList<JLabel>();
		accuracyLabels = new ArrayList<JLabel>();
		precisionLabels = new ArrayList<JLabel>();

		DecimalFormat entropyFormatter = new DecimalFormat("0.000E0");
		DecimalFormat accuracyFormatter = new DecimalFormat("##.##");
		DecimalFormat precisionFormatter = new DecimalFormat("0.000E0");
		
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
			gbc_checkbox.gridy = i;
			indVariablesPanel.add(checkbox, gbc_checkbox);
			columnCheckboxes.add(checkbox);
				
			JLabel entropyVal = new JLabel();
			entropyVal.setText(entropyFormatter.format(entropyArr[i-1]));
			GridBagConstraints gbc_entropyVal = new GridBagConstraints();
			gbc_entropyVal.anchor = GridBagConstraints.FIRST_LINE_START;
			gbc_entropyVal.fill = GridBagConstraints.NONE;
			gbc_entropyVal.insets = new Insets(5, 20, 0, 0);
			gbc_entropyVal.gridx = 2;
			gbc_entropyVal.gridy = i;
			indVariablesPanel.add(entropyVal, gbc_entropyVal);
			entropyLabels.add(entropyVal);
			
			JLabel accuracyVal = new JLabel();
			accuracyVal.setText(accuracyFormatter.format(accuracyArr[i-1])+"%");
			GridBagConstraints gbc_accuracyVal = new GridBagConstraints();
			gbc_accuracyVal.anchor = GridBagConstraints.FIRST_LINE_START;
			gbc_accuracyVal.fill = GridBagConstraints.NONE;
			gbc_accuracyVal.insets = new Insets(5, 20, 0, 0);
			gbc_accuracyVal.gridx = 3;
			gbc_accuracyVal.gridy = i;
			indVariablesPanel.add(accuracyVal, gbc_accuracyVal);
			accuracyLabels.add(accuracyVal);
			
			JLabel precisionVal = new JLabel();
			precisionVal.setText(precisionFormatter.format(precisionArr[i-1]));
			GridBagConstraints gbc_precisionVal = new GridBagConstraints();
			gbc_precisionVal.anchor = GridBagConstraints.FIRST_LINE_START;
			gbc_precisionVal.fill = GridBagConstraints.NONE;
			gbc_precisionVal.insets = new Insets(5, 20, 0, 0);
			gbc_precisionVal.gridx = 4;
			gbc_precisionVal.gridy = i;
			indVariablesPanel.add(precisionVal, gbc_precisionVal);
			precisionLabels.add(precisionVal);
		}
	}
	
	private void fillClusterPanel(JPanel clusterPanel) {
	
	}
	
	private void fillClassifyPanel(JPanel classifyPanel) {
		
		GridBagLayout gbl_classifyPanel = new GridBagLayout();
		gbl_classifyPanel.columnWidths = new int[]{0, 0, 0};
		gbl_classifyPanel.rowHeights = new int[]{0, 0, 0};
		gbl_classifyPanel.columnWeights = new double[]{1.0, 0.0, Double.MIN_VALUE};
		gbl_classifyPanel.rowWeights = new double[]{1.0, 0.0, Double.MIN_VALUE};
		classifyPanel.setLayout(gbl_classifyPanel);
		
		lblSelectClass = new JLabel("Select class:");
		lblSelectClass.setFont(new Font("Tahoma", Font.PLAIN, 11));
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
		classComboBox.setPreferredSize(new Dimension(100, 25));
		String[] cols = new String[names.length-1];
		for(int i=1;i<names.length;i++)
			cols[i-1] = names[i];
		classComboBox.setModel(new DefaultComboBoxModel<String>(cols));
		GridBagConstraints gbc_classComboBox = new GridBagConstraints();
		gbc_classComboBox.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_classComboBox.fill = GridBagConstraints.NONE;
		gbc_classComboBox.insets = new Insets(10, 5, 0, 0);
		gbc_classComboBox.gridx = 0;
		gbc_classComboBox.gridy = 1;
		classifyPanel.add(classComboBox, gbc_classComboBox);
		ClassificationSelectionListener classSelectList = new ClassificationSelectionListener();
		classSelectList.setView(this);
		classComboBox.addActionListener(classSelectList);
		
		lblSelectClassMethod = new JLabel("Select classification Method:");
		lblSelectClassMethod.setFont(new Font("Tahoma", Font.PLAIN, 11));
		GridBagConstraints gbc_lblSelectClassMethod = new GridBagConstraints();
		gbc_lblSelectClassMethod.anchor = GridBagConstraints.FIRST_LINE_END;
		gbc_lblSelectClassMethod.fill = GridBagConstraints.NONE;
		gbc_lblSelectClassMethod.insets = new Insets(10, 5, 0, 0);
		gbc_lblSelectClassMethod.gridx = 1;
		gbc_lblSelectClassMethod.gridy = 0;
		classifyPanel.add(lblSelectClassMethod, gbc_lblSelectClassMethod);

		classificationMethodComboBox = new JComboBox<String>();
		classificationMethodComboBox.setFont(new Font("Tahoma", Font.PLAIN, 11));
		classificationMethodComboBox.setBackground(Color.GRAY);
		classificationMethodComboBox.setPreferredSize(new Dimension(100, 25));
		String[] classTypes = new String[] {"J48","J48GRAFT","SIMPLECART","REPTREE","BFTREE","ADTREE","LADTREE","PART","DECISIONTABLE","DECISIONSTUMP","LMT","SIMPLELOGISTIC"};
		classificationMethodComboBox.setModel(new DefaultComboBoxModel<String>(classTypes));
		GridBagConstraints gbc_classificationMethodComboBox = new GridBagConstraints();
		gbc_classificationMethodComboBox.anchor = GridBagConstraints.FIRST_LINE_END;
		gbc_classificationMethodComboBox.fill = GridBagConstraints.NONE;
		gbc_classificationMethodComboBox.insets = new Insets(10, 5, 0, 0);
		gbc_classificationMethodComboBox.gridx = 1;
		gbc_classificationMethodComboBox.gridy = 1;
		classifyPanel.add(classificationMethodComboBox, gbc_classificationMethodComboBox);

	}

	public void fillDrillDownPanel(JPanel drillDownPanel) {
		GridBagLayout gbl_drillDownPanel = new GridBagLayout();
		gbl_drillDownPanel.columnWidths = new int[]{0, 0, 0};
		gbl_drillDownPanel.rowHeights = new int[]{0, 0, 0};
		gbl_drillDownPanel.columnWeights = new double[]{0.0, 0.0, 1.0};
		gbl_drillDownPanel.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 1.0};
		drillDownPanel.setLayout(gbl_drillDownPanel);
		
		lblDrillDownSelectTab = new JLabel("Select clustering run to drill down on:");
		lblDrillDownSelectTab.setFont(new Font("Tahoma", Font.BOLD, 16));
		GridBagConstraints gbc_lblDrillDownSelectTab = new GridBagConstraints();
		gbc_lblDrillDownSelectTab.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_lblDrillDownSelectTab.fill = GridBagConstraints.NONE;
		gbc_lblDrillDownSelectTab.insets = new Insets(30, 5, 0, 0);
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
		gbc_drillDownTabSelectorComboBox.insets = new Insets(10, 5, 0, 0);
		gbc_drillDownTabSelectorComboBox.gridx = 0;
		gbc_drillDownTabSelectorComboBox.gridy = 1;
		drillDownPanel.add(drillDownTabSelectorComboBox, gbc_drillDownTabSelectorComboBox);
		drillDownTabSelectorComboBox.setVisible(false);
		
		clusterCheckBoxPanel = new JPanel();
		GridBagConstraints gbc_clusterCheckBoxPanel = new GridBagConstraints();
		gbc_clusterCheckBoxPanel.anchor = GridBagConstraints.FIRST_LINE_START;
		gbc_clusterCheckBoxPanel.fill = GridBagConstraints.NONE;
		gbc_clusterCheckBoxPanel.insets = new Insets(10, 5, 0, 0);
		gbc_clusterCheckBoxPanel.gridx = 0;
		gbc_clusterCheckBoxPanel.gridy = 2;
		drillDownPanel.add(clusterCheckBoxPanel, gbc_clusterCheckBoxPanel);
		
		GridBagLayout gbl_clusterCheckBoxPanel = new GridBagLayout();
		gbl_clusterCheckBoxPanel.columnWidths = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
		gbl_clusterCheckBoxPanel.rowHeights = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
		gbl_clusterCheckBoxPanel.columnWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
		gbl_clusterCheckBoxPanel.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
		clusterCheckBoxPanel.setLayout(gbl_clusterCheckBoxPanel);
		
		JLabel lblClusterSelect = new JLabel("Select Clusters to drill down:");
		lblClusterSelect.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_lblClusterSelect = new GridBagConstraints();
		gbc_lblClusterSelect.anchor = GridBagConstraints.NORTHWEST;
		gbc_lblClusterSelect.fill = GridBagConstraints.NONE;
		gbc_lblClusterSelect.insets = new Insets(10, 5, 0, 0);
		gbc_lblClusterSelect.gridx = 0;
		gbc_lblClusterSelect.gridy = 0;
		clusterCheckBoxPanel.add(lblClusterSelect, gbc_lblClusterSelect);
		
		runDrillDown = new CustomButton("Drill Down");
		runDrillDown.setFont(new Font("Tahoma", Font.BOLD, 11));
		runDrillDown.setPreferredSize(new Dimension(150, 25));
		GridBagConstraints gbc_runDrillDown = new GridBagConstraints();
		gbc_runDrillDown.insets = new Insets(10, 5, 0, 40);
		gbc_runDrillDown.anchor = GridBagConstraints.FIRST_LINE_END;
		gbc_runDrillDown.fill = GridBagConstraints.NONE;
		gbc_runDrillDown.gridx = 0;
		gbc_runDrillDown.gridy = 3;
		drillDownPanel.add(runDrillDown, gbc_runDrillDown);
		Style.registerTargetClassName(runDrillDown,  ".createBtn");
		
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
	public void showCluster(Boolean show) {
		clusterPanel.setVisible(show);
	}
	public void enableDrillDown() {
		showDrillDownBtn.setEnabled(true);
	}
	public void showDrillDownPanel(Boolean show) {
		drillDownPanel.setVisible(show);
		lblDrillDownSelectTab.setVisible(show);
		drillDownTabSelectorComboBox.setVisible(show);
		//button to run drill down
		if(show){
			drillDownTabSelectorComboBox.setSelectedIndex(0);
			String tabName = (String) drillDownTabSelectorComboBox.getSelectedItem();
			ClusteringVizPlaySheet playSheet = (ClusteringVizPlaySheet) playSheetHash.get(tabName);
			int clusters = playSheet.getNumClusters();
			updateClusterCheckboxes(clusters);
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
			clusterCheckBoxPanel.add(checkbox, gbc_checkbox);
			clusterCheckboxes.add(checkbox);
		}
	}
	public void setSelectedColumns(String[] selectedCols) {
		for(JCheckBox checkbox : columnCheckboxes) {
			checkbox.setSelected(false);
		}
		for(String checkboxToSelect : selectedCols) {
			for(JCheckBox checkbox : columnCheckboxes) {
				String name = checkbox.getName();
				if(name.equals(checkboxToSelect + checkboxName))
					checkbox.setSelected(true);
			}
		}
	}
	public void setDisabledCheckBox(String checkboxToDisable){
		for(JCheckBox checkbox : columnCheckboxes) {
			String name = checkbox.getName();
			if(name.equals(checkboxToDisable + checkboxName)) {
				checkbox.setSelected(true);
				checkbox.setEnabled(false);
			} else {
				checkbox.setEnabled(true);
			}
		}
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
		return columnCheckboxes;
	}
	public ArrayList<JCheckBox> getClusterCheckboxes() {
		return clusterCheckboxes;
	}	
	public JComboBox<String> getDrillDownTabSelectorComboBox() {
		return drillDownTabSelectorComboBox;
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