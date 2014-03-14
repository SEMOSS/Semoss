/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.ui.components;

import java.awt.CardLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.ComponentOrientation;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.Frame;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Image;
import java.awt.Insets;
import java.awt.LayoutManager;
import java.awt.Panel;
import java.awt.SystemColor;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusListener;
import java.awt.event.InputEvent;
import java.awt.event.KeyEvent;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Vector;

import javax.imageio.ImageIO;
import javax.swing.Box;
import javax.swing.DefaultComboBoxModel;
import javax.swing.DefaultListModel;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JDesktopPane;
import javax.swing.JFrame;
import javax.swing.JInternalFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JProgressBar;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.JSeparator;
import javax.swing.JSlider;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.JTextPane;
import javax.swing.JToggleButton;
import javax.swing.KeyStroke;
import javax.swing.ScrollPaneConstants;
import javax.swing.SwingConstants;
import javax.swing.UIDefaults;
import javax.swing.UIManager;
import javax.swing.border.LineBorder;
import javax.swing.event.ChangeListener;
import javax.swing.event.InternalFrameListener;

import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.specific.tap.FactSheetReportComboBox;
import prerna.ui.components.specific.tap.ServiceSelectPanel;
import prerna.ui.components.specific.tap.SourceSelectPanel;
import prerna.ui.components.specific.tap.TransitionReportComboBox;
import prerna.ui.main.listener.impl.ProcessQueryListener;
import prerna.ui.main.listener.impl.RepoSelectionListener;
import prerna.ui.main.listener.impl.ShowPlaySheetsButtonListener;
import prerna.ui.main.listener.impl.TextUndoListener;
import prerna.ui.swing.custom.ButtonMenuDropDown;
import prerna.ui.swing.custom.CustomAruiStyle;
import prerna.ui.swing.custom.CustomButton;
import prerna.ui.swing.custom.CustomDesktopPane;
import prerna.ui.swing.custom.ToggleButton;
import prerna.util.CSSApplication;
import prerna.util.Constants;
import prerna.util.DIHelper;
import aurelienribon.ui.components.Button;
import aurelienribon.ui.css.Style;
import aurelienribon.ui.css.swing.SwingStyle;

import com.ibm.icu.util.StringTokenizer;

/**
 * The playpane houses all of the components that create the user interface in SEMOSS.
 */
public class PlayPane extends JFrame {

	Logger logger = Logger.getLogger(getClass());

	// Left Control Panel Components
	public JComboBox perspectiveSelector, questionSelector, playSheetComboBox;
	public JPanel paramPanel;
	public JButton submitButton, btnGetQuestionSparql, btnShowHint;
	public JToggleButton btnCustomSparql, appendButton;
	public JList repoList;
	public SparqlArea sparqlArea;
	private JSeparator separator_1, separator_3;

	// Right graphPanel desktopPane
	public JDesktopPane desktopPane;
	public JButton refreshButton;
	public JTable filterTable, edgeTable, propertyTable;
	private JScrollPane filterSliders;
	public ButtonMenuDropDown btnShowPlaySheets;
	public	ShowPlaySheetsButtonListener showPlaySheetsListener;

	// left cosmetic panel components
	public JTable colorShapeTable, sizeTable;
	public JButton btnColorShape, btnResetDefaults;

	// Left label panel
	public JTable labelTable, tooltipTable;

	// SUDOWL Panel Components
	private JPanel owlPanel;
	public JTable objectPropertiesTable, dataPropertiesTable;
	public JTextField dataPropertiesString, objectPropertiesString;
	public JButton btnRepaintGraph;

	// Help Tab Components
	private JTextArea aboutArea;
	private JTextPane releaseNoteArea;
	public JButton htmlTrainingBtn, pptTrainingBtn;

	// Custom Update Components
	public JButton btnCustomUpdate;
	private JScrollPane customUpdateScrollPane;
	public JTextPane customUpdateTextPane;

	// Financial Transition Report Components
	public JScrollPane serviceSelectScrollPane;
	public JPanel transitionServicePanel, transReportSysDropDownPanel,
			transReportTypeDropDownPanel, transReportFormDropDownPanel,
			transReportCheckBoxPanel;
	public TransitionReportComboBox transCostReportSystemcomboBox;
	public JComboBox TransReportFormatcomboBox, TransReportTypecomboBox;
	public JRadioButton rdbtnApplyTapOverhead, rdbtnDoNotApplyOverhead;
	public JCheckBox chckbxDataEsbImplementation, chckbxBluEsbImplementation,
			chckbxDataFederationTransReport, chckbxBLUprovider,
			chckbxDataConsumer;
	public JButton transitionReportGenButton = new JButton();

	// Tap Generate Report Components
	public JScrollPane sourceSelectScrollPane;
	public JPanel sourceSelectPanel;
	public JButton sourceReportGenButton = new JButton();

	// Financial DB Mod Components
	public JToggleButton serviceSelectionBtn, btnAdvancedFinancialFunctions;
	public JPanel advancedFunctionsPanel;
	public JButton btnInsertBudgetProperty, btnInsertServiceProperties,
			calculateTransitionCostsButton, calcTransAdditionalButton;
	public JCheckBox tierCheck1, tierCheck2, tierCheck3;
	public JProgressBar calcTCprogressBar;

	// BV TM Components
	public JButton btnRunBvAlone, btnRunTmAlone, btnInsertDownstream, btnRunCapabilityBV;
	public JTextField soaAlphaValueTextField, depreciationValueTextField,
			appreciationValueTextField;

	// Import Components
	public JComboBox dbImportTypeComboBox, loadingFormatComboBox, dbImportRDBMSDriverComboBox;
	public JPanel advancedImportOptionsPanel, dbImportPanel;
	public JTextField importFileNameField, customBaseURItextField, 
			importMapFileNameField, dbPropFileNameField, questionFileNameField,
			dbSelectorField, dbImportURLField, dbImportUsernameField;
	public JPasswordField dbImportPWField;
	public JButton mapBrowseBtn, dbPropBrowseButton, questionBrowseButton,
			btnShowAdvancedImportFeatures, importButton, fileBrowseBtn, btnTestRDBMSConnection;
	public JLabel selectionFileLbl, dbNameLbl, lblDataInputFormat, lblDBImportURL, 
			lblDBImportUsername, lblDBImportPW, lblDBImportDriverType;

	// Export Components
	public JLabel lblMaxExportLimit;
	public JButton btnExportNodeLoadSheets, btnExportRelationshipsLoadSheets;
	public ParamComboBox exportDataSourceComboBox, subjectNodeTypeComboBox1,
			subjectNodeTypeComboBox2, subjectNodeTypeComboBox3,
			subjectNodeTypeComboBox4, subjectNodeTypeComboBox5,
			subjectNodeTypeComboBox6, subjectNodeTypeComboBox7,
			subjectNodeTypeComboBox8, subjectNodeTypeComboBox9;
	public ParamComboBox objectNodeTypeComboBox1, objectNodeTypeComboBox2,
			objectNodeTypeComboBox3, objectNodeTypeComboBox4,
			objectNodeTypeComboBox5, objectNodeTypeComboBox6,
			objectNodeTypeComboBox7, objectNodeTypeComboBox8,
			objectNodeTypeComboBox9;
	public ParamComboBox nodeRelationshipComboBox1, nodeRelationshipComboBox2,
			nodeRelationshipComboBox3, nodeRelationshipComboBox4,
			nodeRelationshipComboBox5, nodeRelationshipComboBox6,
			nodeRelationshipComboBox7, nodeRelationshipComboBox8,
			nodeRelationshipComboBox9;
	public JButton btnAddExport;
	private Component rigidArea;
	public JButton btnClearAll;
	private JLabel lblChangedDB;
	public CustomButton btnUpdateCostDB;
	private JLabel lblCostDBBaseURI;
	public JTextField costDBBaseURIField;
	private JLabel lblCostDB;
	public JComboBox changedDBComboBox, costDBComboBox;
	public JButton saveSudowl;
	private ButtonMenuDropDown comboBox;

	// Active Systems
	public CustomButton btnUpdateActiveSystems;
	
	// Aggregate TAP Services into TAP Core
	public CustomButton btnAggregateTapServicesIntoTapCore;
	public JComboBox<String> selectTapServicesComboBox, selectTapCoreComboBox;
	
	// Components on settings panel
	public JCheckBox propertyCheck, sudowlCheck, searchCheck,
			highQualityExportCheck;
	private JSeparator separator_4;
	private JLabel distBVtechMatlabel;

	// RFP, vendor, and deconflicting panel
	private JPanel tapReportTopPanel;
	public JTextField RFPNameField;
	public JButton btnUpdateVendorDB;
	private JSeparator separator_6;
	private Panel functionalAreaPanel;
	public JCheckBox HSDCheckBox, HSSCheckBox, FHPCheckBox, DHMSMCheckBox;
	public JButton btnCalculateVendorTMAlone;
	public JButton btnDeconflictingReport;

	// Fact Sheet Report Generator Panel
	private JSeparator separator_7;
	public JButton btnFactSheetImageExport, btnFactSheetReport;
	public JPanel factSheetReportSysDropDownPanel,
			factSheetReportTypeDropDownPanel;
	public FactSheetReportComboBox factSheetReportSyscomboBox;
	public JComboBox FactSheetReportTypecomboBox;

	//Tasker Generation and System Info Panel
	private JSeparator separator_8;
	public JButton btnTaskerGeneration;
	public JButton btnSystemInfoGenButton;
	public FactSheetReportComboBox TaskerGenerationSyscomboBox;
	
//	//Capability Fact Sheet Panel
//	public JButton btnCapabilityFactSheetGeneration;
//	public FactSheetReportComboBox capabilityFactSheetCapComboBox;
	
	private JLabel lblModifyQueryOf;
	private JSeparator separator;
//	public JButton btnCommonGraph;

	/**
	 * Launch the application.
	 */
	public void start() throws Exception {

		// load all the listeners
		// cast it to IChakraListener
		// for each listener specify what is the view field - Listener_VIEW
		// for each listener specify the right panel field -
		// Listener_RIGHT_PANEL
		// utilize reflection to get all the fields
		// for each field go into the properties file and find any of the
		// listeners
		// Drop down scrollbars
		Object popup = questionSelector.getUI().getAccessibleChild(questionSelector, 0);
		Component c = ((Container) popup).getComponent(0);
		if (c instanceof JScrollPane) {
			((JScrollPane) c).getVerticalScrollBar()
					.setUI(new NewScrollBarUI());
		}

		popup = perspectiveSelector.getUI().getAccessibleChild(perspectiveSelector, 0);
		c = ((Container) popup).getComponent(0);
		if (c instanceof JScrollPane) {
			((JScrollPane) c).getVerticalScrollBar()
					.setUI(new NewScrollBarUI());
		}

		popup = TransReportFormatcomboBox.getUI().getAccessibleChild(TransReportFormatcomboBox, 0);
		c = ((Container) popup).getComponent(0);
		if (c instanceof JScrollPane) {
			((JScrollPane) c).getVerticalScrollBar()
					.setUI(new NewScrollBarUI());
		}

		popup = TransReportTypecomboBox.getUI().getAccessibleChild(TransReportTypecomboBox, 0);
		c = ((Container) popup).getComponent(0);
		if (c instanceof JScrollPane) {
			((JScrollPane) c).getVerticalScrollBar()
					.setUI(new NewScrollBarUI());
		}

		popup = factSheetReportSyscomboBox.getUI().getAccessibleChild(factSheetReportSyscomboBox, 0);
		c = ((Container) popup).getComponent(0);
		if (c instanceof JScrollPane) {
			((JScrollPane) c).getVerticalScrollBar()
					.setUI(new NewScrollBarUI());
		}

		popup = dbImportTypeComboBox.getUI().getAccessibleChild(dbImportTypeComboBox, 0);
		c = ((Container) popup).getComponent(0);
		if (c instanceof JScrollPane) {
			((JScrollPane) c).getVerticalScrollBar()
					.setUI(new NewScrollBarUI());
		}

		// start with self reference
		DIHelper.getInstance().setLocalProperty(Constants.MAIN_FRAME, this);

		java.lang.reflect.Field[] fields = PlayPane.class.getFields();

		// run through the view components
		for (int fieldIndex = 0; fieldIndex < fields.length; fieldIndex++) {
			// logger.info(fields[fieldIndex].getName());
			Object obj = fields[fieldIndex].get(this);
			logger.debug("Object set to " + obj);
			String fieldName = fields[fieldIndex].getName();
			if (obj instanceof JComboBox || obj instanceof JButton
					|| obj instanceof JToggleButton || obj instanceof JSlider
					|| obj instanceof JInternalFrame
					|| obj instanceof JRadioButton || obj instanceof JTextArea) {
				// load the controllers
				// find the view
				// right view and listener
				String ctrlNames = DIHelper.getInstance().getProperty(
						fieldName + "_" + Constants.CONTROL);
				if (ctrlNames != null && ctrlNames.length() != 0) {
					logger.debug("Listeners >>>>  " + ctrlNames	+ "   for field " + fieldName);
					StringTokenizer listenerTokens = new StringTokenizer(ctrlNames, ";");
					while (listenerTokens.hasMoreTokens()) {
						String ctrlName = listenerTokens.nextToken();
						logger.debug("Processing widget " + ctrlName);
						String className = DIHelper.getInstance().getProperty(ctrlName);
						IChakraListener listener = (IChakraListener) Class.forName(className).getConstructor(null).newInstance(null);
						// in the future this could be a list
						// add it to this object
						logger.debug("Listener " + ctrlName + "<>" + listener);
						// check to if this is a combobox or button
						if (obj instanceof JComboBox)
							((JComboBox) obj).addActionListener(listener);
						else if (obj instanceof JButton)
							((JButton) obj).addActionListener(listener);
						else if (obj instanceof JRadioButton)
							((JRadioButton) obj).addActionListener(listener);
						else if (obj instanceof JToggleButton)
							((JToggleButton) obj).addActionListener(listener);
						else if (obj instanceof JSlider)
							((JSlider) obj).addChangeListener((ChangeListener) listener);
						else if (obj instanceof JTextArea)
							((JTextArea) obj).addFocusListener((FocusListener) listener);
						else
							((JInternalFrame) obj).addInternalFrameListener((InternalFrameListener) listener);
						System.out.println(ctrlName + ":" + listener);	
						DIHelper.getInstance().setLocalProperty(ctrlName, listener);
					}
				}
			}
			System.out.println(fieldName + ":" + obj);	
			logger.debug("Loading <" + fieldName + "> <> " + obj);
			DIHelper.getInstance().setLocalProperty(fieldName, obj);
		}

		// need to also add the listeners respective views
		// Go through the listeners and add the model
		String listeners = DIHelper.getInstance().getProperty(Constants.LISTENERS);
		StringTokenizer lTokens = new StringTokenizer(listeners, ";");
		while (lTokens.hasMoreElements()) {
			String lToken = lTokens.nextToken();

			// set the views
			String viewName = DIHelper.getInstance().getProperty(lToken + "_" + Constants.VIEW);
			Object listener = DIHelper.getInstance().getLocalProp(lToken);
			if (viewName != null && listener != null) {
				// get the listener object and set it
				Method method = listener.getClass().getMethod("setView", JComponent.class);
				Object param = DIHelper.getInstance().getLocalProp(viewName);
				logger.debug("Param is <" + viewName + "><" + param + ">");
				method.invoke(listener, param);
			}

			// set the parent views
			viewName = DIHelper.getInstance().getProperty(lToken + "_" + Constants.PARENT_VIEW);
			/*
			 * if(viewName != null && listener != null) { // get the listener
			 * object and set it Method method =
			 * listener.getClass().getMethod("setParentView", JComponent.class);
			 * Object param = DIHelper.getInstance().getLocalProp(viewName);
			 * logger.debug("Param is <"+viewName+"><" + param + ">");
			 * method.invoke(listener, param); }
			 * 
			 * 
			 * // set the parent views viewName =
			 * DIHelper.getInstance().getProperty(lToken + "_" +
			 * Constants.RIGHT_VIEW); if(viewName != null && listener != null) {
			 * // get the listener object and set it Method method =
			 * listener.getClass().getMethod("setRightPanel", JComponent.class);
			 * Object param = DIHelper.getInstance().getLocalProp(viewName);
			 * logger.debug("Param is <"+viewName+"><" + param + ">");
			 * method.invoke(listener, param); }
			 */
		}
		// set the repository
		String engines = (String) DIHelper.getInstance().getLocalProp(Constants.ENGINES);

		StringTokenizer tokens = new StringTokenizer(engines, ";");
		DefaultListModel listModel = new DefaultListModel();
		while (tokens.hasMoreTokens()) {
			String engineName = tokens.nextToken();
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
			if (engine.isConnected())
				listModel.addElement(engineName);
		}
		repoList.setModel(listModel);
		repoList.setSelectedIndex(0);
		repoList.setVisibleRowCount(listModel.getSize() / 2);

		// set the models now
		// set the perspectives information
		/*
		 * Hashtable perspectiveHash = (Hashtable)
		 * DIHelper.getInstance().getLocalProp(Constants.PERSPECTIVE);
		 * Vector<String> perspectives =
		 * Utility.convertEnumToArray(perspectiveHash.keys(),
		 * perspectiveHash.size()); Collections.sort(perspectives);
		 * logger.info("Perspectives " + perspectiveHash); for(int
		 * itemIndex = 0;itemIndex <
		 * perspectives.size();this.perspectiveSelector
		 * .addItem(perspectives.get(itemIndex)), itemIndex++);
		 */
	}

	/**
	 * Create the frame.
	 * 
	
	 */
	public PlayPane() throws IOException {
		setExtendedState(Frame.MAXIMIZED_BOTH);
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		// set the icons
		List<Image> images = new Vector();
		String workingDir = System.getProperty("user.dir");
		String imgFile16 = workingDir + "/pictures/finalWhiteLogo16.png";
		ImageIcon img16 = new ImageIcon(imgFile16);
		images.add(img16.getImage());
		String imgFile32 = workingDir + "/pictures/finalWhiteLogo32.png";
		ImageIcon img32 = new ImageIcon(imgFile32);
		images.add(img32.getImage());
		String imgFile64 = workingDir + "/pictures/finalWhiteLogo64.png";
		ImageIcon img64 = new ImageIcon(imgFile64);
		images.add(img64.getImage());
		String imgFile128 = workingDir + "/pictures/finalWhiteLogo128.png";
		ImageIcon img128 = new ImageIcon(imgFile128);
		images.add(img128.getImage());
		setIconImages(images);
		setTitle("SEMOSS - Analytics Environment");

		setSize(new Dimension(1371, 744));

		JScrollPane scrollPane = new JScrollPane();
		GridBagLayout gridBagLayout = new GridBagLayout();
		gridBagLayout.columnWidths = new int[] { 1164, 0 };
		gridBagLayout.rowHeights = new int[] { 540, 0 };
		gridBagLayout.columnWeights = new double[] { 1.0, Double.MIN_VALUE };
		gridBagLayout.rowWeights = new double[] { 1.0, Double.MIN_VALUE };
		getContentPane().setLayout(gridBagLayout);

		JSplitPane splitPane = new JSplitPane();
		splitPane.setContinuousLayout(true);
		splitPane.setOneTouchExpandable(true);

		JPanel rightViewPanel = new JPanel();
		
		LayoutManager rightViewLayout = new RightViewLayoutManager();
		rightViewPanel.setLayout ( rightViewLayout);

		RightView rightView = new RightView(JTabbedPane.TOP);
		
		showPlaySheetsListener = new ShowPlaySheetsButtonListener();

		try {
		    Image img = ImageIO.read(new File(workingDir+"/pictures/showPlaySheets.png"));
		    Image newimg = img.getScaledInstance( 15, 15,  java.awt.Image.SCALE_SMOOTH );
		    btnShowPlaySheets = new ButtonMenuDropDown(new ImageIcon(newimg));
		  } catch (IOException ex) {
			    btnShowPlaySheets = new ButtonMenuDropDown("");
		  }
		GridBagConstraints gbc_btnShowPlaySheets = new GridBagConstraints();
		gbc_btnShowPlaySheets.anchor = GridBagConstraints.WEST;
		btnShowPlaySheets.setFont(new Font("Tahoma", Font.BOLD, 11));
		btnShowPlaySheets.setToolTipText("Show all Play Sheets");
		btnShowPlaySheets.addActionListener(showPlaySheetsListener);
		btnShowPlaySheets.setEnabled(false);

		
		rightViewPanel.add(rightView);
		rightViewPanel.add(btnShowPlaySheets, "special",0);
		splitPane.setRightComponent(rightViewPanel);
		
		JPanel graphPanel = new JPanel();
		rightView.addTab("Display Pane", null, graphPanel, null);
		graphPanel.setLayout(new GridLayout(1, 0, 0, 0));
		desktopPane = new CustomDesktopPane();
		UIDefaults nimbusOverrides = new UIDefaults();
		UIDefaults defaults = UIManager.getLookAndFeelDefaults();
		// DesktopPanePainter painter = new DesktopPanePainter();
		// nimbusOverrides.put("DesktopPane[Enabled].backgroundPainter",
		// painter);
		// desktopPane.putClientProperty("Nimbus.Overrides", nimbusOverrides);
		// desktopPane.putClientProperty("Nimbus.Overrides.InheritDefaults",
		// false);
		graphPanel.add(desktopPane);

		String[] fetching = { "Fetching" };

		// String workingDir = System.getProperty("user.dir");
		FileReader fr = null;

		// Here we read the release notes text file
		String releaseNotesTextFile = workingDir + "/help/info.txt";
		fr = new FileReader(releaseNotesTextFile);
		BufferedReader releaseNotesTextReader = new BufferedReader(fr);

		String releaseNotesData = "<html><body bgcolor=\"#f0f0f0\"> ";
		String line = null;
		while ((line = releaseNotesTextReader.readLine()) != null) {
			releaseNotesData = releaseNotesData + line + "<br>";
		}
		releaseNotesData = releaseNotesData + "";

		JPanel imExPanel = new JPanel();
		imExPanel.setBackground(SystemColor.control);
		JScrollPane imExPanelScroll = new JScrollPane(imExPanel);
		rightView.addTab("DB Modification", null, imExPanelScroll, null);
		GridBagLayout gbl_imExPanel = new GridBagLayout();
		gbl_imExPanel.columnWidths = new int[] { 1026, 0 };
		gbl_imExPanel.rowHeights = new int[] { 250, 0, 0, 0, 0, 0 };
		gbl_imExPanel.columnWeights = new double[] { 1.0, Double.MIN_VALUE };
		gbl_imExPanel.rowWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 1.0, 0.0 };
		imExPanel.setLayout(gbl_imExPanel);

		JPanel importPanel = new JPanel();
		importPanel.setBackground(SystemColor.control);
		GridBagLayout gbl_importPanel = new GridBagLayout();
		gbl_importPanel.columnWidths = new int[] { 160, 117, 300, 0 };
		gbl_importPanel.rowHeights = new int[] { 0, 0, 0, 28, 0, 0 };
		gbl_importPanel.columnWeights = new double[] { 0.0, 1.0, 0.0, Double.MIN_VALUE };
		gbl_importPanel.rowWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE };
		importPanel.setLayout(gbl_importPanel);

		JLabel lblInsertData = new JLabel("Import Data");
		lblInsertData.setHorizontalAlignment(SwingConstants.LEFT);
		lblInsertData.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_lblInsertData = new GridBagConstraints();
		gbc_lblInsertData.anchor = GridBagConstraints.WEST;
		gbc_lblInsertData.gridwidth = 3;
		gbc_lblInsertData.insets = new Insets(10, 0, 10, 0);
		gbc_lblInsertData.gridx = 0;
		gbc_lblInsertData.gridy = 0;
		importPanel.add(lblInsertData, gbc_lblInsertData);

		JLabel lblDatabaseImportOptions = new JLabel("Database Import Options");
		lblDatabaseImportOptions.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblDatabaseImportOptions = new GridBagConstraints();
		gbc_lblDatabaseImportOptions.insets = new Insets(0, 0, 5, 5);
		gbc_lblDatabaseImportOptions.anchor = GridBagConstraints.WEST;
		gbc_lblDatabaseImportOptions.gridx = 0;
		gbc_lblDatabaseImportOptions.gridy = 1;
		importPanel.add(lblDatabaseImportOptions, gbc_lblDatabaseImportOptions);

		dbImportTypeComboBox = new JComboBox();
		dbImportTypeComboBox.setFont(new Font("Tahoma", Font.PLAIN, 11));
		dbImportTypeComboBox.setBackground(Color.GRAY);
		dbImportTypeComboBox.setPreferredSize(new Dimension(400, 25));
		dbImportTypeComboBox.setModel(new DefaultComboBoxModel(new String[] {"Select a database import method",	"Add to existing database engine","Modify/Replace data in existing engine","Create new database engine","Create new RDBMS connection" }));
		GridBagConstraints gbc_dbImportTypeComboBox = new GridBagConstraints();
		gbc_dbImportTypeComboBox.anchor = GridBagConstraints.NORTHWEST;
		gbc_dbImportTypeComboBox.insets = new Insets(0, 0, 5, 5);
		gbc_dbImportTypeComboBox.gridx = 1;
		gbc_dbImportTypeComboBox.gridy = 1;
		importPanel.add(dbImportTypeComboBox, gbc_dbImportTypeComboBox);

		dbImportPanel = new JPanel();
		dbImportPanel.setBackground(SystemColor.control);
		dbImportPanel.setVisible(false);

		lblDataInputFormat = new JLabel("Data Input Format");
		lblDataInputFormat.setVisible(false);
		lblDataInputFormat.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblDataInputFormat = new GridBagConstraints();
		gbc_lblDataInputFormat.anchor = GridBagConstraints.WEST;
		gbc_lblDataInputFormat.insets = new Insets(0, 0, 5, 5);
		gbc_lblDataInputFormat.gridx = 0;
		gbc_lblDataInputFormat.gridy = 2;
		importPanel.add(lblDataInputFormat, gbc_lblDataInputFormat);

		loadingFormatComboBox = new JComboBox();
		loadingFormatComboBox.setPreferredSize(new Dimension(250, 25));
		loadingFormatComboBox.setBackground(Color.GRAY);
		loadingFormatComboBox.setVisible(false);
		GridBagConstraints gbc_loadingFormatComboBox = new GridBagConstraints();
		gbc_loadingFormatComboBox.anchor = GridBagConstraints.WEST;
		gbc_loadingFormatComboBox.insets = new Insets(0, 0, 5, 5);
		gbc_loadingFormatComboBox.gridx = 1;
		gbc_loadingFormatComboBox.gridy = 2;
		importPanel.add(loadingFormatComboBox, gbc_loadingFormatComboBox);
		
		GridBagConstraints gbc_dbImportPanel = new GridBagConstraints();
		gbc_dbImportPanel.gridwidth = 2;
		gbc_dbImportPanel.insets = new Insets(0, 0, 5, 5);
		gbc_dbImportPanel.fill = GridBagConstraints.BOTH;
		gbc_dbImportPanel.gridx = 0;
		gbc_dbImportPanel.gridy = 3;
		
		importPanel.add(dbImportPanel, gbc_dbImportPanel);
		GridBagLayout gbl_dbImportPanel = new GridBagLayout();
		gbl_dbImportPanel.columnWidths = new int[] { 160, 0, 0, 0 };
		gbl_dbImportPanel.rowHeights = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
		gbl_dbImportPanel.columnWeights = new double[] { 0.0, 0.0, 0.0, 1.0 };
		gbl_dbImportPanel.rowWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE };
		dbImportPanel.setLayout(gbl_dbImportPanel);

		dbNameLbl = new JLabel("Create Database Name");
		GridBagConstraints gbc_dbNameLbl = new GridBagConstraints();
		gbc_dbNameLbl.anchor = GridBagConstraints.WEST;
		gbc_dbNameLbl.insets = new Insets(0, 0, 5, 5);
		gbc_dbNameLbl.gridx = 0;
		gbc_dbNameLbl.gridy = 0;
		dbImportPanel.add(dbNameLbl, gbc_dbNameLbl);
		dbNameLbl.setFont(new Font("Tahoma", Font.PLAIN, 12));

		dbSelectorField = new JTextField();
		dbSelectorField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		GridBagConstraints gbc_dbSelectorField = new GridBagConstraints();
		gbc_dbSelectorField.fill = GridBagConstraints.HORIZONTAL;
		gbc_dbSelectorField.gridwidth = 3;
		gbc_dbSelectorField.insets = new Insets(0, 0, 5, 0);
		gbc_dbSelectorField.gridx = 1;
		gbc_dbSelectorField.gridy = 0;
		dbImportPanel.add(dbSelectorField, gbc_dbSelectorField);
		dbSelectorField.setColumns(10);
		
		lblDBImportURL = new JLabel("Enter Database URL");
		lblDBImportURL.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblDBImportURL = new GridBagConstraints();
		gbc_lblDBImportURL.anchor = GridBagConstraints.WEST;
		gbc_lblDBImportURL.insets = new Insets(0, 0, 5, 5);
		gbc_lblDBImportURL.gridx = 0;
		gbc_lblDBImportURL.gridy = 1;
		dbImportPanel.add(lblDBImportURL, gbc_lblDBImportURL);
		
		dbImportURLField = new JTextField();
		dbImportURLField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		dbImportURLField.setColumns(10);
		GridBagConstraints gbc_dbImportURLField = new GridBagConstraints();
		gbc_dbImportURLField.insets = new Insets(0, 0, 5, 0);
		gbc_dbImportURLField.gridwidth = 3;
		gbc_dbImportURLField.fill = GridBagConstraints.HORIZONTAL;
		gbc_dbImportURLField.gridx = 1;
		gbc_dbImportURLField.gridy = 1;
		dbImportPanel.add(dbImportURLField, gbc_dbImportURLField);
		
		lblDBImportDriverType = new JLabel("DB Import Type");
		lblDBImportDriverType.setVisible(false);
		lblDBImportDriverType.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblDBImportDriverType = new GridBagConstraints();
		gbc_lblDBImportDriverType.anchor = GridBagConstraints.WEST;
		gbc_lblDBImportDriverType.insets = new Insets(0, 0, 5, 5);
		gbc_lblDBImportDriverType.gridx = 0;
		gbc_lblDBImportDriverType.gridy = 2;
		dbImportPanel.add(lblDBImportDriverType, gbc_lblDBImportDriverType);

		dbImportRDBMSDriverComboBox = new JComboBox();
		dbImportRDBMSDriverComboBox.setBackground(Color.GRAY);
		dbImportRDBMSDriverComboBox.setVisible(false);
		dbImportRDBMSDriverComboBox.setModel(new DefaultComboBoxModel(new String[] {"Select Relational Database Type", "MySQL","Oracle","MS SQL Server"}));
		dbImportRDBMSDriverComboBox.setPreferredSize(new Dimension(225, 25));
		GridBagConstraints gbc_dbImportRDBMSDriverComboBox = new GridBagConstraints();
		gbc_dbImportRDBMSDriverComboBox.gridwidth = 2;
		gbc_dbImportRDBMSDriverComboBox.anchor = GridBagConstraints.WEST;
		gbc_dbImportRDBMSDriverComboBox.insets = new Insets(0, 0, 5, 5);
		gbc_dbImportRDBMSDriverComboBox.gridx = 1;
		gbc_dbImportRDBMSDriverComboBox.gridy = 2;
		dbImportPanel.add(dbImportRDBMSDriverComboBox, gbc_dbImportRDBMSDriverComboBox);
		
		lblDBImportUsername = new JLabel("Enter Database Username");
		lblDBImportUsername.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblDBImportUsername = new GridBagConstraints();
		gbc_lblDBImportUsername.anchor = GridBagConstraints.WEST;
		gbc_lblDBImportUsername.insets = new Insets(0, 0, 5, 5);
		gbc_lblDBImportUsername.gridx = 0;
		gbc_lblDBImportUsername.gridy = 3;
		dbImportPanel.add(lblDBImportUsername, gbc_lblDBImportUsername);
		
		dbImportUsernameField = new JTextField();
		dbImportUsernameField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		dbImportUsernameField.setColumns(10);
		GridBagConstraints gbc_dbImportUsernameField = new GridBagConstraints();
		gbc_dbImportUsernameField.insets = new Insets(0, 0, 5, 0);
		gbc_dbImportUsernameField.gridwidth = 2;
		gbc_dbImportUsernameField.fill = GridBagConstraints.HORIZONTAL;
		gbc_dbImportUsernameField.gridx = 1;
		gbc_dbImportUsernameField.gridy = 3;
		dbImportPanel.add(dbImportUsernameField, gbc_dbImportUsernameField);
		
		lblDBImportPW = new JLabel("Enter Database Password");
		lblDBImportPW.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblDBImportPW = new GridBagConstraints();
		gbc_lblDBImportPW.anchor = GridBagConstraints.WEST;
		gbc_lblDBImportPW.insets = new Insets(0, 0, 5, 5);
		gbc_lblDBImportPW.gridx = 0;
		gbc_lblDBImportPW.gridy = 4;
		dbImportPanel.add(lblDBImportPW, gbc_lblDBImportPW);
		
		dbImportPWField = new JPasswordField();
		dbImportPWField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		dbImportPWField.setColumns(10);
		GridBagConstraints gbc_dbImportPWField = new GridBagConstraints();
		gbc_dbImportPWField.insets = new Insets(0, 0, 5, 5);
		gbc_dbImportPWField.gridwidth = 2;
		gbc_dbImportPWField.fill = GridBagConstraints.HORIZONTAL;
		gbc_dbImportPWField.gridx = 1;
		gbc_dbImportPWField.gridy = 4;
		dbImportPanel.add(dbImportPWField, gbc_dbImportPWField);

		btnTestRDBMSConnection = new CustomButton("Test Connection");
		btnTestRDBMSConnection.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_btnTestRDBMSConnection = new GridBagConstraints();
		gbc_btnTestRDBMSConnection.anchor = GridBagConstraints.WEST;
		gbc_btnTestRDBMSConnection.insets = new Insets(0, 0, 5, 0);
		gbc_btnTestRDBMSConnection.gridx = 3;
		gbc_btnTestRDBMSConnection.gridy = 4;
		dbImportPanel.add(btnTestRDBMSConnection, gbc_btnTestRDBMSConnection);
		
		selectionFileLbl = new JLabel("Select File(s) to Import");
		GridBagConstraints gbc_selectionFileLbl = new GridBagConstraints();
		gbc_selectionFileLbl.anchor = GridBagConstraints.WEST;
		gbc_selectionFileLbl.insets = new Insets(0, 0, 5, 5);
		gbc_selectionFileLbl.gridx = 0;
		gbc_selectionFileLbl.gridy = 5;
		dbImportPanel.add(selectionFileLbl, gbc_selectionFileLbl);
		selectionFileLbl.setFont(new Font("Tahoma", Font.PLAIN, 12));

		fileBrowseBtn = new CustomButton("Browse");
		fileBrowseBtn.setName(Constants.IMPORT_BUTTON_BROWSE);
		fileBrowseBtn.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_fileBrowseBtn = new GridBagConstraints();
		gbc_fileBrowseBtn.anchor = GridBagConstraints.EAST;
		gbc_fileBrowseBtn.insets = new Insets(0, 0, 5, 5);
		gbc_fileBrowseBtn.gridx = 1;
		gbc_fileBrowseBtn.gridy = 5;
		dbImportPanel.add(fileBrowseBtn, gbc_fileBrowseBtn);

		importFileNameField = new JTextField();
		importFileNameField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		GridBagConstraints gbc_importFileNameField = new GridBagConstraints();
		gbc_importFileNameField.fill = GridBagConstraints.HORIZONTAL;
		gbc_importFileNameField.gridwidth = 2;
		gbc_importFileNameField.insets = new Insets(0, 0, 5, 0);
		gbc_importFileNameField.gridx = 2;
		gbc_importFileNameField.gridy = 5;
		dbImportPanel.add(importFileNameField, gbc_importFileNameField);
		importFileNameField.setColumns(10);

		JLabel lblDesignateBaseUri = new JLabel("<HTML>Designate Base URI<br>(Optional)</HTML>");
		lblDesignateBaseUri.setMinimumSize(new Dimension(155, 32));
		lblDesignateBaseUri.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblDesignateBaseUri = new GridBagConstraints();
		gbc_lblDesignateBaseUri.anchor = GridBagConstraints.WEST;
		gbc_lblDesignateBaseUri.insets = new Insets(0, 0, 0, 5);
		gbc_lblDesignateBaseUri.gridx = 0;
		gbc_lblDesignateBaseUri.gridy = 6;
		dbImportPanel.add(lblDesignateBaseUri, gbc_lblDesignateBaseUri);

		customBaseURItextField = new JTextField();
		customBaseURItextField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		customBaseURItextField.setText("http://semoss.org/ontologies");
		customBaseURItextField.setColumns(10);
		GridBagConstraints gbc_customBaseURItextField = new GridBagConstraints();
		gbc_customBaseURItextField.gridwidth = 3;
		gbc_customBaseURItextField.insets = new Insets(0, 0, 5, 0);
		gbc_customBaseURItextField.fill = GridBagConstraints.HORIZONTAL;
		gbc_customBaseURItextField.gridx = 1;
		gbc_customBaseURItextField.gridy = 6;
		dbImportPanel.add(customBaseURItextField, gbc_customBaseURItextField);

		btnShowAdvancedImportFeatures = new CustomButton("Show Advanced Features");
		btnShowAdvancedImportFeatures.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_btnShowAdvancedImportFeatures = new GridBagConstraints();
		gbc_btnShowAdvancedImportFeatures.anchor = GridBagConstraints.WEST;
		gbc_btnShowAdvancedImportFeatures.gridwidth = 3;
		gbc_btnShowAdvancedImportFeatures.insets = new Insets(0, 0, 5, 5);
		gbc_btnShowAdvancedImportFeatures.gridx = 0;
		gbc_btnShowAdvancedImportFeatures.gridy = 7;
		dbImportPanel.add(btnShowAdvancedImportFeatures, gbc_btnShowAdvancedImportFeatures);

		advancedImportOptionsPanel = new JPanel();
		advancedImportOptionsPanel.setBackground(SystemColor.control);
		GridBagConstraints gbc_advancedImportOptionsPanel = new GridBagConstraints();
		gbc_advancedImportOptionsPanel.gridwidth = 4;
		gbc_advancedImportOptionsPanel.insets = new Insets(0, 0, 5, 0);
		gbc_advancedImportOptionsPanel.fill = GridBagConstraints.BOTH;
		gbc_advancedImportOptionsPanel.gridx = 0;
		gbc_advancedImportOptionsPanel.gridy = 7;
		dbImportPanel.add(advancedImportOptionsPanel,	gbc_advancedImportOptionsPanel);
		GridBagLayout gbl_advancedImportOptionsPanel = new GridBagLayout();
		gbl_advancedImportOptionsPanel.columnWidths = new int[] { 210, 0, 0, 0 };
		gbl_advancedImportOptionsPanel.rowHeights = new int[] { 0, 30, 0, 0 };
		gbl_advancedImportOptionsPanel.columnWeights = new double[] { 0.0, 0.0,	1.0, Double.MIN_VALUE };
		gbl_advancedImportOptionsPanel.rowWeights = new double[] { 0.0, 0.0, 0.0, Double.MIN_VALUE };
		advancedImportOptionsPanel.setLayout(gbl_advancedImportOptionsPanel);

		JLabel lblselectCustomMap = new JLabel("<HTML>Select Custom or Default Map File</HTML>");
		lblselectCustomMap.setMinimumSize(new Dimension(186, 16));
		lblselectCustomMap.setSize(new Dimension(44, 0));
		GridBagConstraints gbc_lblselectCustomMap = new GridBagConstraints();
		gbc_lblselectCustomMap.anchor = GridBagConstraints.WEST;
		gbc_lblselectCustomMap.insets = new Insets(0, 0, 5, 5);
		gbc_lblselectCustomMap.gridx = 0;
		gbc_lblselectCustomMap.gridy = 0;
		advancedImportOptionsPanel.add(lblselectCustomMap, gbc_lblselectCustomMap);
		lblselectCustomMap.setFont(new Font("Tahoma", Font.PLAIN, 12));

		mapBrowseBtn = new CustomButton("Browse");
		mapBrowseBtn.setFont(new Font("Tahoma", Font.BOLD, 11));
		mapBrowseBtn.setName("mapBrowseBtn");
		GridBagConstraints gbc_mapBrowseBtn = new GridBagConstraints();
		gbc_mapBrowseBtn.fill = GridBagConstraints.VERTICAL;
		gbc_mapBrowseBtn.insets = new Insets(0, 0, 5, 5);
		gbc_mapBrowseBtn.gridx = 1;
		gbc_mapBrowseBtn.gridy = 0;
		advancedImportOptionsPanel.add(mapBrowseBtn, gbc_mapBrowseBtn);

		importMapFileNameField = new JTextField();
		importMapFileNameField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		GridBagConstraints gbc_importMapFileNameField = new GridBagConstraints();
		gbc_importMapFileNameField.insets = new Insets(0, 0, 5, 0);
		gbc_importMapFileNameField.fill = GridBagConstraints.HORIZONTAL;
		gbc_importMapFileNameField.gridx = 2;
		gbc_importMapFileNameField.gridy = 0;
		advancedImportOptionsPanel.add(importMapFileNameField,	gbc_importMapFileNameField);
		importMapFileNameField.setColumns(10);

		JLabel lblselectCustomProp = new JLabel("<HTML>Select Custom SMSS File</HTML>");
		lblselectCustomProp.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblselectCustomProp = new GridBagConstraints();
		gbc_lblselectCustomProp.anchor = GridBagConstraints.WEST;
		gbc_lblselectCustomProp.insets = new Insets(0, 0, 5, 5);
		gbc_lblselectCustomProp.gridx = 0;
		gbc_lblselectCustomProp.gridy = 1;
		advancedImportOptionsPanel.add(lblselectCustomProp, gbc_lblselectCustomProp);

		dbPropBrowseButton = new CustomButton("Browse");
		dbPropBrowseButton.setFont(new Font("Tahoma", Font.BOLD, 11));
		dbPropBrowseButton.setName("dbPropBrowseButton");
		GridBagConstraints gbc_dbPropBrowseButton = new GridBagConstraints();
		gbc_dbPropBrowseButton.insets = new Insets(0, 0, 5, 5);
		gbc_dbPropBrowseButton.gridx = 1;
		gbc_dbPropBrowseButton.gridy = 1;
		advancedImportOptionsPanel.add(dbPropBrowseButton,	gbc_dbPropBrowseButton);

		dbPropFileNameField = new JTextField();
		dbPropFileNameField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		dbPropFileNameField.setColumns(10);
		GridBagConstraints gbc_dbPropFileNameField = new GridBagConstraints();
		gbc_dbPropFileNameField.insets = new Insets(0, 0, 5, 0);
		gbc_dbPropFileNameField.fill = GridBagConstraints.HORIZONTAL;
		gbc_dbPropFileNameField.gridx = 2;
		gbc_dbPropFileNameField.gridy = 1;
		advancedImportOptionsPanel.add(dbPropFileNameField,	gbc_dbPropFileNameField);

		JLabel lblselectCustomQuestionssheet = new JLabel("<HTML>Select Custom Questions Sheet</HTML>");
		lblselectCustomQuestionssheet.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblselectCustomQuestionssheet = new GridBagConstraints();
		gbc_lblselectCustomQuestionssheet.anchor = GridBagConstraints.WEST;
		gbc_lblselectCustomQuestionssheet.insets = new Insets(0, 0, 0, 5);
		gbc_lblselectCustomQuestionssheet.gridx = 0;
		gbc_lblselectCustomQuestionssheet.gridy = 2;
		advancedImportOptionsPanel.add(lblselectCustomQuestionssheet, gbc_lblselectCustomQuestionssheet);

		questionBrowseButton = new CustomButton("Browse");
		questionBrowseButton.setFont(new Font("Tahoma", Font.BOLD, 11));
		questionBrowseButton.setName("questionBrowseButton");
		GridBagConstraints gbc_questionBrowseButton = new GridBagConstraints();
		gbc_questionBrowseButton.insets = new Insets(0, 0, 0, 5);
		gbc_questionBrowseButton.gridx = 1;
		gbc_questionBrowseButton.gridy = 2;
		advancedImportOptionsPanel.add(questionBrowseButton, gbc_questionBrowseButton);

		questionFileNameField = new JTextField();
		questionFileNameField.setColumns(10);
		GridBagConstraints gbc_questionFileNameField = new GridBagConstraints();
		gbc_questionFileNameField.fill = GridBagConstraints.HORIZONTAL;
		gbc_questionFileNameField.gridx = 2;
		gbc_questionFileNameField.gridy = 2;
		advancedImportOptionsPanel.add(questionFileNameField, gbc_questionFileNameField);
		advancedImportOptionsPanel.setVisible(false);

		importButton = new CustomButton("Import");
		importButton.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_importButton = new GridBagConstraints();
		gbc_importButton.anchor = GridBagConstraints.EAST;
		gbc_importButton.gridx = 3;
		gbc_importButton.gridy = 7;
		dbImportPanel.add(importButton, gbc_importButton);

		GridBagConstraints gbc_importPanel = new GridBagConstraints();
		gbc_importPanel.fill = GridBagConstraints.HORIZONTAL;
		gbc_importPanel.anchor = GridBagConstraints.NORTH;
		gbc_importPanel.insets = new Insets(0, 15, 5, 0);
		gbc_importPanel.gridx = 0;
		gbc_importPanel.gridy = 0;
		imExPanel.add(importPanel, gbc_importPanel);

		JSeparator dbmod_separator_1 = new JSeparator();
		GridBagConstraints gbc_dbmod_separator_1 = new GridBagConstraints();
		gbc_dbmod_separator_1.fill = GridBagConstraints.HORIZONTAL;
		gbc_dbmod_separator_1.insets = new Insets(0, 0, 5, 0);
		gbc_dbmod_separator_1.gridx = 0;
		gbc_dbmod_separator_1.gridy = 1;
		imExPanel.add(dbmod_separator_1, gbc_dbmod_separator_1);

		JPanel modPanel = new JPanel();
		modPanel.setBackground(SystemColor.control);
		modPanel.setMinimumSize(new Dimension(0, 0));
		GridBagLayout gbl_modPanel = new GridBagLayout();
		gbl_modPanel.columnWidths = new int[] { 0, 0, 0, 0, 0, 501, 0, 0 };
		gbl_modPanel.rowHeights = new int[] { 0, 0, 0, 0, 0 };
		gbl_modPanel.columnWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, Double.MIN_VALUE };
		gbl_modPanel.rowWeights = new double[] { 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE };
		modPanel.setLayout(gbl_modPanel);

		JLabel lblDeleteInsert = new JLabel("Modify Data");
		lblDeleteInsert.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_lblDeleteInsert = new GridBagConstraints();
		gbc_lblDeleteInsert.anchor = GridBagConstraints.WEST;
		gbc_lblDeleteInsert.gridwidth = 7;
		gbc_lblDeleteInsert.insets = new Insets(10, 0, 5, 0);
		gbc_lblDeleteInsert.gridx = 0;
		gbc_lblDeleteInsert.gridy = 0;
		modPanel.add(lblDeleteInsert, gbc_lblDeleteInsert);

		JLabel lblCustomDelete = new JLabel("Custom Insert/Delete Query");
		lblCustomDelete.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblCustomDelete = new GridBagConstraints();
		gbc_lblCustomDelete.gridwidth = 2;
		gbc_lblCustomDelete.anchor = GridBagConstraints.WEST;
		gbc_lblCustomDelete.insets = new Insets(0, 0, 5, 5);
		gbc_lblCustomDelete.gridx = 0;
		gbc_lblCustomDelete.gridy = 1;
		modPanel.add(lblCustomDelete, gbc_lblCustomDelete);

		customUpdateScrollPane = new JScrollPane();
		customUpdateScrollPane.setMaximumSize(new Dimension(32767, 75));
		customUpdateScrollPane.setPreferredSize(new Dimension(6, 75));
		customUpdateScrollPane.setMinimumSize(new Dimension(0, 75));
		GridBagConstraints gbc_customUpdateScrollPane = new GridBagConstraints();
		gbc_customUpdateScrollPane.gridwidth = 6;
		gbc_customUpdateScrollPane.insets = new Insets(0, 0, 5, 5);
		gbc_customUpdateScrollPane.fill = GridBagConstraints.BOTH;
		gbc_customUpdateScrollPane.gridx = 0;
		gbc_customUpdateScrollPane.gridy = 2;
		modPanel.add(customUpdateScrollPane, gbc_customUpdateScrollPane);

		customUpdateTextPane = new JTextPane();
		customUpdateTextPane.setFont(new Font("Tahoma", Font.PLAIN, 11));
		customUpdateScrollPane.setViewportView(customUpdateTextPane);

		btnCustomUpdate = new CustomButton("Update");
		btnCustomUpdate.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_btnCustomUpdate = new GridBagConstraints();
		gbc_btnCustomUpdate.insets = new Insets(0, 0, 20, 0);
		gbc_btnCustomUpdate.anchor = GridBagConstraints.NORTHWEST;
		gbc_btnCustomUpdate.gridx = 6;
		gbc_btnCustomUpdate.gridy = 2;
		modPanel.add(btnCustomUpdate, gbc_btnCustomUpdate);

		GridBagConstraints gbc_modPanel = new GridBagConstraints();
		gbc_modPanel.insets = new Insets(0, 15, 5, 0);
		gbc_modPanel.fill = GridBagConstraints.BOTH;
		gbc_modPanel.gridx = 0;
		gbc_modPanel.gridy = 2;
		imExPanel.add(modPanel, gbc_modPanel);

		JPanel loadSheetExportPanel = new JPanel();
		loadSheetExportPanel.setBackground(SystemColor.control);
		GridBagLayout gbl_loadSheetExportPanel = new GridBagLayout();
		gbl_loadSheetExportPanel.columnWidths = new int[] { 0, 0, 0, 0 };
		gbl_loadSheetExportPanel.rowHeights = new int[] { 10, 0, 0, 0, 0, 36, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 37, 0, 0 };
		gbl_loadSheetExportPanel.columnWeights = new double[] { 0.0, 0.0, 0.0, Double.MIN_VALUE };
		gbl_loadSheetExportPanel.rowWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,	0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE };
		loadSheetExportPanel.setLayout(gbl_loadSheetExportPanel);

		JLabel lblExportDataTitle = new JLabel("Export Data");
		lblExportDataTitle.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_lblExportDataTitle = new GridBagConstraints();
		gbc_lblExportDataTitle.anchor = GridBagConstraints.WEST;
		gbc_lblExportDataTitle.insets = new Insets(0, 0, 5, 5);
		gbc_lblExportDataTitle.gridx = 0;
		gbc_lblExportDataTitle.gridy = 1;
		loadSheetExportPanel.add(lblExportDataTitle, gbc_lblExportDataTitle);

		JLabel lblExportDatabase = new JLabel("Source DB: ");
		lblExportDatabase.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblExportDatabase = new GridBagConstraints();
		gbc_lblExportDatabase.anchor = GridBagConstraints.WEST;
		gbc_lblExportDatabase.insets = new Insets(0, 0, 5, 5);
		gbc_lblExportDatabase.gridx = 0;
		gbc_lblExportDatabase.gridy = 2;
		loadSheetExportPanel.add(lblExportDatabase, gbc_lblExportDatabase);

		exportDataSourceComboBox = new ParamComboBox(new String[0]);
		exportDataSourceComboBox.setName("subjectNodeTypeComboBox1");
		exportDataSourceComboBox.setFont(new Font("Tahoma", Font.PLAIN, 11));
		exportDataSourceComboBox.setBackground(Color.GRAY);
		GridBagConstraints gbc_exportDataSourceComboBox = new GridBagConstraints();
		gbc_exportDataSourceComboBox.fill = GridBagConstraints.HORIZONTAL;
		gbc_exportDataSourceComboBox.insets = new Insets(0, 0, 5, 5);
		gbc_exportDataSourceComboBox.gridx = 0;
		gbc_exportDataSourceComboBox.gridy = 3;
		loadSheetExportPanel.add(exportDataSourceComboBox, gbc_exportDataSourceComboBox);

		JLabel lblSubjectNodeType = new JLabel("Node Type (In)");
		lblSubjectNodeType.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblSubjectNodeType = new GridBagConstraints();
		gbc_lblSubjectNodeType.anchor = GridBagConstraints.WEST;
		gbc_lblSubjectNodeType.insets = new Insets(0, 0, 5, 5);
		gbc_lblSubjectNodeType.gridx = 0;
		gbc_lblSubjectNodeType.gridy = 4;
		loadSheetExportPanel.add(lblSubjectNodeType, gbc_lblSubjectNodeType);

		JLabel lblRelationship = new JLabel("Relationship");
		lblRelationship.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblRelationship = new GridBagConstraints();
		gbc_lblRelationship.anchor = GridBagConstraints.WEST;
		gbc_lblRelationship.insets = new Insets(0, 0, 5, 5);
		gbc_lblRelationship.gridx = 1;
		gbc_lblRelationship.gridy = 4;
		loadSheetExportPanel.add(lblRelationship, gbc_lblRelationship);

		JLabel lblObjectNodeType = new JLabel("Node Type (Out)");
		lblObjectNodeType.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblObjectNodeType = new GridBagConstraints();
		gbc_lblObjectNodeType.anchor = GridBagConstraints.WEST;
		gbc_lblObjectNodeType.insets = new Insets(0, 0, 5, 0);
		gbc_lblObjectNodeType.gridx = 2;
		gbc_lblObjectNodeType.gridy = 4;
		loadSheetExportPanel.add(lblObjectNodeType, gbc_lblObjectNodeType);

		subjectNodeTypeComboBox1 = new ParamComboBox(new String[0]);
		subjectNodeTypeComboBox1.setFont(new Font("Tahoma", Font.PLAIN, 11));
		subjectNodeTypeComboBox1.setBackground(Color.GRAY);
		subjectNodeTypeComboBox1.setName(Constants.EXPORT_LOAD_SHEET_SUBJECT_NODE_TYPE_COMBOBOX	+ "1");
		subjectNodeTypeComboBox1.setPreferredSize(new Dimension(300, 25));
		GridBagConstraints gbc_subjectNodeTypeComboBox1 = new GridBagConstraints();
		gbc_subjectNodeTypeComboBox1.fill = GridBagConstraints.HORIZONTAL;
		gbc_subjectNodeTypeComboBox1.insets = new Insets(0, 0, 5, 5);
		gbc_subjectNodeTypeComboBox1.gridx = 0;
		gbc_subjectNodeTypeComboBox1.gridy = 5;
		loadSheetExportPanel.add(subjectNodeTypeComboBox1, gbc_subjectNodeTypeComboBox1);

		nodeRelationshipComboBox1 = new ParamComboBox(new String[0]);
		nodeRelationshipComboBox1.setFont(new Font("Tahoma", Font.PLAIN, 11));
		nodeRelationshipComboBox1.setBackground(Color.GRAY);
		nodeRelationshipComboBox1.setName(Constants.EXPORT_LOAD_SHEET_NODE_RELATIONSHIP_COMBOBOX + "1");
		nodeRelationshipComboBox1.setPreferredSize(new Dimension(300, 25));
		GridBagConstraints gbc_nodeRelationshipComboBox1 = new GridBagConstraints();
		gbc_nodeRelationshipComboBox1.anchor = GridBagConstraints.WEST;
		gbc_nodeRelationshipComboBox1.insets = new Insets(0, 0, 5, 5);
		gbc_nodeRelationshipComboBox1.gridx = 1;
		gbc_nodeRelationshipComboBox1.gridy = 5;
		loadSheetExportPanel.add(nodeRelationshipComboBox1,	gbc_nodeRelationshipComboBox1);

		objectNodeTypeComboBox1 = new ParamComboBox(new String[0]);
		objectNodeTypeComboBox1.setBackground(Color.GRAY);
		objectNodeTypeComboBox1.setName(Constants.EXPORT_LOAD_SHEET_OBJECT_NODE_TYPE_COMBOBOX + "1");
		objectNodeTypeComboBox1.setPreferredSize(new Dimension(300, 25));
		GridBagConstraints gbc_objectNodeTypeComboBox1 = new GridBagConstraints();
		gbc_objectNodeTypeComboBox1.anchor = GridBagConstraints.WEST;
		gbc_objectNodeTypeComboBox1.insets = new Insets(0, 0, 5, 0);
		gbc_objectNodeTypeComboBox1.gridx = 2;
		gbc_objectNodeTypeComboBox1.gridy = 5;
		loadSheetExportPanel.add(objectNodeTypeComboBox1, gbc_objectNodeTypeComboBox1);

		subjectNodeTypeComboBox2 = new ParamComboBox(new String[0]);
		subjectNodeTypeComboBox2.setFont(new Font("Tahoma", Font.PLAIN, 11));
		subjectNodeTypeComboBox2.setBackground(Color.GRAY);
		subjectNodeTypeComboBox2.setName(Constants.EXPORT_LOAD_SHEET_SUBJECT_NODE_TYPE_COMBOBOX	+ "2");
		subjectNodeTypeComboBox2.setPreferredSize(new Dimension(300, 25));
		GridBagConstraints gbc_subjectNodeTypeComboBox2 = new GridBagConstraints();
		gbc_subjectNodeTypeComboBox2.fill = GridBagConstraints.HORIZONTAL;
		gbc_subjectNodeTypeComboBox2.insets = new Insets(0, 0, 5, 5);
		gbc_subjectNodeTypeComboBox2.gridx = 0;
		gbc_subjectNodeTypeComboBox2.gridy = 6;
		subjectNodeTypeComboBox2.setVisible(false);
		loadSheetExportPanel.add(subjectNodeTypeComboBox2, gbc_subjectNodeTypeComboBox2);

		nodeRelationshipComboBox2 = new ParamComboBox(new String[0]);
		nodeRelationshipComboBox2.setFont(new Font("Tahoma", Font.PLAIN, 11));
		nodeRelationshipComboBox2.setBackground(Color.GRAY);
		nodeRelationshipComboBox2.setName(Constants.EXPORT_LOAD_SHEET_NODE_RELATIONSHIP_COMBOBOX + "2");
		nodeRelationshipComboBox2.setPreferredSize(new Dimension(300, 25));
		GridBagConstraints gbc_nodeRelationshipComboBox2 = new GridBagConstraints();
		gbc_nodeRelationshipComboBox2.insets = new Insets(0, 0, 5, 5);
		gbc_nodeRelationshipComboBox2.fill = GridBagConstraints.HORIZONTAL;
		gbc_nodeRelationshipComboBox2.gridx = 1;
		gbc_nodeRelationshipComboBox2.gridy = 6;
		nodeRelationshipComboBox2.setVisible(false);
		loadSheetExportPanel.add(nodeRelationshipComboBox2,	gbc_nodeRelationshipComboBox2);

		objectNodeTypeComboBox2 = new ParamComboBox(new String[0]);
		objectNodeTypeComboBox2.setBackground(Color.GRAY);
		objectNodeTypeComboBox2.setName(Constants.EXPORT_LOAD_SHEET_OBJECT_NODE_TYPE_COMBOBOX + "2");
		objectNodeTypeComboBox2.setPreferredSize(new Dimension(300, 25));
		GridBagConstraints gbc_objectNodeTypeComboBox2 = new GridBagConstraints();
		gbc_objectNodeTypeComboBox2.insets = new Insets(0, 0, 5, 0);
		gbc_objectNodeTypeComboBox2.fill = GridBagConstraints.HORIZONTAL;
		gbc_objectNodeTypeComboBox2.gridx = 2;
		gbc_objectNodeTypeComboBox2.gridy = 6;
		objectNodeTypeComboBox2.setVisible(false);
		loadSheetExportPanel.add(objectNodeTypeComboBox2, gbc_objectNodeTypeComboBox2);

		subjectNodeTypeComboBox3 = new ParamComboBox(new String[0]);
		subjectNodeTypeComboBox3.setFont(new Font("Tahoma", Font.PLAIN, 11));
		subjectNodeTypeComboBox3.setBackground(Color.GRAY);
		subjectNodeTypeComboBox3.setName(Constants.EXPORT_LOAD_SHEET_SUBJECT_NODE_TYPE_COMBOBOX	+ "3");
		subjectNodeTypeComboBox3.setPreferredSize(new Dimension(300, 25));
		GridBagConstraints gbc_subjectNodeTypeComboBox3 = new GridBagConstraints();
		gbc_subjectNodeTypeComboBox3.insets = new Insets(0, 0, 5, 5);
		gbc_subjectNodeTypeComboBox3.fill = GridBagConstraints.HORIZONTAL;
		gbc_subjectNodeTypeComboBox3.gridx = 0;
		gbc_subjectNodeTypeComboBox3.gridy = 7;
		subjectNodeTypeComboBox3.setVisible(false);
		loadSheetExportPanel.add(subjectNodeTypeComboBox3, gbc_subjectNodeTypeComboBox3);

		nodeRelationshipComboBox3 = new ParamComboBox(new String[0]);
		nodeRelationshipComboBox3.setFont(new Font("Tahoma", Font.PLAIN, 11));
		nodeRelationshipComboBox3.setBackground(Color.GRAY);
		nodeRelationshipComboBox3.setName(Constants.EXPORT_LOAD_SHEET_NODE_RELATIONSHIP_COMBOBOX + "3");
		nodeRelationshipComboBox3.setPreferredSize(new Dimension(300, 25));
		GridBagConstraints gbc_nodeRelationshipComboBox3 = new GridBagConstraints();
		gbc_nodeRelationshipComboBox3.insets = new Insets(0, 0, 5, 5);
		gbc_nodeRelationshipComboBox3.fill = GridBagConstraints.HORIZONTAL;
		gbc_nodeRelationshipComboBox3.gridx = 1;
		gbc_nodeRelationshipComboBox3.gridy = 7;
		nodeRelationshipComboBox3.setVisible(false);
		loadSheetExportPanel.add(nodeRelationshipComboBox3, gbc_nodeRelationshipComboBox3);

		objectNodeTypeComboBox3 = new ParamComboBox(new String[0]);
		objectNodeTypeComboBox3.setBackground(Color.GRAY);
		objectNodeTypeComboBox3.setName(Constants.EXPORT_LOAD_SHEET_OBJECT_NODE_TYPE_COMBOBOX + "3");
		objectNodeTypeComboBox3.setPreferredSize(new Dimension(300, 25));
		GridBagConstraints gbc_objectNodeTypeComboBox3 = new GridBagConstraints();
		gbc_objectNodeTypeComboBox3.insets = new Insets(0, 0, 5, 0);
		gbc_objectNodeTypeComboBox3.fill = GridBagConstraints.HORIZONTAL;
		gbc_objectNodeTypeComboBox3.gridx = 2;
		gbc_objectNodeTypeComboBox3.gridy = 7;
		objectNodeTypeComboBox3.setVisible(false);
		loadSheetExportPanel.add(objectNodeTypeComboBox3, gbc_objectNodeTypeComboBox3);

		subjectNodeTypeComboBox4 = new ParamComboBox(new String[0]);
		subjectNodeTypeComboBox4.setFont(new Font("Tahoma", Font.PLAIN, 11));
		subjectNodeTypeComboBox4.setBackground(Color.GRAY);
		subjectNodeTypeComboBox4.setName(Constants.EXPORT_LOAD_SHEET_SUBJECT_NODE_TYPE_COMBOBOX + "4");
		subjectNodeTypeComboBox4.setPreferredSize(new Dimension(300, 25));
		GridBagConstraints gbc_subjectNodeTypeComboBox4 = new GridBagConstraints();
		gbc_subjectNodeTypeComboBox4.insets = new Insets(0, 0, 5, 5);
		gbc_subjectNodeTypeComboBox4.fill = GridBagConstraints.HORIZONTAL;
		gbc_subjectNodeTypeComboBox4.gridx = 0;
		gbc_subjectNodeTypeComboBox4.gridy = 8;
		subjectNodeTypeComboBox4.setVisible(false);
		loadSheetExportPanel.add(subjectNodeTypeComboBox4, gbc_subjectNodeTypeComboBox4);

		nodeRelationshipComboBox4 = new ParamComboBox(new String[0]);
		nodeRelationshipComboBox4.setFont(new Font("Tahoma", Font.PLAIN, 11));
		nodeRelationshipComboBox4.setBackground(Color.GRAY);
		nodeRelationshipComboBox4.setName(Constants.EXPORT_LOAD_SHEET_NODE_RELATIONSHIP_COMBOBOX + "4");
		nodeRelationshipComboBox4.setPreferredSize(new Dimension(300, 25));
		GridBagConstraints gbc_nodeRelationshipComboBox4 = new GridBagConstraints();
		gbc_nodeRelationshipComboBox4.insets = new Insets(0, 0, 5, 5);
		gbc_nodeRelationshipComboBox4.fill = GridBagConstraints.HORIZONTAL;
		gbc_nodeRelationshipComboBox4.gridx = 1;
		gbc_nodeRelationshipComboBox4.gridy = 8;
		nodeRelationshipComboBox4.setVisible(false);
		loadSheetExportPanel.add(nodeRelationshipComboBox4,	gbc_nodeRelationshipComboBox4);

		objectNodeTypeComboBox4 = new ParamComboBox(new String[0]);
		objectNodeTypeComboBox4.setBackground(Color.GRAY);
		objectNodeTypeComboBox4.setName(Constants.EXPORT_LOAD_SHEET_OBJECT_NODE_TYPE_COMBOBOX + "4");
		objectNodeTypeComboBox4.setPreferredSize(new Dimension(300, 25));
		GridBagConstraints gbc_objectNodeTypeComboBox4 = new GridBagConstraints();
		gbc_objectNodeTypeComboBox4.insets = new Insets(0, 0, 5, 0);
		gbc_objectNodeTypeComboBox4.fill = GridBagConstraints.HORIZONTAL;
		gbc_objectNodeTypeComboBox4.gridx = 2;
		gbc_objectNodeTypeComboBox4.gridy = 8;
		objectNodeTypeComboBox4.setVisible(false);
		loadSheetExportPanel.add(objectNodeTypeComboBox4, gbc_objectNodeTypeComboBox4);

		subjectNodeTypeComboBox5 = new ParamComboBox(new String[0]);
		subjectNodeTypeComboBox5.setFont(new Font("Tahoma", Font.PLAIN, 11));
		subjectNodeTypeComboBox5.setBackground(Color.GRAY);
		subjectNodeTypeComboBox5.setName(Constants.EXPORT_LOAD_SHEET_SUBJECT_NODE_TYPE_COMBOBOX + "5");
		subjectNodeTypeComboBox5.setPreferredSize(new Dimension(300, 25));
		GridBagConstraints gbc_subjectNodeTypeComboBox5 = new GridBagConstraints();
		gbc_subjectNodeTypeComboBox5.insets = new Insets(0, 0, 5, 5);
		gbc_subjectNodeTypeComboBox5.fill = GridBagConstraints.HORIZONTAL;
		gbc_subjectNodeTypeComboBox5.gridx = 0;
		gbc_subjectNodeTypeComboBox5.gridy = 9;
		subjectNodeTypeComboBox5.setVisible(false);
		loadSheetExportPanel.add(subjectNodeTypeComboBox5, gbc_subjectNodeTypeComboBox5);

		nodeRelationshipComboBox5 = new ParamComboBox(new String[0]);
		nodeRelationshipComboBox5.setFont(new Font("Tahoma", Font.PLAIN, 11));
		nodeRelationshipComboBox5.setBackground(Color.GRAY);
		nodeRelationshipComboBox5.setName(Constants.EXPORT_LOAD_SHEET_NODE_RELATIONSHIP_COMBOBOX + "5");
		nodeRelationshipComboBox5.setPreferredSize(new Dimension(300, 25));
		GridBagConstraints gbc_nodeRelationshipComboBox5 = new GridBagConstraints();
		gbc_nodeRelationshipComboBox5.insets = new Insets(0, 0, 5, 5);
		gbc_nodeRelationshipComboBox5.fill = GridBagConstraints.HORIZONTAL;
		gbc_nodeRelationshipComboBox5.gridx = 1;
		gbc_nodeRelationshipComboBox5.gridy = 9;
		nodeRelationshipComboBox5.setVisible(false);
		loadSheetExportPanel.add(nodeRelationshipComboBox5,	gbc_nodeRelationshipComboBox5);

		objectNodeTypeComboBox5 = new ParamComboBox(new String[0]);
		objectNodeTypeComboBox5.setBackground(Color.GRAY);
		objectNodeTypeComboBox5.setName(Constants.EXPORT_LOAD_SHEET_OBJECT_NODE_TYPE_COMBOBOX + "5");
		objectNodeTypeComboBox5.setPreferredSize(new Dimension(300, 25));
		GridBagConstraints gbc_objectNodeTypeComboBox5 = new GridBagConstraints();
		gbc_objectNodeTypeComboBox5.insets = new Insets(0, 0, 5, 0);
		gbc_objectNodeTypeComboBox5.fill = GridBagConstraints.HORIZONTAL;
		gbc_objectNodeTypeComboBox5.gridx = 2;
		gbc_objectNodeTypeComboBox5.gridy = 9;
		objectNodeTypeComboBox5.setVisible(false);
		loadSheetExportPanel.add(objectNodeTypeComboBox5, gbc_objectNodeTypeComboBox5);

		subjectNodeTypeComboBox6 = new ParamComboBox(new String[0]);
		subjectNodeTypeComboBox6.setFont(new Font("Tahoma", Font.PLAIN, 11));
		subjectNodeTypeComboBox6.setBackground(Color.GRAY);
		subjectNodeTypeComboBox6.setName(Constants.EXPORT_LOAD_SHEET_SUBJECT_NODE_TYPE_COMBOBOX	+ "6");
		subjectNodeTypeComboBox6.setPreferredSize(new Dimension(300, 25));
		GridBagConstraints gbc_subjectNodeTypeComboBox6 = new GridBagConstraints();
		gbc_subjectNodeTypeComboBox6.insets = new Insets(0, 0, 5, 5);
		gbc_subjectNodeTypeComboBox6.fill = GridBagConstraints.HORIZONTAL;
		gbc_subjectNodeTypeComboBox6.gridx = 0;
		gbc_subjectNodeTypeComboBox6.gridy = 10;
		subjectNodeTypeComboBox6.setVisible(false);
		loadSheetExportPanel.add(subjectNodeTypeComboBox6, gbc_subjectNodeTypeComboBox6);

		nodeRelationshipComboBox6 = new ParamComboBox(new String[0]);
		nodeRelationshipComboBox6.setFont(new Font("Tahoma", Font.PLAIN, 11));
		nodeRelationshipComboBox6.setBackground(Color.GRAY);
		nodeRelationshipComboBox6.setName(Constants.EXPORT_LOAD_SHEET_NODE_RELATIONSHIP_COMBOBOX + "6");
		nodeRelationshipComboBox6.setPreferredSize(new Dimension(300, 25));
		GridBagConstraints gbc_nodeRelationshipComboBox6 = new GridBagConstraints();
		gbc_nodeRelationshipComboBox6.insets = new Insets(0, 0, 5, 5);
		gbc_nodeRelationshipComboBox6.fill = GridBagConstraints.HORIZONTAL;
		gbc_nodeRelationshipComboBox6.gridx = 1;
		gbc_nodeRelationshipComboBox6.gridy = 10;
		nodeRelationshipComboBox6.setVisible(false);
		loadSheetExportPanel.add(nodeRelationshipComboBox6,
				gbc_nodeRelationshipComboBox6);

		objectNodeTypeComboBox6 = new ParamComboBox(new String[0]);
		objectNodeTypeComboBox6.setBackground(Color.GRAY);
		objectNodeTypeComboBox6.setName(Constants.EXPORT_LOAD_SHEET_OBJECT_NODE_TYPE_COMBOBOX + "6");
		objectNodeTypeComboBox6.setPreferredSize(new Dimension(300, 25));
		GridBagConstraints gbc_objectNodeTypeComboBox6 = new GridBagConstraints();
		gbc_objectNodeTypeComboBox6.insets = new Insets(0, 0, 5, 0);
		gbc_objectNodeTypeComboBox6.fill = GridBagConstraints.HORIZONTAL;
		gbc_objectNodeTypeComboBox6.gridx = 2;
		gbc_objectNodeTypeComboBox6.gridy = 10;
		objectNodeTypeComboBox6.setVisible(false);
		loadSheetExportPanel.add(objectNodeTypeComboBox6, gbc_objectNodeTypeComboBox6);

		subjectNodeTypeComboBox7 = new ParamComboBox(new String[0]);
		subjectNodeTypeComboBox7.setFont(new Font("Tahoma", Font.PLAIN, 11));
		subjectNodeTypeComboBox7.setBackground(Color.GRAY);
		subjectNodeTypeComboBox7.setName(Constants.EXPORT_LOAD_SHEET_SUBJECT_NODE_TYPE_COMBOBOX	+ "7");
		subjectNodeTypeComboBox7.setPreferredSize(new Dimension(300, 25));
		GridBagConstraints gbc_subjectNodeTypeComboBox7 = new GridBagConstraints();
		gbc_subjectNodeTypeComboBox7.insets = new Insets(0, 0, 5, 5);
		gbc_subjectNodeTypeComboBox7.fill = GridBagConstraints.HORIZONTAL;
		gbc_subjectNodeTypeComboBox7.gridx = 0;
		gbc_subjectNodeTypeComboBox7.gridy = 11;
		subjectNodeTypeComboBox7.setVisible(false);
		loadSheetExportPanel.add(subjectNodeTypeComboBox7, gbc_subjectNodeTypeComboBox7);

		nodeRelationshipComboBox7 = new ParamComboBox(new String[0]);
		nodeRelationshipComboBox7.setFont(new Font("Tahoma", Font.PLAIN, 11));
		nodeRelationshipComboBox7.setBackground(Color.GRAY);
		nodeRelationshipComboBox7.setName(Constants.EXPORT_LOAD_SHEET_NODE_RELATIONSHIP_COMBOBOX + "7");
		nodeRelationshipComboBox7.setPreferredSize(new Dimension(300, 25));
		GridBagConstraints gbc_nodeRelationshipComboBox7 = new GridBagConstraints();
		gbc_nodeRelationshipComboBox7.insets = new Insets(0, 0, 5, 5);
		gbc_nodeRelationshipComboBox7.fill = GridBagConstraints.HORIZONTAL;
		gbc_nodeRelationshipComboBox7.gridx = 1;
		gbc_nodeRelationshipComboBox7.gridy = 11;
		nodeRelationshipComboBox7.setVisible(false);
		loadSheetExportPanel.add(nodeRelationshipComboBox7,	gbc_nodeRelationshipComboBox7);

		objectNodeTypeComboBox7 = new ParamComboBox(new String[0]);
		objectNodeTypeComboBox7.setBackground(Color.GRAY);
		objectNodeTypeComboBox7.setName(Constants.EXPORT_LOAD_SHEET_OBJECT_NODE_TYPE_COMBOBOX + "7");
		objectNodeTypeComboBox7.setPreferredSize(new Dimension(300, 25));
		GridBagConstraints gbc_objectNodeTypeComboBox7 = new GridBagConstraints();
		gbc_objectNodeTypeComboBox7.insets = new Insets(0, 0, 5, 0);
		gbc_objectNodeTypeComboBox7.fill = GridBagConstraints.HORIZONTAL;
		gbc_objectNodeTypeComboBox7.gridx = 2;
		gbc_objectNodeTypeComboBox7.gridy = 11;
		objectNodeTypeComboBox7.setVisible(false);
		loadSheetExportPanel.add(objectNodeTypeComboBox7, gbc_objectNodeTypeComboBox7);

		subjectNodeTypeComboBox8 = new ParamComboBox(new String[0]);
		subjectNodeTypeComboBox8.setFont(new Font("Tahoma", Font.PLAIN, 11));
		subjectNodeTypeComboBox8.setBackground(Color.GRAY);
		subjectNodeTypeComboBox8.setName(Constants.EXPORT_LOAD_SHEET_SUBJECT_NODE_TYPE_COMBOBOX	+ "8");
		subjectNodeTypeComboBox8.setPreferredSize(new Dimension(300, 25));
		GridBagConstraints gbc_subjectNodeTypeComboBox8 = new GridBagConstraints();
		gbc_subjectNodeTypeComboBox8.insets = new Insets(0, 0, 5, 5);
		gbc_subjectNodeTypeComboBox8.fill = GridBagConstraints.HORIZONTAL;
		gbc_subjectNodeTypeComboBox8.gridx = 0;
		gbc_subjectNodeTypeComboBox8.gridy = 12;
		subjectNodeTypeComboBox8.setVisible(false);
		loadSheetExportPanel.add(subjectNodeTypeComboBox8, gbc_subjectNodeTypeComboBox8);

		nodeRelationshipComboBox8 = new ParamComboBox(new String[0]);
		nodeRelationshipComboBox8.setFont(new Font("Tahoma", Font.PLAIN, 11));
		nodeRelationshipComboBox8.setBackground(Color.GRAY);
		nodeRelationshipComboBox8.setName(Constants.EXPORT_LOAD_SHEET_NODE_RELATIONSHIP_COMBOBOX + "8");
		nodeRelationshipComboBox8.setPreferredSize(new Dimension(300, 25));
		GridBagConstraints gbc_nodeRelationshipComboBox8 = new GridBagConstraints();
		gbc_nodeRelationshipComboBox8.insets = new Insets(0, 0, 5, 5);
		gbc_nodeRelationshipComboBox8.fill = GridBagConstraints.HORIZONTAL;
		gbc_nodeRelationshipComboBox8.gridx = 1;
		gbc_nodeRelationshipComboBox8.gridy = 12;
		nodeRelationshipComboBox8.setVisible(false);
		loadSheetExportPanel.add(nodeRelationshipComboBox8, gbc_nodeRelationshipComboBox8);

		objectNodeTypeComboBox8 = new ParamComboBox(new String[0]);
		objectNodeTypeComboBox8.setBackground(Color.GRAY);
		objectNodeTypeComboBox8.setName(Constants.EXPORT_LOAD_SHEET_OBJECT_NODE_TYPE_COMBOBOX	+ "8");
		objectNodeTypeComboBox8.setPreferredSize(new Dimension(300, 25));
		GridBagConstraints gbc_objectNodeTypeComboBox8 = new GridBagConstraints();
		gbc_objectNodeTypeComboBox8.insets = new Insets(0, 0, 5, 0);
		gbc_objectNodeTypeComboBox8.fill = GridBagConstraints.HORIZONTAL;
		gbc_objectNodeTypeComboBox8.gridx = 2;
		gbc_objectNodeTypeComboBox8.gridy = 12;
		objectNodeTypeComboBox8.setVisible(false);
		loadSheetExportPanel.add(objectNodeTypeComboBox8, gbc_objectNodeTypeComboBox8);

		subjectNodeTypeComboBox9 = new ParamComboBox(new String[0]);
		subjectNodeTypeComboBox9.setFont(new Font("Tahoma", Font.PLAIN, 11));
		subjectNodeTypeComboBox9.setBackground(Color.GRAY);
		subjectNodeTypeComboBox9.setName(Constants.EXPORT_LOAD_SHEET_SUBJECT_NODE_TYPE_COMBOBOX	+ "9");
		subjectNodeTypeComboBox9.setPreferredSize(new Dimension(300, 25));
		GridBagConstraints gbc_subjectNodeTypeComboBox9 = new GridBagConstraints();
		gbc_subjectNodeTypeComboBox9.insets = new Insets(0, 0, 5, 5);
		gbc_subjectNodeTypeComboBox9.fill = GridBagConstraints.HORIZONTAL;
		gbc_subjectNodeTypeComboBox9.gridx = 0;
		gbc_subjectNodeTypeComboBox9.gridy = 13;
		subjectNodeTypeComboBox9.setVisible(false);
		loadSheetExportPanel.add(subjectNodeTypeComboBox9, gbc_subjectNodeTypeComboBox9);

		nodeRelationshipComboBox9 = new ParamComboBox(new String[0]);
		nodeRelationshipComboBox9.setFont(new Font("Tahoma", Font.PLAIN, 11));
		nodeRelationshipComboBox9.setBackground(Color.GRAY);
		nodeRelationshipComboBox9.setName(Constants.EXPORT_LOAD_SHEET_NODE_RELATIONSHIP_COMBOBOX + "9");
		nodeRelationshipComboBox9.setPreferredSize(new Dimension(300, 25));
		GridBagConstraints gbc_nodeRelationshipComboBox9 = new GridBagConstraints();
		gbc_nodeRelationshipComboBox9.insets = new Insets(0, 0, 5, 5);
		gbc_nodeRelationshipComboBox9.fill = GridBagConstraints.HORIZONTAL;
		gbc_nodeRelationshipComboBox9.gridx = 1;
		gbc_nodeRelationshipComboBox9.gridy = 13;
		nodeRelationshipComboBox9.setVisible(false);
		loadSheetExportPanel.add(nodeRelationshipComboBox9,	gbc_nodeRelationshipComboBox9);

		objectNodeTypeComboBox9 = new ParamComboBox(new String[0]);
		objectNodeTypeComboBox9.setBackground(Color.GRAY);
		objectNodeTypeComboBox9.setName(Constants.EXPORT_LOAD_SHEET_OBJECT_NODE_TYPE_COMBOBOX + "9");
		objectNodeTypeComboBox9.setPreferredSize(new Dimension(300, 25));
		GridBagConstraints gbc_objectNodeTypeComboBox9 = new GridBagConstraints();
		gbc_objectNodeTypeComboBox9.insets = new Insets(0, 0, 5, 0);
		gbc_objectNodeTypeComboBox9.fill = GridBagConstraints.HORIZONTAL;
		gbc_objectNodeTypeComboBox9.gridx = 2;
		gbc_objectNodeTypeComboBox9.gridy = 13;
		objectNodeTypeComboBox9.setVisible(false);
		loadSheetExportPanel.add(objectNodeTypeComboBox9, gbc_objectNodeTypeComboBox9);

		btnAddExport = new CustomButton("Add Node/Relationship ");
		btnAddExport.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_btnAddExport = new GridBagConstraints();
		gbc_btnAddExport.anchor = GridBagConstraints.WEST;
		gbc_btnAddExport.insets = new Insets(0, 0, 5, 5);
		gbc_btnAddExport.gridx = 0;
		gbc_btnAddExport.gridy = 14;
		loadSheetExportPanel.add(btnAddExport, gbc_btnAddExport);

		lblMaxExportLimit = new JLabel("Max Export Limit: "	+ Constants.MAX_EXPORTS);
		lblMaxExportLimit.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblMaxExportLimit = new GridBagConstraints();
		gbc_lblMaxExportLimit.anchor = GridBagConstraints.WEST;
		gbc_lblMaxExportLimit.insets = new Insets(0, 0, 5, 5);
		gbc_lblMaxExportLimit.gridx = 0;
		gbc_lblMaxExportLimit.gridy = 15;
		lblMaxExportLimit.setVisible(false);

		JSeparator dbmod_separator_2 = new JSeparator();
		GridBagConstraints gbc_dbmod_separator_2 = new GridBagConstraints();
		gbc_dbmod_separator_2.fill = GridBagConstraints.HORIZONTAL;
		gbc_dbmod_separator_2.insets = new Insets(0, 0, 5, 0);
		gbc_dbmod_separator_2.gridx = 0;
		gbc_dbmod_separator_2.gridy = 3;
		imExPanel.add(dbmod_separator_2, gbc_dbmod_separator_2);
		loadSheetExportPanel.add(lblMaxExportLimit, gbc_lblMaxExportLimit);

		btnClearAll = new CustomButton("Clear All");
		btnClearAll.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_btnClearAll = new GridBagConstraints();
		gbc_btnClearAll.anchor = GridBagConstraints.WEST;
		gbc_btnClearAll.insets = new Insets(0, 0, 5, 5);
		gbc_btnClearAll.gridx = 0;
		gbc_btnClearAll.gridy = 16;
		loadSheetExportPanel.add(btnClearAll, gbc_btnClearAll);

		rigidArea = Box.createRigidArea(new Dimension(20, 20));
		GridBagConstraints gbc_rigidArea = new GridBagConstraints();
		gbc_rigidArea.insets = new Insets(0, 0, 5, 5);
		gbc_rigidArea.gridx = 0;
		gbc_rigidArea.gridy = 17;
		loadSheetExportPanel.add(rigidArea, gbc_rigidArea);

		btnExportNodeLoadSheets = new CustomButton("Export Node (In) Load Sheet");
		btnExportNodeLoadSheets.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_btnExportNodeLoadSheets = new GridBagConstraints();
		gbc_btnExportNodeLoadSheets.anchor = GridBagConstraints.WEST;
		gbc_btnExportNodeLoadSheets.insets = new Insets(0, 0, 5, 5);
		gbc_btnExportNodeLoadSheets.gridx = 0;
		gbc_btnExportNodeLoadSheets.gridy = 18;
		loadSheetExportPanel.add(btnExportNodeLoadSheets, gbc_btnExportNodeLoadSheets);

		btnExportRelationshipsLoadSheets = new CustomButton("Export Relationship Load Sheet");
		btnExportRelationshipsLoadSheets.setFont(new Font("Tahoma", Font.BOLD,	11));
		GridBagConstraints gbc_btnExportRelationshipsLoad = new GridBagConstraints();
		gbc_btnExportRelationshipsLoad.anchor = GridBagConstraints.WEST;
		gbc_btnExportRelationshipsLoad.insets = new Insets(0, 0, 5, 5);
		gbc_btnExportRelationshipsLoad.gridx = 1;
		gbc_btnExportRelationshipsLoad.gridy = 18;
		loadSheetExportPanel.add(btnExportRelationshipsLoadSheets, gbc_btnExportRelationshipsLoad);

		GridBagConstraints gbc_loadSheetExportPanel = new GridBagConstraints();
		gbc_loadSheetExportPanel.insets = new Insets(0, 15, 5, 0);
		gbc_loadSheetExportPanel.fill = GridBagConstraints.BOTH;
		gbc_loadSheetExportPanel.gridx = 0;
		gbc_loadSheetExportPanel.gridy = 4;
		imExPanel.add(loadSheetExportPanel, gbc_loadSheetExportPanel);
		Style.registerTargetClassName(btnCustomUpdate, ".standardButton");
		Style.registerTargetClassName(fileBrowseBtn, ".standardButton");
		Style.registerTargetClassName(btnShowAdvancedImportFeatures, ".standardButton");
		Style.registerTargetClassName(mapBrowseBtn, ".standardButton");
		Style.registerTargetClassName(dbPropBrowseButton, ".standardButton");
		Style.registerTargetClassName(questionBrowseButton, ".standardButton");
		Style.registerTargetClassName(importButton, ".standardButton");
		Style.registerTargetClassName(btnTestRDBMSConnection, ".standardButton");
		Style.registerTargetClassName(btnAddExport, ".standardButton");
		Style.registerTargetClassName(btnClearAll, ".standardButton");
		Style.registerTargetClassName(btnExportNodeLoadSheets,	".standardButton");
		Style.registerTargetClassName(btnExportRelationshipsLoadSheets, ".standardButton");
		customUpdateScrollPane.getVerticalScrollBar().setUI(new NewScrollBarUI());

		JTabbedPane tapTabPane = new JTabbedPane();
		rightView.addTab("MHS TAP", null, tapTabPane, null);

		JPanel financialsPanel = new JPanel();
		financialsPanel.setBackground(SystemColor.control);
		JScrollPane calcPanelScroll = new JScrollPane(financialsPanel);
		tapTabPane.addTab("Financial Analysis", null, calcPanelScroll, null);
		GridBagLayout gbl_financialsPanel = new GridBagLayout();
		gbl_financialsPanel.columnWidths = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
		gbl_financialsPanel.rowHeights = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
		gbl_financialsPanel.columnWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE };
		gbl_financialsPanel.rowWeights = new double[] { 0.0, 0.0, 0.0, 0.0,	0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,	0.0, 0.0, 0.0, Double.MIN_VALUE };
		financialsPanel.setLayout(gbl_financialsPanel);

		serviceSelectionBtn = new ToggleButton("Service Selection Mode");
		serviceSelectionBtn.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_serviceSelectionBtn = new GridBagConstraints();
		gbc_serviceSelectionBtn.anchor = GridBagConstraints.WEST;
		gbc_serviceSelectionBtn.insets = new Insets(10, 0, 5, 5);
		gbc_serviceSelectionBtn.gridx = 6;
		gbc_serviceSelectionBtn.gridy = 1;
		financialsPanel.add(serviceSelectionBtn, gbc_serviceSelectionBtn);

		tierCheck1 = new JCheckBox("Tier 1");
		tierCheck1.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_tierCheck1 = new GridBagConstraints();
		gbc_tierCheck1.anchor = GridBagConstraints.WEST;
		gbc_tierCheck1.insets = new Insets(10, 0, 5, 5);
		gbc_tierCheck1.gridx = 7;
		gbc_tierCheck1.gridy = 1;
		financialsPanel.add(tierCheck1, gbc_tierCheck1);

		tierCheck2 = new JCheckBox("Tier 2");
		tierCheck2.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_tierCheck2 = new GridBagConstraints();
		gbc_tierCheck2.anchor = GridBagConstraints.WEST;
		gbc_tierCheck2.insets = new Insets(10, 0, 5, 5);
		gbc_tierCheck2.gridx = 8;
		gbc_tierCheck2.gridy = 1;
		financialsPanel.add(tierCheck2, gbc_tierCheck2);

		tierCheck3 = new JCheckBox("Tier 3");
		tierCheck3.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_tierCheck3 = new GridBagConstraints();
		gbc_tierCheck3.anchor = GridBagConstraints.WEST;
		gbc_tierCheck3.insets = new Insets(10, 0, 5, 0);
		gbc_tierCheck3.gridx = 9;
		gbc_tierCheck3.gridy = 1;
		financialsPanel.add(tierCheck3, gbc_tierCheck3);

		transitionServicePanel = new ServiceSelectPanel();
		transitionServicePanel.setBackground(SystemColor.control);
		serviceSelectScrollPane = new JScrollPane(transitionServicePanel);
		GridBagConstraints gbc_scrollPane_13 = new GridBagConstraints();
		gbc_scrollPane_13.gridwidth = 4;
		gbc_scrollPane_13.gridheight = 18;
		gbc_scrollPane_13.fill = GridBagConstraints.BOTH;
		gbc_scrollPane_13.gridx = 6;
		gbc_scrollPane_13.gridy = 2;
		financialsPanel.add(serviceSelectScrollPane, gbc_scrollPane_13);
		serviceSelectScrollPane.setPreferredSize(new Dimension(300, 650));
		serviceSelectScrollPane.setVisible(false);

		JLabel lblTransitionCostReports = new JLabel("Transition Cost Reports");
		lblTransitionCostReports.setFont(new Font("Tahoma", Font.BOLD, 12));
		lblTransitionCostReports.setBackground(Color.WHITE);
		GridBagConstraints gbc_lblTransitionCostReports = new GridBagConstraints();
		gbc_lblTransitionCostReports.gridwidth = 6;
		gbc_lblTransitionCostReports.anchor = GridBagConstraints.WEST;
		gbc_lblTransitionCostReports.insets = new Insets(10, 10, 5, 5);
		gbc_lblTransitionCostReports.gridx = 0;
		gbc_lblTransitionCostReports.gridy = 1;
		financialsPanel.add(lblTransitionCostReports, gbc_lblTransitionCostReports);

		JLabel lblTransitionEstimatesGenerator = new JLabel("Transition Estimates Generator:");
		lblTransitionEstimatesGenerator.setFont(new Font("Tahoma", Font.PLAIN,	12));
		GridBagConstraints gbc_lblTransitionEstimatesGenerator = new GridBagConstraints();
		gbc_lblTransitionEstimatesGenerator.gridwidth = 6;
		gbc_lblTransitionEstimatesGenerator.anchor = GridBagConstraints.NORTHWEST;
		gbc_lblTransitionEstimatesGenerator.insets = new Insets(5, 20, 10, 5);
		gbc_lblTransitionEstimatesGenerator.gridx = 0;
		gbc_lblTransitionEstimatesGenerator.gridy = 2;
		financialsPanel.add(lblTransitionEstimatesGenerator, gbc_lblTransitionEstimatesGenerator);

		transReportCheckBoxPanel = new JPanel();
		transReportCheckBoxPanel.setBackground(SystemColor.control);
		transReportCheckBoxPanel.setMinimumSize(new Dimension(50, 1000));
		transReportCheckBoxPanel.setBorder(null);
		GridBagConstraints gbc_transReportCheckBoxPanel = new GridBagConstraints();
		gbc_transReportCheckBoxPanel.gridheight = 2;
		gbc_transReportCheckBoxPanel.insets = new Insets(0, 10, 5, 5);
		gbc_transReportCheckBoxPanel.fill = GridBagConstraints.BOTH;
		gbc_transReportCheckBoxPanel.gridx = 4;
		gbc_transReportCheckBoxPanel.gridy = 3;
		financialsPanel.add(transReportCheckBoxPanel, gbc_transReportCheckBoxPanel);
		GridBagLayout gbl_transReportCheckBoxPanel = new GridBagLayout();
		gbl_transReportCheckBoxPanel.columnWidths = new int[] { 0, 0 };
		gbl_transReportCheckBoxPanel.rowHeights = new int[] { 0, 0, 0, 0, 0, 0,	0 };
		gbl_transReportCheckBoxPanel.columnWeights = new double[] { 0.0, Double.MIN_VALUE };
		gbl_transReportCheckBoxPanel.rowWeights = new double[] { 0.0, 0.0, 0.0,	0.0, 0.0, 0.0, Double.MIN_VALUE };
		transReportCheckBoxPanel.setLayout(gbl_transReportCheckBoxPanel);

		JLabel lblSelectCostsTo = new JLabel("Select Costs to View");
		lblSelectCostsTo.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblSelectCostsTo = new GridBagConstraints();
		gbc_lblSelectCostsTo.insets = new Insets(0, 0, 5, 0);
		gbc_lblSelectCostsTo.gridx = 0;
		gbc_lblSelectCostsTo.gridy = 0;
		transReportCheckBoxPanel.add(lblSelectCostsTo, gbc_lblSelectCostsTo);

		chckbxDataFederationTransReport = new JCheckBox("Data Federation");
		chckbxDataFederationTransReport.setFont(new Font("Tahoma", Font.PLAIN,	12));
		GridBagConstraints gbc_chckbxDataFederationTransReport = new GridBagConstraints();
		gbc_chckbxDataFederationTransReport.anchor = GridBagConstraints.WEST;
		gbc_chckbxDataFederationTransReport.insets = new Insets(0, 0, 5, 0);
		gbc_chckbxDataFederationTransReport.gridx = 0;
		gbc_chckbxDataFederationTransReport.gridy = 1;
		transReportCheckBoxPanel.add(chckbxDataFederationTransReport, gbc_chckbxDataFederationTransReport);

		chckbxBLUprovider = new JCheckBox("BLU Provider");
		chckbxBLUprovider.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_chckbxBLUprovider = new GridBagConstraints();
		gbc_chckbxBLUprovider.anchor = GridBagConstraints.WEST;
		gbc_chckbxBLUprovider.insets = new Insets(0, 0, 5, 0);
		gbc_chckbxBLUprovider.gridx = 0;
		gbc_chckbxBLUprovider.gridy = 2;
		transReportCheckBoxPanel.add(chckbxBLUprovider, gbc_chckbxBLUprovider);

		chckbxDataConsumer = new JCheckBox("Consumer");
		chckbxDataConsumer.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_chckbxDataConsumer = new GridBagConstraints();
		gbc_chckbxDataConsumer.anchor = GridBagConstraints.WEST;
		gbc_chckbxDataConsumer.insets = new Insets(0, 0, 5, 0);
		gbc_chckbxDataConsumer.gridx = 0;
		gbc_chckbxDataConsumer.gridy = 3;
		transReportCheckBoxPanel.add(chckbxDataConsumer, gbc_chckbxDataConsumer);

		chckbxDataEsbImplementation = new JCheckBox("Data ESB Implementation");
		chckbxDataEsbImplementation.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_chckbxDataEsbImplementation = new GridBagConstraints();
		gbc_chckbxDataEsbImplementation.insets = new Insets(0, 0, 5, 0);
		gbc_chckbxDataEsbImplementation.gridx = 0;
		gbc_chckbxDataEsbImplementation.gridy = 4;
		transReportCheckBoxPanel.add(chckbxDataEsbImplementation, gbc_chckbxDataEsbImplementation);
		chckbxDataEsbImplementation.setVisible(false);

		chckbxBluEsbImplementation = new JCheckBox("BLU ESB Implementation");
		chckbxBluEsbImplementation.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_chckbxBluEsbImplementation = new GridBagConstraints();
		gbc_chckbxBluEsbImplementation.anchor = GridBagConstraints.WEST;
		gbc_chckbxBluEsbImplementation.gridx = 0;
		gbc_chckbxBluEsbImplementation.gridy = 5;
		transReportCheckBoxPanel.add(chckbxBluEsbImplementation, gbc_chckbxBluEsbImplementation);
		chckbxBluEsbImplementation.setVisible(false);

		transReportSysDropDownPanel = new JPanel();
		transReportSysDropDownPanel.setBackground(SystemColor.control);
		GridBagConstraints gbc_transReportSysDropDownPanel = new GridBagConstraints();
		gbc_transReportSysDropDownPanel.gridheight = 2;
		gbc_transReportSysDropDownPanel.insets = new Insets(0, 10, 5, 5);
		gbc_transReportSysDropDownPanel.fill = GridBagConstraints.BOTH;
		gbc_transReportSysDropDownPanel.gridx = 5;
		gbc_transReportSysDropDownPanel.gridy = 3;
		financialsPanel.add(transReportSysDropDownPanel, gbc_transReportSysDropDownPanel);
		GridBagLayout gbl_transReportSysDropDownPanel = new GridBagLayout();
		gbl_transReportSysDropDownPanel.columnWidths = new int[] { 0, 0 };
		gbl_transReportSysDropDownPanel.rowHeights = new int[] { 0, 0, 0 };
		gbl_transReportSysDropDownPanel.columnWeights = new double[] { 1.0, Double.MIN_VALUE };
		gbl_transReportSysDropDownPanel.rowWeights = new double[] { 0.0, 0.0, Double.MIN_VALUE };
		transReportSysDropDownPanel.setLayout(gbl_transReportSysDropDownPanel);

		JLabel lblSelectSystem = new JLabel("Select System");
		lblSelectSystem.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblSelectSystem = new GridBagConstraints();
		gbc_lblSelectSystem.insets = new Insets(0, 0, 5, 0);
		gbc_lblSelectSystem.gridx = 0;
		gbc_lblSelectSystem.gridy = 0;
		transReportSysDropDownPanel.add(lblSelectSystem, gbc_lblSelectSystem);

		transCostReportSystemcomboBox = new TransitionReportComboBox(fetching);
		transCostReportSystemcomboBox.setFont(new Font("Tahoma", Font.PLAIN, 11));
		transCostReportSystemcomboBox.setBackground(Color.GRAY);
		transCostReportSystemcomboBox.setPreferredSize(new Dimension(200, 25));
		GridBagConstraints gbc_transCostReportSystemcomboBox = new GridBagConstraints();
		gbc_transCostReportSystemcomboBox.fill = GridBagConstraints.HORIZONTAL;
		gbc_transCostReportSystemcomboBox.gridx = 0;
		gbc_transCostReportSystemcomboBox.gridy = 1;
		transReportSysDropDownPanel.add(transCostReportSystemcomboBox, gbc_transCostReportSystemcomboBox);

		transReportTypeDropDownPanel = new JPanel();
		transReportTypeDropDownPanel.setBackground(SystemColor.control);
		GridBagConstraints gbc_transReportTypeDropDownPanel = new GridBagConstraints();
		gbc_transReportTypeDropDownPanel.gridheight = 2;
		gbc_transReportTypeDropDownPanel.insets = new Insets(0, 10, 5, 5);
		gbc_transReportTypeDropDownPanel.fill = GridBagConstraints.BOTH;
		gbc_transReportTypeDropDownPanel.gridx = 3;
		gbc_transReportTypeDropDownPanel.gridy = 3;
		financialsPanel.add(transReportTypeDropDownPanel, gbc_transReportTypeDropDownPanel);
		GridBagLayout gbl_transReportTypeDropDownPanel = new GridBagLayout();
		gbl_transReportTypeDropDownPanel.columnWidths = new int[] { 0, 0 };
		gbl_transReportTypeDropDownPanel.rowHeights = new int[] { 0, 0, 0 };
		gbl_transReportTypeDropDownPanel.columnWeights = new double[] { 1.0, Double.MIN_VALUE };
		gbl_transReportTypeDropDownPanel.rowWeights = new double[] { 0.0, 0.0, Double.MIN_VALUE };
		transReportTypeDropDownPanel.setLayout(gbl_transReportTypeDropDownPanel);

		JLabel lblSelectReportType = new JLabel("Select Report Type");
		lblSelectReportType.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblSelectReportType = new GridBagConstraints();
		gbc_lblSelectReportType.insets = new Insets(0, 0, 5, 0);
		gbc_lblSelectReportType.gridx = 0;
		gbc_lblSelectReportType.gridy = 0;
		transReportTypeDropDownPanel.add(lblSelectReportType, gbc_lblSelectReportType);

		TransReportTypecomboBox = new JComboBox();
		TransReportTypecomboBox.setFont(new Font("Tahoma", Font.PLAIN, 11));
		TransReportTypecomboBox.setBackground(Color.GRAY);
		GridBagConstraints gbc_TransReportTypecomboBox = new GridBagConstraints();
		gbc_TransReportTypecomboBox.fill = GridBagConstraints.HORIZONTAL;
		gbc_TransReportTypecomboBox.gridx = 0;
		gbc_TransReportTypecomboBox.gridy = 1;
		transReportTypeDropDownPanel.add(TransReportTypecomboBox, gbc_TransReportTypecomboBox);
		TransReportTypecomboBox.setModel(new DefaultComboBoxModel(new String[] {"System Specific", "Generic" }));

		transReportFormDropDownPanel = new JPanel();
		transReportFormDropDownPanel.setBackground(SystemColor.control);
		GridBagConstraints gbc_transReportFormDropDownPanel = new GridBagConstraints();
		gbc_transReportFormDropDownPanel.gridheight = 2;
		gbc_transReportFormDropDownPanel.insets = new Insets(0, 20, 5, 5);
		gbc_transReportFormDropDownPanel.fill = GridBagConstraints.BOTH;
		gbc_transReportFormDropDownPanel.gridx = 0;
		gbc_transReportFormDropDownPanel.gridy = 3;
		financialsPanel.add(transReportFormDropDownPanel, gbc_transReportFormDropDownPanel);
		GridBagLayout gbl_transReportFormDropDownPanel = new GridBagLayout();
		gbl_transReportFormDropDownPanel.columnWidths = new int[] { 0, 0 };
		gbl_transReportFormDropDownPanel.rowHeights = new int[] { 0, 0, 0, 0 };
		gbl_transReportFormDropDownPanel.columnWeights = new double[] { 1.0, Double.MIN_VALUE };
		gbl_transReportFormDropDownPanel.rowWeights = new double[] { 0.0, 0.0,	0.0, Double.MIN_VALUE };
		transReportFormDropDownPanel.setLayout(gbl_transReportFormDropDownPanel);

		JLabel lblSelectReportFormat = new JLabel("Select Report Format");
		lblSelectReportFormat.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblSelectReportFormat = new GridBagConstraints();
		gbc_lblSelectReportFormat.insets = new Insets(0, 0, 5, 0);
		gbc_lblSelectReportFormat.gridx = 0;
		gbc_lblSelectReportFormat.gridy = 0;
		transReportFormDropDownPanel.add(lblSelectReportFormat,	gbc_lblSelectReportFormat);

		TransReportFormatcomboBox = new JComboBox();
		TransReportFormatcomboBox.setFont(new Font("Tahoma", Font.PLAIN, 11));
		TransReportFormatcomboBox.setBackground(Color.GRAY);
		GridBagConstraints gbc_TransReportFormatcomboBox = new GridBagConstraints();
		gbc_TransReportFormatcomboBox.fill = GridBagConstraints.HORIZONTAL;
		gbc_TransReportFormatcomboBox.gridx = 0;
		gbc_TransReportFormatcomboBox.gridy = 1;
		transReportFormDropDownPanel.add(TransReportFormatcomboBox, gbc_TransReportFormatcomboBox);
		TransReportFormatcomboBox.setModel(new DefaultComboBoxModel(new String[] { "TAP", "ProSite" }));

		transitionReportGenButton = new CustomButton("Generate Report");
		transitionReportGenButton.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_transitionReportGenButton = new GridBagConstraints();
		gbc_transitionReportGenButton.anchor = GridBagConstraints.NORTHWEST;
		gbc_transitionReportGenButton.insets = new Insets(0, 10, 10, 5);
		gbc_transitionReportGenButton.gridx = 5;
		gbc_transitionReportGenButton.gridy = 6;
		financialsPanel.add(transitionReportGenButton, gbc_transitionReportGenButton);

		separator_3 = new JSeparator();
		GridBagConstraints gbc_separator_3 = new GridBagConstraints();
		gbc_separator_3.fill = GridBagConstraints.HORIZONTAL;
		gbc_separator_3.gridwidth = 6;
		gbc_separator_3.insets = new Insets(0, 0, 5, 5);
		gbc_separator_3.gridx = 0;
		gbc_separator_3.gridy = 7;
		financialsPanel.add(separator_3, gbc_separator_3);

		JLabel lblUpdateCostDB = new JLabel("Update Cost DB");
		lblUpdateCostDB.setFont(new Font("Tahoma", Font.BOLD, 12));
		lblUpdateCostDB.setBackground(Color.WHITE);
		GridBagConstraints gbc_lblUpdateCostDB = new GridBagConstraints();
		gbc_lblUpdateCostDB.anchor = GridBagConstraints.WEST;
		gbc_lblUpdateCostDB.gridwidth = 4;
		gbc_lblUpdateCostDB.insets = new Insets(10, 10, 5, 5);
		gbc_lblUpdateCostDB.gridx = 0;
		gbc_lblUpdateCostDB.gridy = 8;
		financialsPanel.add(lblUpdateCostDB, gbc_lblUpdateCostDB);

		JPanel updateCostDBPanel = new JPanel();
		updateCostDBPanel.setBackground(SystemColor.control);
		GridBagConstraints gbc_updateCostDBPanel = new GridBagConstraints();
		gbc_updateCostDBPanel.anchor = GridBagConstraints.WEST;
		gbc_updateCostDBPanel.gridwidth = 6;
		gbc_updateCostDBPanel.insets = new Insets(0, 0, 5, 5);
		gbc_updateCostDBPanel.gridx = 0;
		gbc_updateCostDBPanel.gridy = 9;
		financialsPanel.add(updateCostDBPanel, gbc_updateCostDBPanel);
		GridBagLayout gbl_updateCostDBPanel = new GridBagLayout();
		gbl_updateCostDBPanel.columnWidths = new int[] { 0, 75, 75, 300 };
		gbl_updateCostDBPanel.rowHeights = new int[] { 0, 0, 0, 0, 0, 0 };
		gbl_updateCostDBPanel.columnWeights = new double[] { 0.0, 0.0, 0.0, 0.0 };
		gbl_updateCostDBPanel.rowWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 };
		updateCostDBPanel.setLayout(gbl_updateCostDBPanel);

		lblChangedDB = new JLabel("Changed DB:");
		GridBagConstraints gbc_lblChangedDB = new GridBagConstraints();
		gbc_lblChangedDB.anchor = GridBagConstraints.WEST;
		gbc_lblChangedDB.insets = new Insets(0, 20, 5, 5);
		gbc_lblChangedDB.gridx = 0;
		gbc_lblChangedDB.gridy = 0;
		updateCostDBPanel.add(lblChangedDB, gbc_lblChangedDB);

		changedDBComboBox = new JComboBox();
		changedDBComboBox.setEditable(false);
		GridBagConstraints gbc_changedDBComboBox = new GridBagConstraints();
		gbc_changedDBComboBox.gridwidth = 2;
		gbc_changedDBComboBox.insets = new Insets(0, 0, 5, 5);
		gbc_changedDBComboBox.fill = GridBagConstraints.HORIZONTAL;
		gbc_changedDBComboBox.gridx = 1;
		gbc_changedDBComboBox.gridy = 0;
		updateCostDBPanel.add(changedDBComboBox, gbc_changedDBComboBox);

		lblCostDBBaseURI = new JLabel("Designate Base URI:");
		lblCostDBBaseURI.setMinimumSize(new Dimension(155, 32));
		lblCostDBBaseURI.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblCostDBBaseURI = new GridBagConstraints();
		gbc_lblCostDBBaseURI.anchor = GridBagConstraints.WEST;
		gbc_lblCostDBBaseURI.insets = new Insets(0, 20, 5, 5);
		gbc_lblCostDBBaseURI.gridx = 0;
		gbc_lblCostDBBaseURI.gridy = 1;
		updateCostDBPanel.add(lblCostDBBaseURI, gbc_lblCostDBBaseURI);

		costDBBaseURIField = new JTextField();
		costDBBaseURIField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		costDBBaseURIField.setText("http://semoss.org/ontologies");
		costDBBaseURIField.setColumns(10);
		GridBagConstraints gbc_costDBBaseURIField = new GridBagConstraints();
		gbc_costDBBaseURIField.gridwidth = 3;
		gbc_costDBBaseURIField.insets = new Insets(0, 0, 5, 0);
		gbc_costDBBaseURIField.fill = GridBagConstraints.HORIZONTAL;
		gbc_costDBBaseURIField.gridx = 1;
		gbc_costDBBaseURIField.gridy = 1;
		updateCostDBPanel.add(costDBBaseURIField, gbc_costDBBaseURIField);

		lblCostDB = new JLabel("Cost DB:");
		GridBagConstraints gbc_lblCostDB = new GridBagConstraints();
		gbc_lblCostDB.anchor = GridBagConstraints.WEST;
		gbc_lblCostDB.insets = new Insets(0, 20, 5, 5);
		gbc_lblCostDB.gridx = 0;
		gbc_lblCostDB.gridy = 2;
		updateCostDBPanel.add(lblCostDB, gbc_lblCostDB);

		costDBComboBox = new JComboBox();
		GridBagConstraints gbc_costDBComboBox = new GridBagConstraints();
		gbc_costDBComboBox.gridwidth = 2;
		gbc_costDBComboBox.insets = new Insets(0, 0, 5, 5);
		gbc_costDBComboBox.fill = GridBagConstraints.HORIZONTAL;
		gbc_costDBComboBox.gridx = 1;
		gbc_costDBComboBox.gridy = 2;
		updateCostDBPanel.add(costDBComboBox, gbc_costDBComboBox);

		btnUpdateCostDB = new CustomButton("Update Cost DB");
		btnUpdateCostDB.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_btnUpdateCostDB = new GridBagConstraints();
		gbc_btnUpdateCostDB.anchor = GridBagConstraints.WEST;
		gbc_btnUpdateCostDB.insets = new Insets(0, 20, 5, 5);
		gbc_btnUpdateCostDB.gridx = 0;
		gbc_btnUpdateCostDB.gridy = 3;
		updateCostDBPanel.add(btnUpdateCostDB, gbc_btnUpdateCostDB);
		Style.registerTargetClassName(btnUpdateCostDB, ".standardButton");

		JPanel tapCalcPanel = new JPanel();
		tapCalcPanel.setBackground(SystemColor.control);
		JScrollPane tapCalcScroll = new JScrollPane(tapCalcPanel);
		tapTabPane.addTab("Additional Calculations", null, tapCalcScroll, null);
		GridBagLayout tapCalcPanelLayout = new GridBagLayout();
		tapCalcPanelLayout.rowHeights = new int[] { 15, 0, 0, 0, 0, 0, 0, 0, 0,	0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
		tapCalcPanelLayout.rowWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0,	0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0 };
		tapCalcPanelLayout.columnWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0 };
		tapCalcPanelLayout.columnWidths = new int[] { 10, 10, 0, 0, 0, 0, 0, 0, 0};
		tapCalcPanel.setLayout(tapCalcPanelLayout);

		JLabel healthGridTitleLabel = new JLabel("Business Value and Technical Maturity Calculations");
		healthGridTitleLabel.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_healthGridTitleLabel = new GridBagConstraints();
		gbc_healthGridTitleLabel.anchor = GridBagConstraints.WEST;
		gbc_healthGridTitleLabel.gridwidth = 4;
		gbc_healthGridTitleLabel.insets = new Insets(0, 0, 5, 5);
		gbc_healthGridTitleLabel.gridx = 1;
		gbc_healthGridTitleLabel.gridy = 1;
		tapCalcPanel.add(healthGridTitleLabel, gbc_healthGridTitleLabel);

		JLabel distBVtechMatlabel_1 = new JLabel("Sys-IO Distance Downstream, Business Value and Technical Maturity Insert");
		distBVtechMatlabel_1.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_distBVtechMatlabel_1 = new GridBagConstraints();
		gbc_distBVtechMatlabel_1.anchor = GridBagConstraints.WEST;
		gbc_distBVtechMatlabel_1.gridwidth = 5;
		gbc_distBVtechMatlabel_1.insets = new Insets(10, 0, 5, 5);
		gbc_distBVtechMatlabel_1.gridx = 2;
		gbc_distBVtechMatlabel_1.gridy = 2;
		tapCalcPanel.add(distBVtechMatlabel_1, gbc_distBVtechMatlabel_1);

		JLabel lblAppreciationValue = new JLabel("Appreciation Value:");
		lblAppreciationValue.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblAppreciationValue = new GridBagConstraints();
		gbc_lblAppreciationValue.anchor = GridBagConstraints.WEST;
		gbc_lblAppreciationValue.insets = new Insets(0, 0, 5, 5);
		gbc_lblAppreciationValue.gridx = 2;
		gbc_lblAppreciationValue.gridy = 3;
		tapCalcPanel.add(lblAppreciationValue, gbc_lblAppreciationValue);

		JLabel lblDepreciationValue = new JLabel("Depreciation Value:");
		lblDepreciationValue.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblDepreciationValue = new GridBagConstraints();
		gbc_lblDepreciationValue.anchor = GridBagConstraints.WEST;
		gbc_lblDepreciationValue.insets = new Insets(0, 10, 5, 5);
		gbc_lblDepreciationValue.gridx = 3;
		gbc_lblDepreciationValue.gridy = 3;
		tapCalcPanel.add(lblDepreciationValue, gbc_lblDepreciationValue);

		JLabel lblSoaAlphaValue = new JLabel("SOA Alpha Value:");
		lblSoaAlphaValue.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblSoaAlphaValue = new GridBagConstraints();
		gbc_lblSoaAlphaValue.insets = new Insets(0, 10, 5, 5);
		gbc_lblSoaAlphaValue.gridx = 4;
		gbc_lblSoaAlphaValue.gridy = 3;
		tapCalcPanel.add(lblSoaAlphaValue, gbc_lblSoaAlphaValue);

		appreciationValueTextField = new JTextField();
		appreciationValueTextField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		appreciationValueTextField.setMinimumSize(new Dimension(122, 28));
		appreciationValueTextField.setSize(new Dimension(10, 28));
		appreciationValueTextField.setText("0.8");
		appreciationValueTextField.setMaximumSize(new Dimension(15, 2147483647));
		GridBagConstraints gbc_appreciationValueTextField = new GridBagConstraints();
		gbc_appreciationValueTextField.anchor = GridBagConstraints.WEST;
		gbc_appreciationValueTextField.insets = new Insets(0, 0, 5, 5);
		gbc_appreciationValueTextField.gridx = 2;
		gbc_appreciationValueTextField.gridy = 4;
		tapCalcPanel.add(appreciationValueTextField, gbc_appreciationValueTextField);
		appreciationValueTextField.setColumns(12);

		depreciationValueTextField = new JTextField();
		depreciationValueTextField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		depreciationValueTextField.setMinimumSize(new Dimension(122, 28));
		depreciationValueTextField.setSize(new Dimension(10, 28));
		depreciationValueTextField.setText("0.8");
		GridBagConstraints gbc_depreciationValueTextField = new GridBagConstraints();
		gbc_depreciationValueTextField.anchor = GridBagConstraints.WEST;
		gbc_depreciationValueTextField.insets = new Insets(0, 10, 5, 5);
		gbc_depreciationValueTextField.gridx = 3;
		gbc_depreciationValueTextField.gridy = 4;
		tapCalcPanel.add(depreciationValueTextField, gbc_depreciationValueTextField);
		depreciationValueTextField.setColumns(12);

		soaAlphaValueTextField = new JTextField();
		soaAlphaValueTextField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		soaAlphaValueTextField.setMinimumSize(new Dimension(122, 28));
		soaAlphaValueTextField.setText("0.61");
		GridBagConstraints gbc_soaAlphaValueTextField = new GridBagConstraints();
		gbc_soaAlphaValueTextField.insets = new Insets(0, 10, 5, 5);
		gbc_soaAlphaValueTextField.gridx = 4;
		gbc_soaAlphaValueTextField.gridy = 4;
		tapCalcPanel.add(soaAlphaValueTextField, gbc_soaAlphaValueTextField);
		soaAlphaValueTextField.setColumns(10);

		btnInsertDownstream = new CustomButton("Run Calculations and Insert");
		btnInsertDownstream.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_btnInsertDownstream = new GridBagConstraints();
		gbc_btnInsertDownstream.fill = GridBagConstraints.HORIZONTAL;
		gbc_btnInsertDownstream.gridwidth = 2;
		gbc_btnInsertDownstream.insets = new Insets(0, 0, 5, 5);
		gbc_btnInsertDownstream.gridx = 2;
		gbc_btnInsertDownstream.gridy = 5;
		tapCalcPanel.add(btnInsertDownstream, gbc_btnInsertDownstream);
		Style.registerTargetClassName(btnInsertDownstream, ".standardButton");

		btnRunBvAlone = new CustomButton("Run BV Alone");
		btnRunBvAlone.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_btnRunBvAlone = new GridBagConstraints();
		gbc_btnRunBvAlone.anchor = GridBagConstraints.WEST;
		gbc_btnRunBvAlone.insets = new Insets(0, 0, 5, 5);
		gbc_btnRunBvAlone.gridx = 2;
		gbc_btnRunBvAlone.gridy = 6;
		tapCalcPanel.add(btnRunBvAlone, gbc_btnRunBvAlone);
		Style.registerTargetClassName(btnRunBvAlone, ".standardButton");

		btnRunTmAlone = new CustomButton("Run TM Alone");
		btnRunTmAlone.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_btnRunTmAlone = new GridBagConstraints();
		gbc_btnRunTmAlone.anchor = GridBagConstraints.EAST;
		gbc_btnRunTmAlone.insets = new Insets(0, 0, 5, 5);
		gbc_btnRunTmAlone.gridx = 3;
		gbc_btnRunTmAlone.gridy = 6;
		tapCalcPanel.add(btnRunTmAlone, gbc_btnRunTmAlone);
		Style.registerTargetClassName(btnRunTmAlone, ".standardButton");

		JLabel lblSystemSustainmentBudget = new JLabel("System Sustainment Budget Property Insert");
		lblSystemSustainmentBudget.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblSystemSustainmentBudget = new GridBagConstraints();
		gbc_lblSystemSustainmentBudget.anchor = GridBagConstraints.WEST;
		gbc_lblSystemSustainmentBudget.gridwidth = 3;
		gbc_lblSystemSustainmentBudget.insets = new Insets(10, 0, 5, 5);
		gbc_lblSystemSustainmentBudget.gridx = 2;
		gbc_lblSystemSustainmentBudget.gridy = 7;
		tapCalcPanel.add(lblSystemSustainmentBudget, gbc_lblSystemSustainmentBudget);

		btnInsertBudgetProperty = new CustomButton("Insert Budget Property");
		btnInsertBudgetProperty.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_btnInsertBudgetProperty = new GridBagConstraints();
		gbc_btnInsertBudgetProperty.anchor = GridBagConstraints.WEST;
		gbc_btnInsertBudgetProperty.gridwidth = 3;
		gbc_btnInsertBudgetProperty.insets = new Insets(0, 0, 5, 5);
		gbc_btnInsertBudgetProperty.gridx = 2;
		gbc_btnInsertBudgetProperty.gridy = 8;
		tapCalcPanel.add(btnInsertBudgetProperty, gbc_btnInsertBudgetProperty);

		JLabel lblServiceBusinessValue = new JLabel("Service Business Value and ICD Property Insert");
		lblServiceBusinessValue.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblServiceBusinessValue = new GridBagConstraints();
		gbc_lblServiceBusinessValue.anchor = GridBagConstraints.WEST;
		gbc_lblServiceBusinessValue.gridwidth = 5;
		gbc_lblServiceBusinessValue.insets = new Insets(10, 0, 5, 5);
		gbc_lblServiceBusinessValue.gridx = 2;
		gbc_lblServiceBusinessValue.gridy = 9;
		tapCalcPanel.add(lblServiceBusinessValue, gbc_lblServiceBusinessValue);

		btnInsertServiceProperties = new CustomButton("Run Calculations and Insert");
		btnInsertServiceProperties.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_btnInsertServiceProperties = new GridBagConstraints();
		gbc_btnInsertServiceProperties.anchor = GridBagConstraints.NORTHWEST;
		gbc_btnInsertServiceProperties.gridwidth = 3;
		gbc_btnInsertServiceProperties.insets = new Insets(0, 0, 5, 5);
		gbc_btnInsertServiceProperties.gridx = 2;
		gbc_btnInsertServiceProperties.gridy = 10;
		tapCalcPanel.add(btnInsertServiceProperties, gbc_btnInsertServiceProperties);

		JLabel lblCapabilityBV = new JLabel("Capability Business Value Insert");
		lblCapabilityBV.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblCapabilityBV = new GridBagConstraints();
		gbc_lblCapabilityBV.anchor = GridBagConstraints.WEST;
		gbc_lblCapabilityBV.gridwidth = 3;
		gbc_lblCapabilityBV.insets = new Insets(10, 0, 5, 5);
		gbc_lblCapabilityBV.gridx = 2;
		gbc_lblCapabilityBV.gridy = 11;
		tapCalcPanel.add(lblCapabilityBV, gbc_lblCapabilityBV);

		btnRunCapabilityBV = new CustomButton("Insert Capability BV");
		btnRunCapabilityBV.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_btnRunCapabilityBV = new GridBagConstraints();
		gbc_btnRunCapabilityBV.anchor = GridBagConstraints.NORTHWEST;
		gbc_btnRunCapabilityBV.gridwidth = 3;
		gbc_btnRunCapabilityBV.insets = new Insets(0, 0, 5, 5);
		gbc_btnRunCapabilityBV.gridx = 2;
		gbc_btnRunCapabilityBV.gridy = 12;
		tapCalcPanel.add(btnRunCapabilityBV, gbc_btnRunCapabilityBV);
		Style.registerTargetClassName(btnRunCapabilityBV, ".standardButton");
		
		separator = new JSeparator();
		GridBagConstraints gbc_separator = new GridBagConstraints();
		gbc_separator.fill = GridBagConstraints.HORIZONTAL;
		gbc_separator.gridwidth = 6;
		gbc_separator.insets = new Insets(5, 0, 5, 5);
		gbc_separator.gridx = 0;
		gbc_separator.gridy = 13;
		tapCalcPanel.add(separator, gbc_separator);
		
		JLabel lblUpdateActiveSystem = new JLabel("Update TAP Core Active Systems");
		lblUpdateActiveSystem.setFont(new Font("Tahoma", Font.BOLD, 12));
		lblUpdateActiveSystem.setBackground(Color.WHITE);
		GridBagConstraints gbc_lblUpdateActiveSystem = new GridBagConstraints();
		gbc_lblUpdateActiveSystem.anchor = GridBagConstraints.WEST;
		gbc_lblUpdateActiveSystem.gridwidth = 4;
		gbc_lblUpdateActiveSystem.insets = new Insets(10, 10, 5, 5);
		gbc_lblUpdateActiveSystem.gridx = 0;
		gbc_lblUpdateActiveSystem.gridy = 14;
		tapCalcPanel.add(lblUpdateActiveSystem, gbc_lblUpdateActiveSystem);
		
		btnUpdateActiveSystems = new CustomButton("Update TAP Core Active Systems");
		btnUpdateActiveSystems.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_btnUpdateActiveSystems = new GridBagConstraints();
		gbc_btnUpdateActiveSystems.anchor = GridBagConstraints.NORTHWEST;
		gbc_btnUpdateActiveSystems.gridwidth = 4;
		gbc_btnUpdateActiveSystems.insets = new Insets(10, 10, 10, 5);
		gbc_btnUpdateActiveSystems.gridx = 1;
		gbc_btnUpdateActiveSystems.gridy = 15;
		tapCalcPanel.add(btnUpdateActiveSystems, gbc_btnUpdateActiveSystems);
		Style.registerTargetClassName(btnUpdateActiveSystems, ".standardButton");
		
		JSeparator separateActiveSystems_AggregateTapServiceIntoTapCore = new JSeparator();
		GridBagConstraints gbc_separateActiveSystems_AggregateTapServiceIntoTapCore = new GridBagConstraints();
		gbc_separateActiveSystems_AggregateTapServiceIntoTapCore.fill = GridBagConstraints.HORIZONTAL;
		gbc_separateActiveSystems_AggregateTapServiceIntoTapCore.gridwidth = 6;
		gbc_separateActiveSystems_AggregateTapServiceIntoTapCore.insets = new Insets(5, 0, 5, 5);
		gbc_separateActiveSystems_AggregateTapServiceIntoTapCore.gridx = 0;
		gbc_separateActiveSystems_AggregateTapServiceIntoTapCore.gridy = 16;
		tapCalcPanel.add(separateActiveSystems_AggregateTapServiceIntoTapCore, gbc_separateActiveSystems_AggregateTapServiceIntoTapCore);
		
		JPanel aggregateTapServicesIntoTapCorePanel = new JPanel();
		aggregateTapServicesIntoTapCorePanel.setBackground(SystemColor.control);
		GridBagConstraints gbc_aggregateTapServicesIntoTapCorePanel = new GridBagConstraints();
		gbc_aggregateTapServicesIntoTapCorePanel.anchor = GridBagConstraints.WEST;
		gbc_aggregateTapServicesIntoTapCorePanel.gridwidth = 6;
		gbc_aggregateTapServicesIntoTapCorePanel.insets = new Insets(0, 0, 5, 5);
		gbc_aggregateTapServicesIntoTapCorePanel.gridx = 0;
		gbc_aggregateTapServicesIntoTapCorePanel.gridy = 17;
		tapCalcPanel.add(aggregateTapServicesIntoTapCorePanel, gbc_aggregateTapServicesIntoTapCorePanel);
		GridBagLayout gbl_aggregateTapServicesIntoTapCorePanel = new GridBagLayout();
		gbl_aggregateTapServicesIntoTapCorePanel.columnWidths = new int[] { 0, 75, 100, 75 };
		gbl_aggregateTapServicesIntoTapCorePanel.rowHeights = new int[] { 0, 0, 0, 0, 0 };
		gbl_aggregateTapServicesIntoTapCorePanel.columnWeights = new double[] { 0.0, 0.0, 0.0, 0.0 };
		gbl_aggregateTapServicesIntoTapCorePanel.rowWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0 };
		aggregateTapServicesIntoTapCorePanel.setLayout(gbl_aggregateTapServicesIntoTapCorePanel);

		JLabel lblAggregateTapServiceIntoTapCore = new JLabel("Aggregate TAP Services into TAP Core");
		lblAggregateTapServiceIntoTapCore.setFont(new Font("Tahoma", Font.BOLD, 12));
		lblAggregateTapServiceIntoTapCore.setBackground(Color.WHITE);
		GridBagConstraints gbc_lblAggregateTapServiceIntoTapCore = new GridBagConstraints();
		gbc_lblAggregateTapServiceIntoTapCore.anchor = GridBagConstraints.WEST;
		gbc_lblAggregateTapServiceIntoTapCore.gridwidth = 3;
		gbc_lblAggregateTapServiceIntoTapCore.insets = new Insets(10, 10, 5, 5);
		gbc_lblAggregateTapServiceIntoTapCore.gridx = 0;
		gbc_lblAggregateTapServiceIntoTapCore.gridy = 0;
		aggregateTapServicesIntoTapCorePanel.add(lblAggregateTapServiceIntoTapCore, gbc_lblAggregateTapServiceIntoTapCore);
		
		JLabel lblSelectTapServicesToInsertIntoTapCore = new JLabel("Select TAP Services Database:");
		GridBagConstraints gbc_lblSelectTapServicesToInsertIntoTapCore = new GridBagConstraints();
		gbc_lblSelectTapServicesToInsertIntoTapCore.anchor = GridBagConstraints.WEST;
		gbc_lblSelectTapServicesToInsertIntoTapCore.insets = new Insets(0, 20, 5, 5);
		gbc_lblSelectTapServicesToInsertIntoTapCore.gridx = 1;
		gbc_lblSelectTapServicesToInsertIntoTapCore.gridy = 1;
		aggregateTapServicesIntoTapCorePanel.add(lblSelectTapServicesToInsertIntoTapCore, gbc_lblSelectTapServicesToInsertIntoTapCore);

		selectTapServicesComboBox = new JComboBox<String>();
		selectTapServicesComboBox.setEditable(false);
		GridBagConstraints gbc_selectTapServicesComboBox = new GridBagConstraints();
		gbc_selectTapServicesComboBox.gridwidth = 2;
		gbc_selectTapServicesComboBox.insets = new Insets(0, 0, 5, 5);
		gbc_selectTapServicesComboBox.fill = GridBagConstraints.HORIZONTAL;
		gbc_selectTapServicesComboBox.gridx = 2;
		gbc_selectTapServicesComboBox.gridy = 1;
		aggregateTapServicesIntoTapCorePanel.add(selectTapServicesComboBox, gbc_selectTapServicesComboBox);
		
		JLabel lblSelectTapCoreToInsertTapServices = new JLabel("Select TAP Core Database:");
		GridBagConstraints gbc_lblSelectTapCoreToInsertTapServices = new GridBagConstraints();
		gbc_lblSelectTapCoreToInsertTapServices.anchor = GridBagConstraints.WEST;
		gbc_lblSelectTapCoreToInsertTapServices.insets = new Insets(0, 20, 5, 5);
		gbc_lblSelectTapCoreToInsertTapServices.gridx = 1;
		gbc_lblSelectTapCoreToInsertTapServices.gridy = 2;
		aggregateTapServicesIntoTapCorePanel.add(lblSelectTapCoreToInsertTapServices, gbc_lblSelectTapCoreToInsertTapServices);
		
		selectTapCoreComboBox = new JComboBox<String>();
		selectTapCoreComboBox.setEditable(false);
		GridBagConstraints gbc_selectTapCoreComboBox = new GridBagConstraints();
		gbc_selectTapCoreComboBox.gridwidth = 2;
		gbc_selectTapCoreComboBox.insets = new Insets(0, 0, 5, 5);
		gbc_selectTapCoreComboBox.fill = GridBagConstraints.HORIZONTAL;
		gbc_selectTapCoreComboBox.gridx = 2;
		gbc_selectTapCoreComboBox.gridy = 2;
		aggregateTapServicesIntoTapCorePanel.add(selectTapCoreComboBox, gbc_selectTapCoreComboBox);

		btnAggregateTapServicesIntoTapCore = new CustomButton("Aggregate TAP Services into TAP Core");
		btnAggregateTapServicesIntoTapCore.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_btnAggregateTapServicesIntoTapCore = new GridBagConstraints();
		gbc_btnAggregateTapServicesIntoTapCore.anchor = GridBagConstraints.NORTHWEST;
		gbc_btnAggregateTapServicesIntoTapCore.gridwidth = 4;
		gbc_btnAggregateTapServicesIntoTapCore.insets = new Insets(10, 20, 10, 0);
		gbc_btnAggregateTapServicesIntoTapCore.gridx = 1;
		gbc_btnAggregateTapServicesIntoTapCore.gridy = 3;
		aggregateTapServicesIntoTapCorePanel.add(btnAggregateTapServicesIntoTapCore, gbc_btnAggregateTapServicesIntoTapCore);
		Style.registerTargetClassName(btnAggregateTapServicesIntoTapCore, ".standardButton");
		
		JPanel tapReportPanel = new JPanel();
		tapReportPanel.setBackground(SystemColor.control);
		JScrollPane tapReportScroll = new JScrollPane(tapReportPanel);
		tapTabPane.addTab("Report Generator", null, tapReportScroll, null);
		GridBagLayout tapReportPanelLayout = new GridBagLayout();
		tapReportPanelLayout.rowHeights = new int[] { 30, 0, 0, 0, 0 };
		tapReportPanelLayout.rowWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 1.0 };
		tapReportPanelLayout.columnWeights = new double[] { 0.0, 1.0, 0.0 };
		tapReportPanelLayout.columnWidths = new int[] { 0, 0, 0 };
		tapReportPanel.setLayout(tapReportPanelLayout);

		tapReportTopPanel = new JPanel();
		tapReportTopPanel.setBackground(SystemColor.control);
		GridBagConstraints gbc_tapReportTopPanel = new GridBagConstraints();
		gbc_tapReportTopPanel.fill = GridBagConstraints.HORIZONTAL;
		gbc_tapReportTopPanel.insets = new Insets(0, 0, 5, 5);
		gbc_tapReportTopPanel.anchor = GridBagConstraints.NORTH;
		gbc_tapReportTopPanel.gridx = 0;
		gbc_tapReportTopPanel.gridy = 0;
		tapReportPanel.add(tapReportTopPanel, gbc_tapReportTopPanel);
		GridBagLayout gbl_tapReportTopPanel = new GridBagLayout();
		gbl_tapReportTopPanel.columnWidths = new int[] { 10, 10, 10, 0, 0 };
		gbl_tapReportTopPanel.rowHeights = new int[] { 10, 35, 0, 0, 0, 0, 0, 0, 0, 0 };
		gbl_tapReportTopPanel.columnWeights = new double[] { 0.0, 0.0, 0.0,	0.0, 1.0 };
		gbl_tapReportTopPanel.rowWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE };
		tapReportTopPanel.setLayout(gbl_tapReportTopPanel);

		JLabel tapReportTitleLabel = new JLabel("Generate Vendor Input Report");
		GridBagConstraints gbc_tapReportTitleLabel = new GridBagConstraints();
		gbc_tapReportTitleLabel.gridwidth = 2;
		gbc_tapReportTitleLabel.anchor = GridBagConstraints.WEST;
		gbc_tapReportTitleLabel.insets = new Insets(0, 0, 5, 5);
		gbc_tapReportTitleLabel.gridx = 1;
		gbc_tapReportTitleLabel.gridy = 1;
		tapReportTopPanel.add(tapReportTitleLabel, gbc_tapReportTitleLabel);
		tapReportTitleLabel.setFont(new Font("Tahoma", Font.BOLD, 12));

		JLabel lblRFPName = new JLabel("Designate RFP Identifier:");
		lblRFPName.setMinimumSize(new Dimension(155, 32));
		lblRFPName.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblRFPName = new GridBagConstraints();
		gbc_lblRFPName.anchor = GridBagConstraints.WEST;
		gbc_lblRFPName.insets = new Insets(0, 0, 5, 5);
		gbc_lblRFPName.gridx = 2;
		gbc_lblRFPName.gridy = 2;
		tapReportTopPanel.add(lblRFPName, gbc_lblRFPName);

		RFPNameField = new JTextField();
		RFPNameField.setFont(new Font("Tahoma", Font.PLAIN, 11));
		RFPNameField.setText("RFP1");
		RFPNameField.setColumns(10);
		GridBagConstraints gbc_RFPNameField = new GridBagConstraints();
		gbc_RFPNameField.fill = GridBagConstraints.HORIZONTAL;
		gbc_RFPNameField.insets = new Insets(0, 0, 5, 5);
		gbc_RFPNameField.gridx = 2;
		gbc_RFPNameField.gridy = 3;
		tapReportTopPanel.add(RFPNameField, gbc_RFPNameField);

		functionalAreaPanel = new Panel();
		GridBagConstraints gbc_functionalAreaPanel = new GridBagConstraints();
		gbc_functionalAreaPanel.anchor = GridBagConstraints.WEST;
		gbc_functionalAreaPanel.gridx = 2;
		gbc_functionalAreaPanel.gridy = 4;
		tapReportTopPanel.add(functionalAreaPanel, gbc_functionalAreaPanel);
		functionalAreaPanel.setLayout(new FlowLayout(FlowLayout.CENTER, 5, 5));

		HSDCheckBox = new JCheckBox("HSD");
		functionalAreaPanel.add(HSDCheckBox);

		HSSCheckBox = new JCheckBox("HSS");
		functionalAreaPanel.add(HSSCheckBox);

		FHPCheckBox = new JCheckBox("FHP");
		functionalAreaPanel.add(FHPCheckBox);
		
		DHMSMCheckBox = new JCheckBox("DHMSM");
		functionalAreaPanel.add(DHMSMCheckBox);

		JLabel lblCapabilityName = new JLabel("Capabilities:");
		lblCapabilityName.setMinimumSize(new Dimension(155, 32));
		lblCapabilityName.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblCapabilityName = new GridBagConstraints();
		gbc_lblCapabilityName.anchor = GridBagConstraints.WEST;
		gbc_lblCapabilityName.insets = new Insets(0, 0, 5, 5);
		gbc_lblCapabilityName.gridx = 2;
		gbc_lblCapabilityName.gridy = 5;
		tapReportTopPanel.add(lblCapabilityName, gbc_lblCapabilityName);

		sourceSelectPanel = new SourceSelectPanel();// change this
		FlowLayout flowLayout = (FlowLayout) sourceSelectPanel.getLayout();
		flowLayout.setAlignment(FlowLayout.LEFT);
		sourceSelectPanel.setBackground(SystemColor.control);

		sourceSelectScrollPane = new JScrollPane(sourceSelectPanel);
		GridBagConstraints gbc_sourceSelectScrollPane = new GridBagConstraints();
		gbc_sourceSelectScrollPane.fill = GridBagConstraints.HORIZONTAL;
		gbc_sourceSelectScrollPane.insets = new Insets(0, 0, 5, 5);
		gbc_sourceSelectScrollPane.gridx = 2;
		gbc_sourceSelectScrollPane.gridy = 6;
		tapReportTopPanel.add(sourceSelectScrollPane, gbc_sourceSelectScrollPane);
		sourceSelectScrollPane.setPreferredSize(new Dimension(300, 300));

		sourceReportGenButton = new CustomButton("Generate Vendor Input Report");
		GridBagConstraints gbc_sourceReportGenButton = new GridBagConstraints();
		gbc_sourceReportGenButton.anchor = GridBagConstraints.WEST;
		gbc_sourceReportGenButton.insets = new Insets(0, 0, 5, 5);
		gbc_sourceReportGenButton.gridx = 2;
		gbc_sourceReportGenButton.gridy = 7;
		tapReportTopPanel.add(sourceReportGenButton, gbc_sourceReportGenButton);
		sourceReportGenButton.setFont(new Font("Tahoma", Font.BOLD, 11));
		Style.registerTargetClassName(sourceReportGenButton, ".standardButton");

		separator_6 = new JSeparator();
		GridBagConstraints gbc_separator_6 = new GridBagConstraints();
		gbc_separator_6.fill = GridBagConstraints.HORIZONTAL;
		gbc_separator_6.gridwidth = 1;
		gbc_separator_6.insets = new Insets(0, 0, 5, 5);
		gbc_separator_6.gridx = 0;
		gbc_separator_6.gridy = 1;
		tapReportPanel.add(separator_6, gbc_separator_6);

		JPanel updateTaskWeightPanel = new JPanel();
		updateTaskWeightPanel.setBackground(SystemColor.control);
		GridBagConstraints gbc_updateTaskWeightPanel = new GridBagConstraints();
		gbc_updateTaskWeightPanel.fill = GridBagConstraints.HORIZONTAL;
		gbc_updateTaskWeightPanel.insets = new Insets(0, 0, 5, 5);
		gbc_updateTaskWeightPanel.gridx = 0;
		gbc_updateTaskWeightPanel.gridy = 2;
		tapReportPanel.add(updateTaskWeightPanel, gbc_updateTaskWeightPanel);
		GridBagLayout gbl_updateTaskWeightPanel = new GridBagLayout();
		gbl_updateTaskWeightPanel.columnWidths = new int[] { 10, 10, 0, 0, 75, 75, 300 };
		gbl_updateTaskWeightPanel.rowHeights = new int[] { 0, 0, 0, 0, 0 };
		gbl_updateTaskWeightPanel.columnWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0 };
		gbl_updateTaskWeightPanel.rowWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0 };
		updateTaskWeightPanel.setLayout(gbl_updateTaskWeightPanel);

		JLabel updateTaskWeightTitleLabel = new JLabel("Calculate Vendor Selection Scores");
		GridBagConstraints gbc_updateTaskWeightTitleLabel = new GridBagConstraints();
		gbc_updateTaskWeightTitleLabel.gridwidth = 3;
		gbc_updateTaskWeightTitleLabel.fill = GridBagConstraints.BOTH;
		gbc_updateTaskWeightTitleLabel.insets = new Insets(5, 0, 10, 5);
		gbc_updateTaskWeightTitleLabel.gridx = 1;
		gbc_updateTaskWeightTitleLabel.gridy = 0;
		updateTaskWeightPanel.add(updateTaskWeightTitleLabel, gbc_updateTaskWeightTitleLabel);
		updateTaskWeightTitleLabel.setFont(new Font("Tahoma", Font.BOLD, 12));

		btnUpdateVendorDB = new CustomButton("Calculate Business and Tech Standard Fulfillment");
		btnUpdateVendorDB.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_btnUpdateVendorDB = new GridBagConstraints();
		gbc_btnUpdateVendorDB.fill = GridBagConstraints.HORIZONTAL;
		gbc_btnUpdateVendorDB.gridwidth = 2;
		gbc_btnUpdateVendorDB.insets = new Insets(0, 0, 5, 5);
		gbc_btnUpdateVendorDB.gridx = 2;
		gbc_btnUpdateVendorDB.gridy = 1;
		updateTaskWeightPanel.add(btnUpdateVendorDB, gbc_btnUpdateVendorDB);
		btnUpdateVendorDB.setFont(new Font("Tahoma", Font.BOLD, 11));
		Style.registerTargetClassName(btnUpdateVendorDB, ".standardButton");
		
		btnCalculateVendorTMAlone = new CustomButton("Calculate External Stability");
		btnCalculateVendorTMAlone.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_btnCalculateVendorTMAlone = new GridBagConstraints();
		gbc_btnCalculateVendorTMAlone.gridwidth = 2;
		gbc_btnCalculateVendorTMAlone.insets = new Insets(0, 0, 5, 5);
		gbc_btnCalculateVendorTMAlone.gridx = 2;
		gbc_btnCalculateVendorTMAlone.gridy = 2;
		gbc_btnCalculateVendorTMAlone.fill = GridBagConstraints.HORIZONTAL;
		updateTaskWeightPanel.add(btnCalculateVendorTMAlone,gbc_btnCalculateVendorTMAlone);
		Style.registerTargetClassName(btnCalculateVendorTMAlone,".standardButton");

		JPanel deconflictingPanel = new JPanel();
		deconflictingPanel.setBackground(SystemColor.control);
		GridBagConstraints gbc_deconflictingPanel = new GridBagConstraints();
		gbc_deconflictingPanel.fill = GridBagConstraints.HORIZONTAL;
		gbc_deconflictingPanel.insets = new Insets(0, 0, 5, 5);
		gbc_deconflictingPanel.gridx = 1;
		gbc_deconflictingPanel.gridy = 2;
		tapReportPanel.add(deconflictingPanel, gbc_deconflictingPanel);
		GridBagLayout gbl_deconflictingPanel = new GridBagLayout();
		gbl_deconflictingPanel.columnWidths = new int[]{10, 10, 0, 0, 75, 75, 300};
		gbl_deconflictingPanel.rowHeights = new int[]{0, 0, 0, 0, 0};
		gbl_deconflictingPanel.columnWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0};
		gbl_deconflictingPanel.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0};
		deconflictingPanel.setLayout(gbl_deconflictingPanel);

		JLabel deconflictingTitleLabel = new JLabel("Generate Deconflicting and Missing ICD/Data Report");
		GridBagConstraints gbc_deconflictingTitleLabel = new GridBagConstraints();
		gbc_deconflictingTitleLabel.gridwidth = 3;
		gbc_deconflictingTitleLabel.fill = GridBagConstraints.BOTH;
		gbc_deconflictingTitleLabel.insets = new Insets(5, 0, 10, 0);
		gbc_deconflictingTitleLabel.gridx = 5;
		gbc_deconflictingTitleLabel.gridy = 0;
		updateTaskWeightPanel.add(deconflictingTitleLabel,	gbc_deconflictingTitleLabel);
		deconflictingTitleLabel.setFont(new Font("Tahoma", Font.BOLD, 12));

		btnDeconflictingReport = new CustomButton("Generate Report");
		btnDeconflictingReport.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_btnDeconflictingReport = new GridBagConstraints();
		gbc_btnDeconflictingReport.fill = GridBagConstraints.HORIZONTAL;
		gbc_btnDeconflictingReport.gridwidth = 2;
		gbc_btnDeconflictingReport.insets = new Insets(0, 0, 5, 5);
		gbc_btnDeconflictingReport.gridx = 5;
		gbc_btnDeconflictingReport.gridy = 1;
		updateTaskWeightPanel.add(btnDeconflictingReport,gbc_btnDeconflictingReport);
		Style.registerTargetClassName(btnDeconflictingReport, ".standardButton");

		separator_7 = new JSeparator();
		GridBagConstraints gbc_separator_7 = new GridBagConstraints();
		gbc_separator_7.fill = GridBagConstraints.HORIZONTAL;
		gbc_separator_7.gridwidth = 1;
		gbc_separator_7.insets = new Insets(5, 5, 5, 5);
		gbc_separator_7.gridx = 0;
		gbc_separator_7.gridy = 3;
		tapReportPanel.add(separator_7, gbc_separator_7);

		JPanel FactSheetPanel = new JPanel();
		FactSheetPanel.setBackground(SystemColor.control);
		GridBagConstraints gbc_FactSheetPanel = new GridBagConstraints();
		gbc_FactSheetPanel.fill = GridBagConstraints.HORIZONTAL;
		gbc_FactSheetPanel.insets = new Insets(0, 0, 5, 5);
		gbc_FactSheetPanel.gridx = 0;
		gbc_FactSheetPanel.gridy = 4;
		tapReportPanel.add(FactSheetPanel, gbc_FactSheetPanel);
		GridBagLayout gbl_FactSheetPanel = new GridBagLayout();
		gbl_FactSheetPanel.columnWidths = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
		gbl_FactSheetPanel.rowHeights = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
		gbl_FactSheetPanel.columnWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE };
		gbl_FactSheetPanel.rowWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0,	0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,	0.0, 0.0, Double.MIN_VALUE };
		FactSheetPanel.setLayout(gbl_FactSheetPanel);

		JLabel FactSheetTitleLabel = new JLabel("Services Fact Sheet Reports");
		FactSheetTitleLabel.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_FactSheetTitleLabel = new GridBagConstraints();
		gbc_FactSheetTitleLabel.gridwidth = 3;
		gbc_FactSheetTitleLabel.fill = GridBagConstraints.BOTH;
		gbc_FactSheetTitleLabel.insets = new Insets(10, 10, 5, 5);
		gbc_FactSheetTitleLabel.gridx = 1;
		gbc_FactSheetTitleLabel.gridy = 0;
		FactSheetPanel.add(FactSheetTitleLabel, gbc_FactSheetTitleLabel);

		JLabel lblFactSheetGenerator = new JLabel("Fact Sheet Generator:");
		lblFactSheetGenerator.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblFactSheetGenerator = new GridBagConstraints();
		gbc_lblFactSheetGenerator.gridwidth = 6;
		gbc_lblFactSheetGenerator.anchor = GridBagConstraints.NORTHWEST;
		gbc_lblFactSheetGenerator.insets = new Insets(5, 20, 10, 5);
		gbc_lblFactSheetGenerator.gridx = 1;
		gbc_lblFactSheetGenerator.gridy = 1;
		FactSheetPanel.add(lblFactSheetGenerator, gbc_lblFactSheetGenerator);

		factSheetReportSysDropDownPanel = new JPanel();
		factSheetReportSysDropDownPanel.setBackground(SystemColor.control);
		GridBagConstraints gbc_factSheetReportSysDropDownPanel = new GridBagConstraints();
		gbc_factSheetReportSysDropDownPanel.gridheight = 2;
		gbc_factSheetReportSysDropDownPanel.insets = new Insets(0, 10, 5, 5);
		gbc_factSheetReportSysDropDownPanel.fill = GridBagConstraints.BOTH;
		gbc_factSheetReportSysDropDownPanel.gridx = 4;
		gbc_factSheetReportSysDropDownPanel.gridy = 3;
		FactSheetPanel.add(factSheetReportSysDropDownPanel,	gbc_factSheetReportSysDropDownPanel);
		GridBagLayout gbl_factSheetReportSysDropDownPanel = new GridBagLayout();
		gbl_factSheetReportSysDropDownPanel.columnWidths = new int[] { 0, 0 };
		gbl_factSheetReportSysDropDownPanel.rowHeights = new int[] { 0, 0, 0 };
		gbl_factSheetReportSysDropDownPanel.columnWeights = new double[] { 1.0,Double.MIN_VALUE };
		gbl_factSheetReportSysDropDownPanel.rowWeights = new double[] { 0.0,0.0, Double.MIN_VALUE };
		factSheetReportSysDropDownPanel.setLayout(gbl_factSheetReportSysDropDownPanel);
		factSheetReportSysDropDownPanel.setVisible(false);

		JLabel lblFactSheetSelectSystem = new JLabel("Select System");
		lblSelectSystem.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblFactSheetSelectSystem = new GridBagConstraints();
		gbc_lblFactSheetSelectSystem.insets = new Insets(0, 0, 5, 0);
		gbc_lblFactSheetSelectSystem.gridx = 0;
		gbc_lblFactSheetSelectSystem.gridy = 0;
		factSheetReportSysDropDownPanel.add(lblFactSheetSelectSystem,gbc_lblFactSheetSelectSystem);

		factSheetReportSyscomboBox = new FactSheetReportComboBox(fetching);
		factSheetReportSyscomboBox.setFont(new Font("Tahoma", Font.PLAIN, 11));
		factSheetReportSyscomboBox.setBackground(Color.GRAY);
		factSheetReportSyscomboBox.setPreferredSize(new Dimension(200, 25));
		GridBagConstraints gbc_factSheetReportSyscomboBox = new GridBagConstraints();
		gbc_factSheetReportSyscomboBox.fill = GridBagConstraints.HORIZONTAL;
		gbc_factSheetReportSyscomboBox.gridx = 0;
		gbc_factSheetReportSyscomboBox.gridy = 1;
		factSheetReportSysDropDownPanel.add(factSheetReportSyscomboBox,	gbc_factSheetReportSyscomboBox);

		factSheetReportTypeDropDownPanel = new JPanel();
		factSheetReportTypeDropDownPanel.setBackground(SystemColor.control);
		GridBagConstraints gbc_factSheetReportTypeDropDownPanel = new GridBagConstraints();
		gbc_factSheetReportTypeDropDownPanel.gridheight = 2;
		gbc_factSheetReportTypeDropDownPanel.insets = new Insets(0, 10, 5, 5);
		gbc_factSheetReportTypeDropDownPanel.fill = GridBagConstraints.BOTH;
		gbc_factSheetReportTypeDropDownPanel.gridx = 3;
		gbc_factSheetReportTypeDropDownPanel.gridy = 3;
		FactSheetPanel.add(factSheetReportTypeDropDownPanel,gbc_factSheetReportTypeDropDownPanel);
		GridBagLayout gbl_factSheetReportTypeDropDownPanel = new GridBagLayout();
		gbl_factSheetReportTypeDropDownPanel.columnWidths = new int[] { 0, 0 };
		gbl_factSheetReportTypeDropDownPanel.rowHeights = new int[] { 0, 0, 0 };
		gbl_factSheetReportTypeDropDownPanel.columnWeights = new double[] {	1.0, Double.MIN_VALUE };
		gbl_factSheetReportTypeDropDownPanel.rowWeights = new double[] { 0.0,0.0, Double.MIN_VALUE };
		factSheetReportTypeDropDownPanel.setLayout(gbl_factSheetReportTypeDropDownPanel);
		

		JLabel lblSelectFactSheetReportType = new JLabel("Select Report Type");
		lblSelectFactSheetReportType.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblSelectFactSheetReportType = new GridBagConstraints();
		gbc_lblSelectFactSheetReportType.insets = new Insets(0, 0, 5, 0);
		gbc_lblSelectFactSheetReportType.gridx = 0;
		gbc_lblSelectFactSheetReportType.gridy = 0;
		factSheetReportTypeDropDownPanel.add(lblSelectFactSheetReportType, gbc_lblSelectFactSheetReportType);

		FactSheetReportTypecomboBox = new JComboBox();
		FactSheetReportTypecomboBox.setFont(new Font("Tahoma", Font.PLAIN, 11));
		FactSheetReportTypecomboBox.setBackground(Color.GRAY);
		GridBagConstraints gbc_factSheetReportTypecomboBox = new GridBagConstraints();
		gbc_factSheetReportTypecomboBox.fill = GridBagConstraints.HORIZONTAL;
		gbc_factSheetReportTypecomboBox.gridx = 0;
		gbc_factSheetReportTypecomboBox.gridy = 1;
		factSheetReportTypeDropDownPanel.add(FactSheetReportTypecomboBox, gbc_factSheetReportTypecomboBox);
		FactSheetReportTypecomboBox.setModel(new DefaultComboBoxModel(new String[] { "All Systems" , "System Specific" }));

		btnFactSheetReport = new CustomButton("Generate Fact Sheet Report");
		btnFactSheetReport.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_btnFactSheetReport = new GridBagConstraints();
		gbc_btnFactSheetReport.fill = GridBagConstraints.HORIZONTAL;
		gbc_btnFactSheetReport.gridwidth = 2;
		gbc_btnFactSheetReport.insets = new Insets(0, 0, 5, 5);
		gbc_btnFactSheetReport.gridx = 4;
		gbc_btnFactSheetReport.gridy = 5;
		FactSheetPanel.add(btnFactSheetReport, gbc_btnFactSheetReport);
		Style.registerTargetClassName(btnFactSheetReport, ".standardButton");

		JLabel CONUSMapExportTitleLabel = new JLabel("Export Application Health Grid and Maps for all Systems:");
		GridBagConstraints gbc_CONUSMapExportTitleLabel = new GridBagConstraints();
		gbc_CONUSMapExportTitleLabel.gridwidth = 3;
		gbc_CONUSMapExportTitleLabel.fill = GridBagConstraints.BOTH;
		gbc_CONUSMapExportTitleLabel.insets = new Insets(5, 20, 10, 5);
		gbc_CONUSMapExportTitleLabel.gridx = 1;
		gbc_CONUSMapExportTitleLabel.gridy = 6;
		FactSheetPanel.add(CONUSMapExportTitleLabel, gbc_CONUSMapExportTitleLabel);
		CONUSMapExportTitleLabel.setFont(new Font("Tahoma", Font.PLAIN, 12));

		btnFactSheetImageExport = new CustomButton("Export all");
		btnFactSheetImageExport.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_btnCONUSMapExport = new GridBagConstraints();
		gbc_btnCONUSMapExport.fill = GridBagConstraints.HORIZONTAL;
		gbc_btnCONUSMapExport.gridwidth = 1;
		gbc_btnCONUSMapExport.insets = new Insets(0, 0, 5, 5);
		gbc_btnCONUSMapExport.gridx = 4;
		gbc_btnCONUSMapExport.gridy = 6;
		FactSheetPanel.add(btnFactSheetImageExport, gbc_btnCONUSMapExport);
		Style.registerTargetClassName(btnFactSheetImageExport, ".standardButton");

		separator_8 = new JSeparator();
		GridBagConstraints gbc_separator_8 = new GridBagConstraints();
		gbc_separator_8.fill = GridBagConstraints.HORIZONTAL;
		gbc_separator_8.gridwidth = 1;
		gbc_separator_8.insets = new Insets(5, 5, 5, 5);
		gbc_separator_8.gridx = 0;
		gbc_separator_8.gridy = 5;
		tapReportPanel.add(separator_8, gbc_separator_8);

		JPanel TaskerGenerationPanel = new JPanel();
		TaskerGenerationPanel.setBackground(SystemColor.control);
		GridBagConstraints gbc_TaskerGenerationPanel = new GridBagConstraints();
		gbc_TaskerGenerationPanel.fill = GridBagConstraints.HORIZONTAL;
		gbc_TaskerGenerationPanel.insets = new Insets(0, 0, 5, 5);
		gbc_TaskerGenerationPanel.gridx = 0;
		gbc_TaskerGenerationPanel.gridy = 6;
		tapReportPanel.add(TaskerGenerationPanel, gbc_TaskerGenerationPanel);
		
		GridBagLayout gbl_TaskerGenerationPanel = new GridBagLayout();
		gbl_TaskerGenerationPanel.columnWidths = new int[] {30, 0, 30, 30, 60, 30, 30, 30, 30, 30, 0};
		gbl_TaskerGenerationPanel.rowHeights = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
		gbl_TaskerGenerationPanel.columnWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE };
		gbl_TaskerGenerationPanel.rowWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0,	0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,	0.0, 0.0, Double.MIN_VALUE };
		TaskerGenerationPanel.setLayout(gbl_TaskerGenerationPanel);

		JLabel TaskerGenerationTitleLabel = new JLabel("Tasker and System Info Report Generation");
		TaskerGenerationTitleLabel.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_TaskerGenerationTitleLabel = new GridBagConstraints();
		gbc_TaskerGenerationTitleLabel.gridwidth = 3;
		gbc_TaskerGenerationTitleLabel.fill = GridBagConstraints.BOTH;
		gbc_TaskerGenerationTitleLabel.insets = new Insets(10, 10, 5, 5);
		gbc_TaskerGenerationTitleLabel.gridx = 1;
		gbc_TaskerGenerationTitleLabel.gridy = 0;
		TaskerGenerationPanel.add(TaskerGenerationTitleLabel, gbc_TaskerGenerationTitleLabel);

		JLabel lblTaskerGenerationSelectSystem = new JLabel("Select System");
		lblTaskerGenerationSelectSystem.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblTaskerGenerationSelectSystem = new GridBagConstraints();
		gbc_lblTaskerGenerationSelectSystem.insets = new Insets(0, 0, 5, 0);
		gbc_lblTaskerGenerationSelectSystem.gridx = 1;
		gbc_lblTaskerGenerationSelectSystem.gridy = 1;
		TaskerGenerationPanel.add(lblTaskerGenerationSelectSystem,gbc_lblTaskerGenerationSelectSystem);

		TaskerGenerationSyscomboBox = new FactSheetReportComboBox(fetching);
		TaskerGenerationSyscomboBox.setFont(new Font("Tahoma", Font.PLAIN, 11));
		TaskerGenerationSyscomboBox.setBackground(Color.GRAY);
		TaskerGenerationSyscomboBox.setPreferredSize(new Dimension(200, 25));
		GridBagConstraints gbc_TaskerGenerationSyscomboBox = new GridBagConstraints();
		gbc_TaskerGenerationSyscomboBox.fill = GridBagConstraints.HORIZONTAL;
		gbc_TaskerGenerationSyscomboBox.gridx = 1;
		gbc_TaskerGenerationSyscomboBox.gridy = 2;
		TaskerGenerationPanel.add(TaskerGenerationSyscomboBox,	gbc_TaskerGenerationSyscomboBox);		
		
		btnTaskerGeneration = new CustomButton("Generate Tasker Report");
		btnTaskerGeneration.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_btnTaskerGeneration = new GridBagConstraints();
		gbc_btnTaskerGeneration.fill = GridBagConstraints.HORIZONTAL;
		gbc_btnTaskerGeneration.gridwidth = 2;
		gbc_btnTaskerGeneration.insets = new Insets(0, 0, 5, 5);
		gbc_btnTaskerGeneration.gridx = 1;
		gbc_btnTaskerGeneration.gridy = 3;
		TaskerGenerationPanel.add(btnTaskerGeneration, gbc_btnTaskerGeneration);
		Style.registerTargetClassName(btnTaskerGeneration, ".standardButton");
		
		btnSystemInfoGenButton = new CustomButton("Generate System Info Report");
		btnSystemInfoGenButton.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_btnSystemInfoGenerator = new GridBagConstraints();
		gbc_btnSystemInfoGenerator.fill = GridBagConstraints.HORIZONTAL;
		gbc_btnSystemInfoGenerator.gridwidth = 2;
		gbc_btnSystemInfoGenerator.insets = new Insets(0, 0, 5, 5);
		gbc_btnSystemInfoGenerator.gridx = 1;
		gbc_btnSystemInfoGenerator.gridy = 4;
		TaskerGenerationPanel.add(btnSystemInfoGenButton, gbc_btnSystemInfoGenerator);
		Style.registerTargetClassName(btnSystemInfoGenButton, ".standardButton");
		
//		JLabel CapabilityFactSheetTitleLabel = new JLabel("Capability Fact Sheet Generation");
//		CapabilityFactSheetTitleLabel.setFont(new Font("Tahoma", Font.BOLD, 12));
//		GridBagConstraints gbc_CapabilityFactSheetTitleLabel = new GridBagConstraints();
//		gbc_CapabilityFactSheetTitleLabel.gridwidth = 3;
//		gbc_CapabilityFactSheetTitleLabel.fill = GridBagConstraints.BOTH;
//		gbc_CapabilityFactSheetTitleLabel.insets = new Insets(10, 10, 5, 5);
//		gbc_CapabilityFactSheetTitleLabel.gridx = 4;
//		gbc_CapabilityFactSheetTitleLabel.gridy = 0;
//		TaskerGenerationPanel.add(CapabilityFactSheetTitleLabel, gbc_CapabilityFactSheetTitleLabel);
//
//		JLabel lblCapabilityFactSheetSelectCapability = new JLabel("Select Capability");
//		lblCapabilityFactSheetSelectCapability.setFont(new Font("Tahoma", Font.PLAIN, 12));
//		GridBagConstraints gbc_lblCapabilityFactSheetSelectCapability = new GridBagConstraints();
//		gbc_lblCapabilityFactSheetSelectCapability.insets = new Insets(0, 0, 5, 0);
//		gbc_lblCapabilityFactSheetSelectCapability.gridx = 4;
//		gbc_lblCapabilityFactSheetSelectCapability.gridy = 1;
//		TaskerGenerationPanel.add(lblCapabilityFactSheetSelectCapability,gbc_lblCapabilityFactSheetSelectCapability);
//
//		capabilityFactSheetCapComboBox = new FactSheetReportComboBox(fetching);
//		capabilityFactSheetCapComboBox.setFont(new Font("Tahoma", Font.PLAIN, 11));
//		capabilityFactSheetCapComboBox.setBackground(Color.GRAY);
//		capabilityFactSheetCapComboBox.setPreferredSize(new Dimension(200, 25));
//		GridBagConstraints gbc_capabilityFactSheetCapComboBox = new GridBagConstraints();
//		gbc_capabilityFactSheetCapComboBox.fill = GridBagConstraints.HORIZONTAL;
//		gbc_capabilityFactSheetCapComboBox.gridx = 4;
//		gbc_capabilityFactSheetCapComboBox.gridy = 2;
//		TaskerGenerationPanel.add(capabilityFactSheetCapComboBox,	gbc_capabilityFactSheetCapComboBox);		
//		
//		btnCapabilityFactSheetGeneration = new CustomButton("Generate Capability Fact Sheet");
//		btnCapabilityFactSheetGeneration.setFont(new Font("Tahoma", Font.BOLD, 11));
//		GridBagConstraints gbc_btnCapabilityFactSheetGeneration = new GridBagConstraints();
//		gbc_btnCapabilityFactSheetGeneration.fill = GridBagConstraints.HORIZONTAL;
//		gbc_btnCapabilityFactSheetGeneration.gridwidth = 2;
//		gbc_btnCapabilityFactSheetGeneration.insets = new Insets(0, 0, 5, 5);
//		gbc_btnCapabilityFactSheetGeneration.gridx = 4;
//		gbc_btnCapabilityFactSheetGeneration.gridy = 3;
//		TaskerGenerationPanel.add(btnCapabilityFactSheetGeneration, gbc_btnCapabilityFactSheetGeneration);
//		Style.registerTargetClassName(btnCapabilityFactSheetGeneration, ".standardButton");
	
		
		JSeparator separator_5 = new JSeparator();
		GridBagConstraints gbc_separator_5 = new GridBagConstraints();
		gbc_separator_5.fill = GridBagConstraints.HORIZONTAL;
		gbc_separator_5.gridwidth = 6;
		gbc_separator_5.insets = new Insets(0, 0, 5, 5);
		gbc_separator_5.gridx = 0;
		gbc_separator_5.gridy = 15;
		financialsPanel.add(separator_5, gbc_separator_5);

		JLabel lblAdvancedFinancialFunctions = new JLabel("Advanced Financial Functions");
		lblAdvancedFinancialFunctions.setFont(new Font("Tahoma", Font.BOLD, 12));
		lblAdvancedFinancialFunctions.setBackground(Color.WHITE);
		GridBagConstraints gbc_lblAdvancedFinancialFunctions = new GridBagConstraints();
		gbc_lblAdvancedFinancialFunctions.anchor = GridBagConstraints.WEST;
		gbc_lblAdvancedFinancialFunctions.gridwidth = 4;
		gbc_lblAdvancedFinancialFunctions.insets = new Insets(10, 10, 5, 5);
		gbc_lblAdvancedFinancialFunctions.gridx = 0;
		gbc_lblAdvancedFinancialFunctions.gridy = 16;
		financialsPanel.add(lblAdvancedFinancialFunctions, gbc_lblAdvancedFinancialFunctions);

		btnAdvancedFinancialFunctions = new ToggleButton("Show");
		btnAdvancedFinancialFunctions.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_btnAdvancedFinancialFunctions = new GridBagConstraints();
		gbc_btnAdvancedFinancialFunctions.fill = GridBagConstraints.HORIZONTAL;
		gbc_btnAdvancedFinancialFunctions.insets = new Insets(10, 20, 5, 5);
		gbc_btnAdvancedFinancialFunctions.gridx = 0;
		gbc_btnAdvancedFinancialFunctions.gridy = 17;
		financialsPanel.add(btnAdvancedFinancialFunctions,	gbc_btnAdvancedFinancialFunctions);

		advancedFunctionsPanel = new JPanel();
		advancedFunctionsPanel.setBackground(SystemColor.control);
		GridBagConstraints gbc_advancedFunctionsPanel = new GridBagConstraints();
		gbc_advancedFunctionsPanel.anchor = GridBagConstraints.NORTH;
		gbc_advancedFunctionsPanel.gridwidth = 6;
		gbc_advancedFunctionsPanel.insets = new Insets(0, 0, 5, 5);
		gbc_advancedFunctionsPanel.fill = GridBagConstraints.HORIZONTAL;
		gbc_advancedFunctionsPanel.gridx = 0;
		gbc_advancedFunctionsPanel.gridy = 18;
		financialsPanel.add(advancedFunctionsPanel, gbc_advancedFunctionsPanel);
		GridBagLayout gbl_advancedFunctionsPanel = new GridBagLayout();
		gbl_advancedFunctionsPanel.columnWidths = new int[] { 0, 0, 0 };
		gbl_advancedFunctionsPanel.rowHeights = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
		gbl_advancedFunctionsPanel.columnWeights = new double[] { 0.0, 1.0,	Double.MIN_VALUE };
		gbl_advancedFunctionsPanel.rowWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,	Double.MIN_VALUE };
		advancedFunctionsPanel.setLayout(gbl_advancedFunctionsPanel);

		JLabel lblTransitionCostCalculations = new JLabel("Transition Cost Calculations");
		GridBagConstraints gbc_lblTransitionCostCalculations = new GridBagConstraints();
		gbc_lblTransitionCostCalculations.gridwidth = 2;
		gbc_lblTransitionCostCalculations.anchor = GridBagConstraints.WEST;
		gbc_lblTransitionCostCalculations.insets = new Insets(20, 10, 5, 0);
		gbc_lblTransitionCostCalculations.gridx = 0;
		gbc_lblTransitionCostCalculations.gridy = 0;
		advancedFunctionsPanel.add(lblTransitionCostCalculations, gbc_lblTransitionCostCalculations);
		lblTransitionCostCalculations.setFont(new Font("Tahoma", Font.BOLD, 12));
		lblTransitionCostCalculations.setBackground(Color.WHITE);

		JLabel lblcalculateAndInsert = new JLabel("<HTML>Calculate and insert base transition estimates:\r\n </HTML>");
		lblcalculateAndInsert.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblcalculateAndInsert = new GridBagConstraints();
		gbc_lblcalculateAndInsert.anchor = GridBagConstraints.WEST;
		gbc_lblcalculateAndInsert.gridwidth = 2;
		gbc_lblcalculateAndInsert.insets = new Insets(5, 20, 5, 0);
		gbc_lblcalculateAndInsert.gridx = 0;
		gbc_lblcalculateAndInsert.gridy = 1;
		advancedFunctionsPanel.add(lblcalculateAndInsert, gbc_lblcalculateAndInsert);

		calculateTransitionCostsButton = new CustomButton("Calculate Base Transition Costs");
		calculateTransitionCostsButton.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_calculateTransitionCostsButton = new GridBagConstraints();
		gbc_calculateTransitionCostsButton.fill = GridBagConstraints.BOTH;
		gbc_calculateTransitionCostsButton.gridheight = 2;
		gbc_calculateTransitionCostsButton.insets = new Insets(5, 20, 5, 5);
		gbc_calculateTransitionCostsButton.gridx = 0;
		gbc_calculateTransitionCostsButton.gridy = 2;
		advancedFunctionsPanel.add(calculateTransitionCostsButton, gbc_calculateTransitionCostsButton);

		rdbtnApplyTapOverhead = new JRadioButton("Apply TAP Overhead in Calculation");
		rdbtnApplyTapOverhead.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_rdbtnApplyTapOverhead = new GridBagConstraints();
		gbc_rdbtnApplyTapOverhead.anchor = GridBagConstraints.WEST;
		gbc_rdbtnApplyTapOverhead.insets = new Insets(5, 0, 5, 0);
		gbc_rdbtnApplyTapOverhead.gridx = 1;
		gbc_rdbtnApplyTapOverhead.gridy = 2;
		advancedFunctionsPanel.add(rdbtnApplyTapOverhead, gbc_rdbtnApplyTapOverhead);
		rdbtnApplyTapOverhead.setSelected(true);

		rdbtnDoNotApplyOverhead = new JRadioButton("Do Not Apply TAP Overhead in Calculation");
		rdbtnDoNotApplyOverhead.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_rdbtnDoNotApplyOverhead = new GridBagConstraints();
		gbc_rdbtnDoNotApplyOverhead.anchor = GridBagConstraints.WEST;
		gbc_rdbtnDoNotApplyOverhead.insets = new Insets(0, 0, 5, 0);
		gbc_rdbtnDoNotApplyOverhead.gridx = 1;
		gbc_rdbtnDoNotApplyOverhead.gridy = 3;
		advancedFunctionsPanel.add(rdbtnDoNotApplyOverhead,	gbc_rdbtnDoNotApplyOverhead);

		JLabel lbltransitionCostsCalulations_1 = new JLabel("<HTML>\r\nCalculate and insert Semantics, Training, and Sustainment: </HTML>");
		lbltransitionCostsCalulations_1.setFont(new Font("Tahoma", Font.PLAIN,	12));
		GridBagConstraints gbc_lbltransitionCostsCalulations_1 = new GridBagConstraints();
		gbc_lbltransitionCostsCalulations_1.anchor = GridBagConstraints.WEST;
		gbc_lbltransitionCostsCalulations_1.insets = new Insets(10, 20, 5, 0);
		gbc_lbltransitionCostsCalulations_1.gridwidth = 2;
		gbc_lbltransitionCostsCalulations_1.gridx = 0;
		gbc_lbltransitionCostsCalulations_1.gridy = 4;
		advancedFunctionsPanel.add(lbltransitionCostsCalulations_1,	gbc_lbltransitionCostsCalulations_1);

		calcTransAdditionalButton = new CustomButton("Calculate Additional Transition Costs");
		calcTransAdditionalButton.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_calcTransAdditionalButton = new GridBagConstraints();
		gbc_calcTransAdditionalButton.anchor = GridBagConstraints.WEST;
		gbc_calcTransAdditionalButton.insets = new Insets(5, 20, 5, 5);
		gbc_calcTransAdditionalButton.gridx = 0;
		gbc_calcTransAdditionalButton.gridy = 5;
		advancedFunctionsPanel.add(calcTransAdditionalButton, gbc_calcTransAdditionalButton);

		calcTCprogressBar = new JProgressBar();
		GridBagConstraints gbc_calcTCprogressBar = new GridBagConstraints();
		gbc_calcTCprogressBar.insets = new Insets(10, 20, 5, 0);
		gbc_calcTCprogressBar.fill = GridBagConstraints.HORIZONTAL;
		gbc_calcTCprogressBar.gridwidth = 2;
		gbc_calcTCprogressBar.gridx = 0;
		gbc_calcTCprogressBar.gridy = 6;
		advancedFunctionsPanel.add(calcTCprogressBar, gbc_calcTCprogressBar);

		JPanel settingsPanel = new JPanel();
		settingsPanel.setBackground(SystemColor.control);

		rightView.addTab("Settings", null, settingsPanel, null);
		GridBagLayout gbl_settingsPanel = new GridBagLayout();
		gbl_settingsPanel.columnWidths = new int[] { 15, 0, 0 };
		gbl_settingsPanel.rowHeights = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0 };
		gbl_settingsPanel.columnWeights = new double[] { 0.0, 0.0,	Double.MIN_VALUE };
		gbl_settingsPanel.rowWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,	Double.MIN_VALUE };
		settingsPanel.setLayout(gbl_settingsPanel);

		JLabel lblNetworkGraphsheetSettings = new JLabel("Network Graphsheet Generation Settings");
		lblNetworkGraphsheetSettings.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_lblNetworkGraphsheetSettings = new GridBagConstraints();
		gbc_lblNetworkGraphsheetSettings.insets = new Insets(15, 0, 10, 0);
		gbc_lblNetworkGraphsheetSettings.gridx = 1;
		gbc_lblNetworkGraphsheetSettings.gridy = 0;
		settingsPanel.add(lblNetworkGraphsheetSettings,	gbc_lblNetworkGraphsheetSettings);

		propertyCheck = new JCheckBox("Enable graph properties");
		propertyCheck.setFont(new Font("Tahoma", Font.PLAIN, 11));
		boolean propBool = false;
		if(DIHelper.getInstance().getProperty(Constants.GPSProp) != null)
			propBool = Boolean.parseBoolean(DIHelper.getInstance().getProperty(Constants.GPSProp));
		else
			logger.error("Failed to read default sudowl boolean from map");
		propertyCheck.setSelected(propBool);
		GridBagConstraints gbc_propertyCheck = new GridBagConstraints();
		gbc_propertyCheck.anchor = GridBagConstraints.WEST;
		gbc_propertyCheck.insets = new Insets(0, 5, 5, 0);
		gbc_propertyCheck.gridx = 1;
		gbc_propertyCheck.gridy = 1;
		settingsPanel.add(propertyCheck, gbc_propertyCheck);

		sudowlCheck = new JCheckBox("Enable SUDOWL");
		boolean sudowlBool = false;
		if(DIHelper.getInstance().getProperty(Constants.GPSSudowl) != null)
			sudowlBool = Boolean.parseBoolean(DIHelper.getInstance().getProperty(Constants.GPSSudowl));
		else
			logger.error("Failed to read default sudowl boolean from map");
		sudowlCheck.setSelected(sudowlBool);
		sudowlCheck.setFont(new Font("Tahoma", Font.PLAIN, 11));
		GridBagConstraints gbc_sudowlCheck = new GridBagConstraints();
		gbc_sudowlCheck.anchor = GridBagConstraints.WEST;
		gbc_sudowlCheck.insets = new Insets(0, 5, 5, 0);
		gbc_sudowlCheck.gridx = 1;
		gbc_sudowlCheck.gridy = 2;
		settingsPanel.add(sudowlCheck, gbc_sudowlCheck);

		searchCheck = new JCheckBox("Enable graph search");
		boolean searchBool = false;
		if(DIHelper.getInstance().getProperty(Constants.GPSSearch) != null)
			searchBool = Boolean.parseBoolean(DIHelper.getInstance().getProperty(Constants.GPSSearch));
		else
			logger.error("Failed to read default sudowl boolean from map");
		searchCheck.setFont(new Font("Tahoma", Font.PLAIN, 11));
		searchCheck.setSelected(searchBool);
		GridBagConstraints gbc_searchCheck = new GridBagConstraints();
		gbc_searchCheck.insets = new Insets(0, 5, 5, 0);
		gbc_searchCheck.anchor = GridBagConstraints.WEST;
		gbc_searchCheck.gridx = 1;
		gbc_searchCheck.gridy = 3;
		settingsPanel.add(searchCheck, gbc_searchCheck);

		highQualityExportCheck = new JCheckBox(	"Enable high quality vector graph export (*.eps)");
		highQualityExportCheck.setFont(new Font("Tahoma", Font.PLAIN, 11));
		highQualityExportCheck.setSelected(false);
		GridBagConstraints gbc_highQualityExportCheck = new GridBagConstraints();
		gbc_highQualityExportCheck.insets = new Insets(0, 5, 5, 0);
		gbc_highQualityExportCheck.anchor = GridBagConstraints.WEST;
		gbc_highQualityExportCheck.gridx = 1;
		gbc_highQualityExportCheck.gridy = 4;
		settingsPanel.add(highQualityExportCheck, gbc_highQualityExportCheck);
		DIHelper.getInstance().setLocalProperty(Constants.highQualityExport,false);
		
//		btnCommonGraph = new JButton("Find Common Graph");
//		btnCommonGraph.setFont(new Font("Tahoma", Font.BOLD, 11));
//		GridBagConstraints gbc_btnCommonGraph = new GridBagConstraints();
//		gbc_btnCommonGraph.gridx = 1;
//		gbc_btnCommonGraph.gridy = 7;
//		settingsPanel.add(btnCommonGraph, gbc_btnCommonGraph);
//		Style.registerTargetClassName(btnCommonGraph, ".standardButton");
		
		JPanel overAllHelpPanel = new JPanel();
		overAllHelpPanel.setBackground(SystemColor.control);
		overAllHelpPanel.setBorder(null);
		JScrollPane settingPanelScroll = new JScrollPane(overAllHelpPanel);
		rightView.addTab("Help", null, settingPanelScroll, null);
		GridBagLayout gbl_overAllHelpPanel = new GridBagLayout();
		gbl_overAllHelpPanel.columnWidths = new int[] { 0, 0 };
		gbl_overAllHelpPanel.rowHeights = new int[] { 337, 0 };
		gbl_overAllHelpPanel.columnWeights = new double[] { 1.0,Double.MIN_VALUE };
		gbl_overAllHelpPanel.rowWeights = new double[] { 1.0, Double.MIN_VALUE };
		overAllHelpPanel.setLayout(gbl_overAllHelpPanel);

		JPanel helpPanel = new JPanel();
		helpPanel.setBackground(SystemColor.control);
		// panel.setComponentOrientation(java.awt.ComponentOrientation.RIGHT_TO_LEFT);
		// overAllHelpPanel.setComponentOrientation(java.awt.ComponentOrientation.RIGHT_TO_LEFT);
		GridBagConstraints gbc_helpPanel = new GridBagConstraints();
		gbc_helpPanel.anchor = GridBagConstraints.NORTH;
		gbc_helpPanel.fill = GridBagConstraints.HORIZONTAL;
		gbc_helpPanel.gridx = 0;
		gbc_helpPanel.gridy = 0;
		overAllHelpPanel.add(helpPanel, gbc_helpPanel);
		GridBagLayout gbl_helpPanel = new GridBagLayout();
		gbl_helpPanel.columnWidths = new int[] { 0, 0, 0, 0 };
		gbl_helpPanel.rowHeights = new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,	0 };
		gbl_helpPanel.columnWeights = new double[] { 0.0, 0.0, 1.0,	Double.MIN_VALUE };
		gbl_helpPanel.rowWeights = new double[] { 0.0, 0.0, 0.0, 1.0, 0.0, 0.0,0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE };
		helpPanel.setLayout(gbl_helpPanel);

		JLabel lblAbout = new JLabel("About");
		lblAbout.setBackground(Color.WHITE);
		GridBagConstraints gbc_lblAbout = new GridBagConstraints();
		gbc_lblAbout.gridwidth = 2;
		gbc_lblAbout.anchor = GridBagConstraints.NORTHWEST;
		gbc_lblAbout.insets = new Insets(10, 20, 10, 250);
		gbc_lblAbout.gridx = 1;
		gbc_lblAbout.gridy = 0;
		helpPanel.add(lblAbout, gbc_lblAbout);
		lblAbout.setFont(new Font("Tahoma", Font.BOLD, 12));

		aboutArea = new JTextArea();
		aboutArea.setFont(new Font("Tahoma", Font.PLAIN, 12));
		aboutArea.setEditable(false);
		aboutArea.setBorder(null);
		aboutArea.setBackground(SystemColor.control);
		aboutArea.setForeground(new Color(0, 0, 0));
		aboutArea.setWrapStyleWord(true);
		aboutArea.setLineWrap(true);
		aboutArea.setText("The Graph Tool is an innovative and data-driven application that allows users to explore and uncover connections among existing data from multiple repositories in an effort to make more informed decisions.\r\n\r\nThe tool displays data from one or more databases in an interactive format that users can customize, overlay, and extend based on their individual needs. This can help establish connections between a specific piece of data, that a healthcare provider needs to perform a certain procedure, with the system(s) that provide it.\r\n");
		GridBagConstraints gbc_aboutArea = new GridBagConstraints();
		gbc_aboutArea.gridwidth = 2;
		gbc_aboutArea.fill = GridBagConstraints.HORIZONTAL;
		gbc_aboutArea.insets = new Insets(0, 40, 5, 250);
		gbc_aboutArea.gridx = 1;
		gbc_aboutArea.gridy = 1;
		helpPanel.add(aboutArea, gbc_aboutArea);

		JLabel lblReleaseNotes = new JLabel("Release Notes");
		lblReleaseNotes.setHorizontalAlignment(SwingConstants.LEFT);
		lblReleaseNotes.setFont(new Font("Tahoma", Font.BOLD, 12));
		lblReleaseNotes.setBackground(Color.WHITE);
		GridBagConstraints gbc_lblReleaseNotes = new GridBagConstraints();
		gbc_lblReleaseNotes.anchor = GridBagConstraints.NORTHWEST;
		gbc_lblReleaseNotes.insets = new Insets(0, 20, 10, 250);
		gbc_lblReleaseNotes.gridx = 1;
		gbc_lblReleaseNotes.gridy = 2;
		helpPanel.add(lblReleaseNotes, gbc_lblReleaseNotes);

		releaseNoteArea = new JTextPane();
		GridBagConstraints gbc_releaseNoteArea = new GridBagConstraints();
		gbc_releaseNoteArea.anchor = GridBagConstraints.WEST;
		gbc_releaseNoteArea.insets = new Insets(0, 40, 5, 250);
		gbc_releaseNoteArea.gridx = 1;
		gbc_releaseNoteArea.gridy = 3;
		helpPanel.add(releaseNoteArea, gbc_releaseNoteArea);
		releaseNoteArea.setMinimumSize(new Dimension(12, 20));
		releaseNoteArea.setContentType("text/html");
		releaseNoteArea.setBorder(null);
		releaseNoteArea.setText(releaseNotesData);
		releaseNoteArea.setForeground(Color.DARK_GRAY);
		releaseNoteArea.setBackground(SystemColor.control);
		releaseNoteArea.setEditable(false);

		JLabel lblLearningMaterials = new JLabel("Learning Materials");
		lblLearningMaterials.setHorizontalAlignment(SwingConstants.LEFT);
		lblLearningMaterials.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_lblLearningMaterials = new GridBagConstraints();
		gbc_lblLearningMaterials.anchor = GridBagConstraints.WEST;
		gbc_lblLearningMaterials.insets = new Insets(10, 20, 10, 250);
		gbc_lblLearningMaterials.gridx = 1;
		gbc_lblLearningMaterials.gridy = 5;
//		helpPanel.add(lblLearningMaterials, gbc_lblLearningMaterials);

		JLabel lblHeidiMaterials = new JLabel("Interactive Powerpoint");
		lblHeidiMaterials.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblHeidiMaterials = new GridBagConstraints();
		gbc_lblHeidiMaterials.anchor = GridBagConstraints.NORTHWEST;
		gbc_lblHeidiMaterials.insets = new Insets(0, 40, 5, 250);
		gbc_lblHeidiMaterials.gridx = 1;
		gbc_lblHeidiMaterials.gridy = 6;
//		helpPanel.add(lblHeidiMaterials, gbc_lblHeidiMaterials);

		pptTrainingBtn = new CustomButton(
				"Starting Interactive Powerpoint Training");
		GridBagConstraints gbc_pptTrainingBtn = new GridBagConstraints();
		gbc_pptTrainingBtn.anchor = GridBagConstraints.NORTHWEST;
		gbc_pptTrainingBtn.insets = new Insets(0, 45, 15, 250);
		gbc_pptTrainingBtn.gridx = 1;
		gbc_pptTrainingBtn.gridy = 7;
//		helpPanel.add(pptTrainingBtn, gbc_pptTrainingBtn);

		JLabel lblHowToVideo = new JLabel("How to Videos");
		lblHowToVideo.setFont(new Font("Tahoma", Font.PLAIN, 12));
		GridBagConstraints gbc_lblHowToVideo = new GridBagConstraints();
		gbc_lblHowToVideo.anchor = GridBagConstraints.NORTHWEST;
		gbc_lblHowToVideo.insets = new Insets(0, 40, 5, 250);
		gbc_lblHowToVideo.gridx = 1;
		gbc_lblHowToVideo.gridy = 8;
//		helpPanel.add(lblHowToVideo, gbc_lblHowToVideo);

		htmlTrainingBtn = new CustomButton("Start Simulation Training");
		GridBagConstraints gbc_htmlTrainingBtn = new GridBagConstraints();
		gbc_htmlTrainingBtn.anchor = GridBagConstraints.NORTHWEST;
		gbc_htmlTrainingBtn.insets = new Insets(0, 45, 5, 250);
		gbc_htmlTrainingBtn.gridx = 1;
		gbc_htmlTrainingBtn.gridy = 9;
//		helpPanel.add(htmlTrainingBtn, gbc_htmlTrainingBtn);

		advancedFunctionsPanel.setVisible(false);
		calcTCprogressBar.setVisible(false);

		JTabbedPane leftView = new JTabbedPane(JTabbedPane.TOP);
		// JScrollPane leftScrollView = new JScrollPane(leftView);
		splitPane.setLeftComponent(leftView);

		JPanel inputPanel = new JPanel();
		inputPanel.setBackground(Color.WHITE);
		// JScrollPane inputPanelScroll = new JScrollPane(inputPanel);
		leftView.addTab("Main", null, inputPanel, null);
		GridBagLayout gbl_inputPanel = new GridBagLayout();
		gbl_inputPanel.columnWidths = new int[] { 5, 0, 55, 0, 0 };
		gbl_inputPanel.rowHeights = new int[] { 30, 0, 89, 19, 0, 15, 0, 0, 0,0, 0, 0, 0, 30, 0, 30 };
		gbl_inputPanel.columnWeights = new double[] { 0.0, 0.0, 1.0, 0.0, 0.0 };
		gbl_inputPanel.rowWeights = new double[] { 0.0, 0.0, 0.0, 0.0, 0.0,0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0 };	
		inputPanel.setLayout(gbl_inputPanel);

		JLabel lblSectionADefine = new JLabel("Define Graph");
		lblSectionADefine.setForeground(Color.DARK_GRAY);
		lblSectionADefine.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_lblSectionADefine = new GridBagConstraints();
		gbc_lblSectionADefine.gridwidth = 5;
		gbc_lblSectionADefine.anchor = GridBagConstraints.SOUTHWEST;
		gbc_lblSectionADefine.insets = new Insets(0, 5, 5, 5);
		gbc_lblSectionADefine.gridx = 1;
		gbc_lblSectionADefine.gridy = 0;
		inputPanel.add(lblSectionADefine, gbc_lblSectionADefine);

		JLabel lblSelectA = new JLabel("1. Select a database to pull data from");
		lblSelectA.setForeground(Color.DARK_GRAY);
		lblSelectA.setFont(new Font("SansSerif", Font.PLAIN, 10));
		GridBagConstraints gbc_lblSelectA = new GridBagConstraints();
		gbc_lblSelectA.anchor = GridBagConstraints.WEST;
		gbc_lblSelectA.gridwidth = 5;
		gbc_lblSelectA.insets = new Insets(0, 5, 5, 5);
		gbc_lblSelectA.gridx = 1;
		gbc_lblSelectA.gridy = 1;
		inputPanel.add(lblSelectA, gbc_lblSelectA);

		repoList = new JList();
		repoList.setFont(new Font("Tahoma", Font.PLAIN, 11));
		repoList.setBorder(new LineBorder(Color.LIGHT_GRAY));
		repoList.setComponentOrientation(ComponentOrientation.LEFT_TO_RIGHT);
		repoList.setLayoutOrientation(JList.HORIZONTAL_WRAP);
		// repoList.setVisibleRowCount(5);
		repoList.ensureIndexIsVisible(repoList.getSelectedIndex());
		GridBagConstraints gbc_repoList = new GridBagConstraints();
		gbc_repoList.gridwidth = 4;
		gbc_repoList.fill = GridBagConstraints.BOTH;
		gbc_repoList.insets = new Insets(5, 10, 5, 10);
		gbc_repoList.gridx = 1;
		gbc_repoList.gridy = 2;
		JScrollPane listScrollPane = new JScrollPane(repoList);
		listScrollPane.setPreferredSize(new Dimension(150, 100));
		// listScrollPane.setMinimumSize(new Dimension(200,100));
		listScrollPane.setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_NEVER);
		listScrollPane.getVerticalScrollBar().setUI(new NewScrollBarUI());
		inputPanel.add(listScrollPane, gbc_repoList);
		// set the listener
		repoList.addListSelectionListener(RepoSelectionListener.getInstance());

		JLabel lblPerspective = new JLabel(	"2. Select the category you'd like to address");
		lblPerspective.setForeground(Color.DARK_GRAY);
		lblPerspective.setFont(new Font("SansSerif", Font.PLAIN, 10));
		GridBagConstraints gbc_lblPerspective = new GridBagConstraints();
		gbc_lblPerspective.gridwidth = 5;
		gbc_lblPerspective.anchor = GridBagConstraints.NORTHWEST;
		gbc_lblPerspective.insets = new Insets(5, 5, 5, 5);
		gbc_lblPerspective.gridx = 1;
		gbc_lblPerspective.gridy = 3;
		inputPanel.add(lblPerspective, gbc_lblPerspective);

		perspectiveSelector = new JComboBox();
		perspectiveSelector.setFont(new Font("Tahoma", Font.PLAIN, 11));
		perspectiveSelector.setBackground(new Color(119, 136, 153));
		perspectiveSelector.setPreferredSize(new Dimension(150, 25));
		// perspectiveSelector.setMinimumSize(new Dimension(150, 25));
		// perspectiveSelector.setMaximumSize(new Dimension(200, 32767));
		GridBagConstraints gbc_perspectiveSelector = new GridBagConstraints();
		gbc_perspectiveSelector.fill = GridBagConstraints.HORIZONTAL;
		gbc_perspectiveSelector.gridwidth = 4;
		gbc_perspectiveSelector.anchor = GridBagConstraints.NORTH;
		gbc_perspectiveSelector.insets = new Insets(5, 5, 5, 5);
		gbc_perspectiveSelector.gridx = 1;
		gbc_perspectiveSelector.gridy = 4;
		inputPanel.add(perspectiveSelector, gbc_perspectiveSelector);

		JLabel lblQuery = new JLabel("3. Select a specific question");
		lblQuery.setForeground(Color.DARK_GRAY);
		lblQuery.setFont(new Font("SansSerif", Font.PLAIN, 10));
		GridBagConstraints gbc_lblQuery = new GridBagConstraints();
		gbc_lblQuery.gridwidth = 5;
		gbc_lblQuery.anchor = GridBagConstraints.WEST;
		gbc_lblQuery.insets = new Insets(5, 5, 5, 5);
		gbc_lblQuery.gridx = 1;
		gbc_lblQuery.gridy = 5;
		inputPanel.add(lblQuery, gbc_lblQuery);

		questionSelector = new JComboBox();
		questionSelector.setFont(new Font("Tahoma", Font.PLAIN, 11));
		questionSelector.setBackground(new Color(119, 136, 153));
		questionSelector.setMinimumSize(new Dimension(60, 25));
		// questionSelector.setPreferredSize(new Dimension(150, 25));
		// questionSelector.setMaximumSize(new Dimension(200, 32767));
		GridBagConstraints gbc_questionSelector = new GridBagConstraints();
		gbc_questionSelector.anchor = GridBagConstraints.NORTH;
		gbc_questionSelector.gridwidth = 4;
		gbc_questionSelector.fill = GridBagConstraints.HORIZONTAL;
		gbc_questionSelector.insets = new Insets(5, 5, 5, 5);
		gbc_questionSelector.gridx = 1;
		gbc_questionSelector.gridy = 6;
		inputPanel.add(questionSelector, gbc_questionSelector);

		JLabel lblSelectAvailable = new JLabel("4. Select available parameters");
		lblSelectAvailable.setForeground(Color.DARK_GRAY);
		lblSelectAvailable.setFont(new Font("SansSerif", Font.PLAIN, 10));
		GridBagConstraints gbc_lblSelectAvailable = new GridBagConstraints();
		gbc_lblSelectAvailable.anchor = GridBagConstraints.WEST;
		gbc_lblSelectAvailable.gridwidth = 5;
		gbc_lblSelectAvailable.insets = new Insets(5, 5, 5, 5);
		gbc_lblSelectAvailable.gridx = 1;
		gbc_lblSelectAvailable.gridy = 7;
		inputPanel.add(lblSelectAvailable, gbc_lblSelectAvailable);

		paramPanel = new JPanel();
		paramPanel.setForeground(Color.DARK_GRAY);
		paramPanel.setBackground(Color.WHITE);
		GridBagConstraints gbc_paramPanel = new GridBagConstraints();
		gbc_paramPanel.fill = GridBagConstraints.HORIZONTAL;
		gbc_paramPanel.anchor = GridBagConstraints.NORTH;
		gbc_paramPanel.insets = new Insets(5, 0, 5, 5);
		gbc_paramPanel.gridwidth = 4;
		gbc_paramPanel.gridx = 1;
		gbc_paramPanel.gridy = 8;
		inputPanel.add(paramPanel, gbc_paramPanel);
		paramPanel.setLayout(new CardLayout(0, 0));

		submitButton = new Button();
		submitButton.setFont(new Font("Tahoma", Font.BOLD, 12));
		submitButton.setText("Create");
		// key binding
		String keyStroke = "enter";
		KeyStroke key = KeyStroke.getKeyStroke(KeyEvent.VK_ENTER,InputEvent.CTRL_DOWN_MASK);
		// map keystroke with action id
		submitButton.getInputMap(JComponent.WHEN_IN_FOCUSED_WINDOW).put(key,keyStroke);
		// map action id to listener
		ProcessQueryListener processQueryListener = new ProcessQueryListener();
		submitButton.getActionMap().put(keyStroke, processQueryListener);
		GridBagConstraints gbc_submitButton = new GridBagConstraints();
		gbc_submitButton.fill = GridBagConstraints.HORIZONTAL;
		gbc_submitButton.insets = new Insets(5, 5, 5, 5);
		gbc_submitButton.gridx = 1;
		gbc_submitButton.gridy = 9;
		inputPanel.add(submitButton, gbc_submitButton);

		JLabel lblNewLabel = new JLabel("<HTML> Opens window with<br>query results </HTML>");
		lblNewLabel.setForeground(Color.GRAY);
		lblNewLabel.setFont(new Font("SansSerif", Font.ITALIC, 10));
		GridBagConstraints gbc_lblNewLabel = new GridBagConstraints();
		gbc_lblNewLabel.gridwidth = 4;
		gbc_lblNewLabel.fill = GridBagConstraints.HORIZONTAL;
		gbc_lblNewLabel.insets = new Insets(5, 5, 5, 5);
		gbc_lblNewLabel.gridx = 2;
		gbc_lblNewLabel.gridy = 9;
		inputPanel.add(lblNewLabel, gbc_lblNewLabel);

		UIManager.put("Separator.foreground", Color.RED);

		appendButton = new ToggleButton("Overlay");
		appendButton.setEnabled(false);
		appendButton.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_appendButton = new GridBagConstraints();
		gbc_appendButton.fill = GridBagConstraints.HORIZONTAL;
		gbc_appendButton.insets = new Insets(5, 5, 5, 5);
		gbc_appendButton.gridx = 1;
		gbc_appendButton.gridy = 10;
		inputPanel.add(appendButton, gbc_appendButton);
		Style.registerTargetClassName(appendButton, ".toggleButtonDisabled");
		// appendButton.setEnabled(false);

		JLabel lblAddsAdditionalGraph = new JLabel("<HTML> Adds graph to window<br>based off another query</HTML>");
		lblAddsAdditionalGraph.setForeground(Color.GRAY);
		lblAddsAdditionalGraph.setFont(new Font("SansSerif", Font.ITALIC, 10));
		GridBagConstraints gbc_lblAddsAdditionalGraph = new GridBagConstraints();
		gbc_lblAddsAdditionalGraph.gridwidth = 4;
		gbc_lblAddsAdditionalGraph.fill = GridBagConstraints.HORIZONTAL;
		gbc_lblAddsAdditionalGraph.insets = new Insets(5, 5, 5, 5);
		gbc_lblAddsAdditionalGraph.gridx = 2;
		gbc_lblAddsAdditionalGraph.gridy = 10;
		inputPanel.add(lblAddsAdditionalGraph, gbc_lblAddsAdditionalGraph);

		separator_1 = new JSeparator();
		GridBagConstraints gbc_separator_1 = new GridBagConstraints();
		gbc_separator_1.fill = GridBagConstraints.HORIZONTAL;
		gbc_separator_1.gridwidth = 4;
		gbc_separator_1.insets = new Insets(5, 0, 5, 5);
		gbc_separator_1.gridx = 1;
		gbc_separator_1.gridy = 11;
		inputPanel.add(separator_1, gbc_separator_1);

		JLabel lblSectionCCustomize = new JLabel("Custom Sparql Query");
		lblSectionCCustomize.setForeground(Color.DARK_GRAY);
		lblSectionCCustomize.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_lblSectionCCustomize = new GridBagConstraints();
		gbc_lblSectionCCustomize.anchor = GridBagConstraints.WEST;
		gbc_lblSectionCCustomize.gridwidth = 5;
		gbc_lblSectionCCustomize.insets = new Insets(5, 5, 5, 5);
		gbc_lblSectionCCustomize.gridx = 1;
		gbc_lblSectionCCustomize.gridy = 12;
		inputPanel.add(lblSectionCCustomize, gbc_lblSectionCCustomize);

		btnCustomSparql = new ToggleButton("Custom");
		btnCustomSparql.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_btnCustomSparql = new GridBagConstraints();
		gbc_btnCustomSparql.fill = GridBagConstraints.HORIZONTAL;
		gbc_btnCustomSparql.insets = new Insets(5, 5, 5, 5);
		gbc_btnCustomSparql.gridx = 1;
		gbc_btnCustomSparql.gridy = 13;
		inputPanel.add(btnCustomSparql, gbc_btnCustomSparql);

		btnShowHint = new JButton();
		btnShowHint.setEnabled(false);
		btnShowHint.setHorizontalAlignment(SwingConstants.LEFT);
		btnShowHint.setToolTipText("Display Hint for PlaySheet");
		// btnShowHint.setText("Show Hint");
		try {
			Image img = ImageIO.read(new File(workingDir
					+ "/pictures/questionMark.png"));
			Image newimg = img.getScaledInstance(15, 15,
					java.awt.Image.SCALE_SMOOTH);
			btnShowHint.setIcon(new ImageIcon(newimg));
		} catch (IOException ex) {
		}

		lblModifyQueryOf = new JLabel("<HTML> Modify current query<br>or create new query</HTML>");
		lblModifyQueryOf.setForeground(Color.GRAY);
		lblModifyQueryOf.setFont(new Font("SansSerif", Font.ITALIC, 10));
		GridBagConstraints gbc_lblModifyQueryOf = new GridBagConstraints();
		gbc_lblModifyQueryOf.gridwidth = 4;
		gbc_lblModifyQueryOf.fill = GridBagConstraints.HORIZONTAL;
		gbc_lblModifyQueryOf.insets = new Insets(0, 5, 5, 5);
		gbc_lblModifyQueryOf.gridx = 2;
		gbc_lblModifyQueryOf.gridy = 13;
		inputPanel.add(lblModifyQueryOf, gbc_lblModifyQueryOf);
		GridBagConstraints gbc_btnShowHint = new GridBagConstraints();
		gbc_btnShowHint.anchor = GridBagConstraints.WEST;
		btnShowHint.setFont(new Font("Tahoma", Font.BOLD, 11));
		gbc_btnShowHint.fill = GridBagConstraints.VERTICAL;
		gbc_btnShowHint.insets = new Insets(5, 0, 5, 5);
		gbc_btnShowHint.gridx = 4;
		gbc_btnShowHint.gridy = 14;
		inputPanel.add(btnShowHint, gbc_btnShowHint);

		btnGetQuestionSparql = new JButton();
		btnGetQuestionSparql.setEnabled(false);
		// btnGetQuestionSparql.setText("Fill Current Query");
		btnGetQuestionSparql.setToolTipText("Display Sparql Query for Current Question");
		try {
			Image img = ImageIO.read(new File(workingDir
					+ "/pictures/download.png"));
			Image newimg = img.getScaledInstance(15, 15,
					java.awt.Image.SCALE_SMOOTH);
			btnGetQuestionSparql.setIcon(new ImageIcon(newimg));
		} catch (IOException ex) {
		}
		GridBagConstraints gbc_btnGetQuestionSparql = new GridBagConstraints();
		gbc_btnGetQuestionSparql.anchor = GridBagConstraints.WEST;
		btnGetQuestionSparql.setFont(new Font("Tahoma", Font.BOLD, 11));
		gbc_btnGetQuestionSparql.fill = GridBagConstraints.VERTICAL;
		gbc_btnGetQuestionSparql.insets = new Insets(5, 0, 5, 5);
		gbc_btnGetQuestionSparql.gridx = 3;
		gbc_btnGetQuestionSparql.gridy = 14;
		inputPanel.add(btnGetQuestionSparql, gbc_btnGetQuestionSparql);

		playSheetComboBox = new JComboBox();
		playSheetComboBox.setEnabled(false);
		playSheetComboBox.setFont(new Font("Tahoma", Font.PLAIN, 11));
		playSheetComboBox.setBackground(new Color(119, 136, 153));
		playSheetComboBox.setMinimumSize(new Dimension(125, 25));
		playSheetComboBox.setPreferredSize(new Dimension(125, 25));
		// entries in combobox specified in question listener
		GridBagConstraints gbc_playSheetComboBox = new GridBagConstraints();
		gbc_playSheetComboBox.fill = GridBagConstraints.HORIZONTAL;
		gbc_playSheetComboBox.gridwidth = 2;
		gbc_playSheetComboBox.anchor = GridBagConstraints.NORTH;
		gbc_playSheetComboBox.insets = new Insets(5, 5, 5, 5);
		gbc_playSheetComboBox.gridx = 1;
		gbc_playSheetComboBox.gridy = 14;
		inputPanel.add(playSheetComboBox, gbc_playSheetComboBox);
		playSheetComboBox.setEnabled(false);

		sparqlArea = new SparqlArea();
		sparqlArea.setEnabled(false);
		sparqlArea.setFont(new Font("Tahoma", Font.PLAIN, 11));
		// ctrl-z/ctrl-y in sparql box
		sparqlArea.getDocument().addUndoableEditListener(new TextUndoListener(sparqlArea));

		JScrollPane scrollPane_1 = new JScrollPane(sparqlArea);
		GridBagConstraints gbc_scrollPane_1 = new GridBagConstraints();
		gbc_scrollPane_1.insets = new Insets(5, 0, 0, 5);
		gbc_scrollPane_1.fill = GridBagConstraints.BOTH;
		gbc_scrollPane_1.gridwidth = 4;
		gbc_scrollPane_1.gridx = 1;
		gbc_scrollPane_1.gridy = 15;
		inputPanel.add(scrollPane_1, gbc_scrollPane_1);

		sparqlArea.setColumns(12);
		sparqlArea.setLineWrap(true);
		sparqlArea.setWrapStyleWord(true);
		sparqlArea.setEnabled(false);
		scrollPane_1.setPreferredSize(new Dimension(80, 40));

		owlPanel = new JPanel();
		owlPanel.setBackground(SystemColor.control);
		leftView.addTab("SUDOWL", null, owlPanel, null);
		GridBagLayout gbl_owlPanel = new GridBagLayout();
		gbl_owlPanel.columnWidths = new int[] { 228, 0 };
		gbl_owlPanel.rowHeights = new int[] { 29, 0, 0, 0, 0, 0, 0, 0, 0 };
		gbl_owlPanel.columnWeights = new double[] { 1.0, Double.MIN_VALUE };
		gbl_owlPanel.rowWeights = new double[] { 0.0, 1.0, 0.0, 0.0, 1.0, 0.0,0.0, 0.0, Double.MIN_VALUE };
		owlPanel.setLayout(gbl_owlPanel);

		JLabel lblDataProperties = new JLabel("Data Properties");
		lblDataProperties.setFont(new Font("Tahoma", Font.BOLD, 11));
		lblDataProperties.setHorizontalAlignment(SwingConstants.CENTER);
		GridBagConstraints gbc_lblDataProperties = new GridBagConstraints();
		gbc_lblDataProperties.anchor = GridBagConstraints.WEST;
		gbc_lblDataProperties.insets = new Insets(0, 0, 5, 0);
		gbc_lblDataProperties.gridx = 0;
		gbc_lblDataProperties.gridy = 0;
		owlPanel.add(lblDataProperties, gbc_lblDataProperties);

		JScrollPane scrollPane_8 = new JScrollPane();
		scrollPane_8.setPreferredSize(new Dimension(150, 350));
		scrollPane_8.setMinimumSize(new Dimension(150, 350));
		scrollPane_8.setMaximumSize(new Dimension(150, 350));
		GridBagConstraints gbc_scrollPane_8 = new GridBagConstraints();
		gbc_scrollPane_8.fill = GridBagConstraints.HORIZONTAL;
		gbc_scrollPane_8.insets = new Insets(0, 0, 5, 0);
		gbc_scrollPane_8.gridx = 0;
		gbc_scrollPane_8.gridy = 1;
		owlPanel.add(scrollPane_8, gbc_scrollPane_8);

		dataPropertiesTable = new JTable();
		dataPropertiesTable.setAutoResizeMode(JTable.AUTO_RESIZE_LAST_COLUMN);
		scrollPane_8.setViewportView(dataPropertiesTable);
		dataPropertiesTable.setFillsViewportHeight(true);
		dataPropertiesTable.setShowGrid(true);
		dataPropertiesTable.setShowHorizontalLines(true);
		dataPropertiesTable.setShowVerticalLines(true);

		dataPropertiesString = new JTextField();
		dataPropertiesString.setText(DIHelper.getInstance().getProperty(Constants.PROP_URI));
		GridBagConstraints gbc_dataPropertiesString = new GridBagConstraints();
		gbc_dataPropertiesString.insets = new Insets(0, 0, 5, 0);
		gbc_dataPropertiesString.fill = GridBagConstraints.HORIZONTAL;
		gbc_dataPropertiesString.gridx = 0;
		gbc_dataPropertiesString.gridy = 2;
		owlPanel.add(dataPropertiesString, gbc_dataPropertiesString);
		dataPropertiesString.setColumns(10);
		// add the routine to do the predicate and properties

		JLabel lblObjectProperties = new JLabel("Object Properties");
		lblObjectProperties.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_lblObjectProperties = new GridBagConstraints();
		gbc_lblObjectProperties.anchor = GridBagConstraints.WEST;
		gbc_lblObjectProperties.insets = new Insets(0, 0, 5, 0);
		gbc_lblObjectProperties.gridx = 0;
		gbc_lblObjectProperties.gridy = 3;
		owlPanel.add(lblObjectProperties, gbc_lblObjectProperties);

		JScrollPane scrollPane_7 = new JScrollPane();
		scrollPane_7.setPreferredSize(new Dimension(150, 350));
		scrollPane_7.setMinimumSize(new Dimension(150, 350));
		scrollPane_7.setMaximumSize(new Dimension(150, 350));
		GridBagConstraints gbc_scrollPane_7 = new GridBagConstraints();
		gbc_scrollPane_7.fill = GridBagConstraints.BOTH;
		gbc_scrollPane_7.insets = new Insets(0, 0, 5, 0);
		gbc_scrollPane_7.gridx = 0;
		gbc_scrollPane_7.gridy = 4;
		owlPanel.add(scrollPane_7, gbc_scrollPane_7);

		objectPropertiesTable = new JTable();
		scrollPane_7.setViewportView(objectPropertiesTable);
		objectPropertiesTable.setShowGrid(true);
		objectPropertiesTable.setShowHorizontalLines(true);
		objectPropertiesTable.setShowVerticalLines(true);

		objectPropertiesString = new JTextField();
		objectPropertiesString.setText(DIHelper.getInstance().getProperty(Constants.PREDICATE_URI));

		GridBagConstraints gbc_objectPropertiesString = new GridBagConstraints();
		gbc_objectPropertiesString.anchor = GridBagConstraints.BELOW_BASELINE;
		gbc_objectPropertiesString.insets = new Insets(0, 0, 5, 0);
		gbc_objectPropertiesString.fill = GridBagConstraints.HORIZONTAL;
		gbc_objectPropertiesString.gridx = 0;
		gbc_objectPropertiesString.gridy = 5;
		owlPanel.add(objectPropertiesString, gbc_objectPropertiesString);
		objectPropertiesString.setColumns(10);

		btnRepaintGraph = new CustomButton("Refresh");
		btnRepaintGraph.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_btnRepaintGraph = new GridBagConstraints();
		gbc_btnRepaintGraph.insets = new Insets(0, 0, 5, 0);
		gbc_btnRepaintGraph.gridx = 0;
		gbc_btnRepaintGraph.gridy = 6;
		owlPanel.add(btnRepaintGraph, gbc_btnRepaintGraph);

		GridBagConstraints gbc_table_2;
		// scrollPane_1.setViewportView(sparqlArea);

		JPanel outputPanel = new JPanel();
		outputPanel.setBackground(SystemColor.control);
		leftView.addTab("Graph Labels", null, outputPanel, null);
		GridBagLayout gbl_outputPanel = new GridBagLayout();
		gbl_outputPanel.columnWidths = new int[] { 231 };
		gbl_outputPanel.rowHeights = new int[] { 0, 0, 350, 0, 350, 150 };
		gbl_outputPanel.columnWeights = new double[] { 1.0 };
		gbl_outputPanel.rowWeights = new double[] { 0.0, 0.0, 1.0, 0.0, 1.0, Double.MIN_VALUE };
		outputPanel.setLayout(gbl_outputPanel);

		JLabel lblLabelDisplay = new JLabel("Label Display");
		lblLabelDisplay.setFont(new Font("Tahoma", Font.BOLD, 12));
		lblLabelDisplay.setForeground(Color.BLACK);
		lblLabelDisplay.setBackground(Color.BLACK);
		GridBagConstraints gbc_lblLabelDisplay = new GridBagConstraints();
		gbc_lblLabelDisplay.insets = new Insets(0, 0, 5, 0);
		gbc_lblLabelDisplay.gridx = 0;
		gbc_lblLabelDisplay.gridy = 1;
		outputPanel.add(lblLabelDisplay, gbc_lblLabelDisplay);

		JScrollPane scrollPane_3 = new JScrollPane((Component) null);
		scrollPane_3.setPreferredSize(new Dimension(150, 350));
		scrollPane_3.setMinimumSize(new Dimension(150, 350));
		scrollPane_3.setMaximumSize(new Dimension(150, 350));
		scrollPane_3.setForeground(Color.GRAY);
		scrollPane_3.setBorder(null);
		scrollPane_3.setBackground(Color.WHITE);
		GridBagConstraints gbc_scrollPane_3 = new GridBagConstraints();
		gbc_scrollPane_3.fill = GridBagConstraints.BOTH;
		gbc_scrollPane_3.insets = new Insets(0, 0, 5, 0);
		gbc_scrollPane_3.gridx = 0;
		gbc_scrollPane_3.gridy = 2;
		outputPanel.add(scrollPane_3, gbc_scrollPane_3);

		labelTable = new JTable();
		labelTable.setAutoResizeMode(JTable.AUTO_RESIZE_LAST_COLUMN);
		scrollPane_3.setViewportView(labelTable);
		labelTable.setShowGrid(true);
		labelTable.setShowHorizontalLines(true);
		labelTable.setShowVerticalLines(true);

		JLabel lblDisplayTooltip = new JLabel("Tooltip Display");
		lblDisplayTooltip.setFont(new Font("Tahoma", Font.BOLD, 12));
		lblDisplayTooltip.setBackground(Color.BLACK);
		lblDisplayTooltip.setForeground(Color.BLACK);
		GridBagConstraints gbc_lblDisplayTooltip = new GridBagConstraints();
		gbc_lblDisplayTooltip.insets = new Insets(0, 0, 5, 0);
		gbc_lblDisplayTooltip.gridx = 0;
		gbc_lblDisplayTooltip.gridy = 3;
		outputPanel.add(lblDisplayTooltip, gbc_lblDisplayTooltip);

		JScrollPane scrollPane_6 = new JScrollPane();
		scrollPane_6.setMaximumSize(new Dimension(150, 350));
		scrollPane_6.setPreferredSize(new Dimension(150, 350));
		scrollPane_6.setMinimumSize(new Dimension(150, 350));
		GridBagConstraints gbc_scrollPane_6 = new GridBagConstraints();
		gbc_scrollPane_6.fill = GridBagConstraints.BOTH;
		gbc_scrollPane_6.gridx = 0;
		gbc_scrollPane_6.gridy = 4;
		outputPanel.add(scrollPane_6, gbc_scrollPane_6);

		tooltipTable = new JTable();
		scrollPane_6.setViewportView(tooltipTable);
		tooltipTable.setShowGrid(true);
		tooltipTable.setShowHorizontalLines(true);
		tooltipTable.setShowVerticalLines(true);

		JPanel filterPanel = new JPanel();
		filterPanel.setBackground(SystemColor.control);
		leftView.addTab("Graph Filter", null, filterPanel, null);
		GridBagLayout gbl_filterPanel = new GridBagLayout();
		gbl_filterPanel.columnWidths = new int[] { 239, 0 };
		gbl_filterPanel.rowHeights = new int[] { 44, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
		gbl_filterPanel.columnWeights = new double[] { 1.0, Double.MIN_VALUE };
		gbl_filterPanel.rowWeights = new double[] { 1.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE };
		filterPanel.setLayout(gbl_filterPanel);

		filterTable = new JTable();
		filterTable.setShowGrid(true);
		filterTable.setShowHorizontalLines(true);
		filterTable.setShowVerticalLines(true);

		JScrollPane scrollPane_2 = new JScrollPane(filterTable);
		GridBagConstraints gbc_scrollPane_2 = new GridBagConstraints();
		gbc_scrollPane_2.gridheight = 2;
		gbc_scrollPane_2.insets = new Insets(0, 0, 5, 0);
		gbc_scrollPane_2.fill = GridBagConstraints.BOTH;
		gbc_scrollPane_2.gridx = 0;
		gbc_scrollPane_2.gridy = 0;
		filterPanel.add(scrollPane_2, gbc_scrollPane_2);

		filterSliders = new JScrollPane();
		GridBagConstraints gbc_filterSliders = new GridBagConstraints();
		gbc_filterSliders.insets = new Insets(0, 0, 5, 0);
		gbc_filterSliders.fill = GridBagConstraints.BOTH;
		gbc_filterSliders.gridx = 0;
		gbc_filterSliders.gridy = 2;
		filterPanel.add(filterSliders, gbc_filterSliders);

		propertyTable = new JTable();
		filterSliders.setViewportView(propertyTable);
		propertyTable.setShowGrid(true);
		propertyTable.setShowHorizontalLines(true);
		propertyTable.setShowVerticalLines(true);

		JScrollPane scrollPane_4 = new JScrollPane();
		GridBagConstraints gbc_scrollPane_4 = new GridBagConstraints();
		gbc_scrollPane_4.insets = new Insets(0, 0, 5, 0);
		gbc_scrollPane_4.fill = GridBagConstraints.BOTH;
		gbc_scrollPane_4.gridx = 0;
		gbc_scrollPane_4.gridy = 3;
		filterPanel.add(scrollPane_4, gbc_scrollPane_4);

		edgeTable = new JTable();
		scrollPane_4.setViewportView(edgeTable);
		edgeTable.setShowGrid(true);
		edgeTable.setShowHorizontalLines(true);
		edgeTable.setShowVerticalLines(true);

		// scrollPane_2.setColumnHeaderView(filterTable);

		refreshButton = new CustomButton("Refresh Graph");
		refreshButton.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_refreshGraph = new GridBagConstraints();
		gbc_refreshGraph.insets = new Insets(0, 0, 5, 0);
		gbc_refreshGraph.gridx = 0;
		gbc_refreshGraph.gridy = 6;
		filterPanel.add(refreshButton, gbc_refreshGraph);

		JPanel cosmeticPanel = new JPanel();
		cosmeticPanel.setBackground(SystemColor.control);
		leftView.addTab("Graph Cosmetics", null, cosmeticPanel, null);
		GridBagLayout gbl_cosmeticPanel = new GridBagLayout();
		gbl_cosmeticPanel.columnWidths = new int[] { 231 };
		gbl_cosmeticPanel.rowHeights = new int[] { 0, 0, 350, 0, 350, 0, 0, 0,	150 };
		gbl_cosmeticPanel.columnWeights = new double[] { 1.0 };
		gbl_cosmeticPanel.rowWeights = new double[] { 0.0, 0.0, 1.0, 0.0, 1.0,	0.0, 1.0, 0.0, Double.MIN_VALUE };
		cosmeticPanel.setLayout(gbl_cosmeticPanel);

		JScrollPane scrollPane_9 = new JScrollPane((Component) null);
		scrollPane_9.setPreferredSize(new Dimension(150, 100));
		scrollPane_9.setMinimumSize(new Dimension(0, 0));
		scrollPane_9.setMaximumSize(new Dimension(0, 0));
		GridBagConstraints gbc_scrollPane_9 = new GridBagConstraints();
		gbc_scrollPane_9.fill = GridBagConstraints.BOTH;
		gbc_scrollPane_9.insets = new Insets(0, 0, 5, 0);
		gbc_scrollPane_9.gridx = 0;
		gbc_scrollPane_9.gridy = 0;
		cosmeticPanel.add(scrollPane_9, gbc_scrollPane_9);

		JLabel lblColorAndShape = new JLabel("Color and Shape");
		lblColorAndShape.setForeground(Color.DARK_GRAY);
		lblColorAndShape.setFont(new Font("Tahoma", Font.BOLD, 12));
		lblColorAndShape.setBackground(Color.BLACK);
		GridBagConstraints gbc_lblColorAndShape = new GridBagConstraints();
		gbc_lblColorAndShape.insets = new Insets(0, 0, 5, 0);
		gbc_lblColorAndShape.gridx = 0;
		gbc_lblColorAndShape.gridy = 1;
		cosmeticPanel.add(lblColorAndShape, gbc_lblColorAndShape);

		JScrollPane scrollPane_10 = new JScrollPane((Component) null);
		scrollPane_10.setPreferredSize(new Dimension(150, 350));
		scrollPane_10.setMinimumSize(new Dimension(150, 350));
		scrollPane_10.setMaximumSize(new Dimension(150, 350));
		scrollPane_10.setForeground(Color.GRAY);
		scrollPane_10.setBorder(null);
		scrollPane_10.setBackground(Color.WHITE);
		GridBagConstraints gbc_scrollPane_10 = new GridBagConstraints();
		gbc_scrollPane_10.fill = GridBagConstraints.BOTH;
		gbc_scrollPane_10.insets = new Insets(0, 0, 5, 0);
		gbc_scrollPane_10.gridx = 0;
		gbc_scrollPane_10.gridy = 2;
		cosmeticPanel.add(scrollPane_10, gbc_scrollPane_10);

		colorShapeTable = new JTable();
		scrollPane_10.setViewportView(colorShapeTable);
		colorShapeTable.setShowGrid(true);
		colorShapeTable.setShowHorizontalLines(true);
		colorShapeTable.setShowVerticalLines(true);

		JLabel lblSize = new JLabel("Size");
		lblSize.setForeground(Color.BLACK);
		lblSize.setFont(new Font("Tahoma", Font.BOLD, 12));
		lblSize.setBackground(Color.BLACK);
		GridBagConstraints gbc_lblSize = new GridBagConstraints();
		gbc_lblSize.insets = new Insets(0, 0, 5, 0);
		gbc_lblSize.gridx = 0;
		gbc_lblSize.gridy = 3;
		cosmeticPanel.add(lblSize, gbc_lblSize);

		JScrollPane scrollPane_11 = new JScrollPane();
		scrollPane_11.setPreferredSize(new Dimension(150, 350));
		scrollPane_11.setMinimumSize(new Dimension(150, 350));
		scrollPane_11.setMaximumSize(new Dimension(150, 350));
		GridBagConstraints gbc_scrollPane_11 = new GridBagConstraints();
		gbc_scrollPane_11.insets = new Insets(0, 0, 5, 0);
		gbc_scrollPane_11.fill = GridBagConstraints.BOTH;
		gbc_scrollPane_11.gridx = 0;
		gbc_scrollPane_11.gridy = 4;
		cosmeticPanel.add(scrollPane_11, gbc_scrollPane_11);

		sizeTable = new JTable();
		scrollPane_11.setViewportView(sizeTable);

		btnColorShape = new CustomButton("Refresh Graph");
		btnColorShape.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_btnColorShape = new GridBagConstraints();
		gbc_btnColorShape.insets = new Insets(0, 0, 5, 0);
		gbc_btnColorShape.gridx = 0;
		gbc_btnColorShape.gridy = 5;
		cosmeticPanel.add(btnColorShape, gbc_btnColorShape);
		btnResetDefaults = new CustomButton("Reset Defaults");
		btnResetDefaults.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_btnResetDefaults = new GridBagConstraints();
		gbc_btnResetDefaults.gridx = 0;
		gbc_btnResetDefaults.gridy = 7;
		cosmeticPanel.add(btnResetDefaults, gbc_btnResetDefaults);
		splitPane.setDividerLocation(250);
		GridBagConstraints gbc_splitPane = new GridBagConstraints();
		gbc_splitPane.fill = GridBagConstraints.BOTH;
		gbc_splitPane.gridx = 0;
		gbc_splitPane.gridy = 0;
		getContentPane().add(splitPane, gbc_splitPane);
		// UIManager.put("nimbusBase", Color.BLUE);
		// UIManager.put("nimbusBlueGrey", new Color(102,0,0));
		// UIManager.put("control", Color.WHITE);
		SwingStyle.init(); // for swing rules and functions
		CustomAruiStyle.init(); // for custom components rules and functions
		// Components to style
		// Style.registerTargetClassName(lblLearningMaterials, ".label");
		Style.registerTargetClassName(submitButton, ".createBtn");
		Style.registerTargetClassName(btnCustomSparql, ".toggleButton");
		Style.registerTargetClassName(btnGetQuestionSparql, ".standardButton");
		Style.registerTargetClassName(btnShowHint, ".standardButton");
		Style.registerTargetClassName(btnInsertBudgetProperty, ".standardButton");
		Style.registerTargetClassName(btnInsertServiceProperties, ".standardButton");
		Style.registerTargetClassName(btnRepaintGraph, ".standardButton");

		saveSudowl = new CustomButton("Refresh");
		saveSudowl.setText("Save");
		saveSudowl.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_saveSudowl = new GridBagConstraints();
		gbc_saveSudowl.gridx = 0;
		gbc_saveSudowl.gridy = 7;
		owlPanel.add(saveSudowl, gbc_saveSudowl);
		Style.registerTargetClassName(btnColorShape, ".standardButton");
		Style.registerTargetClassName(btnResetDefaults, ".standardButton");
		Style.registerTargetClassName(refreshButton, ".standardButton");
		Style.registerTargetClassName(calcTransAdditionalButton, ".standardButton");
		Style.registerTargetClassName(calculateTransitionCostsButton, ".standardButton");
		Style.registerTargetClassName(transitionReportGenButton, ".standardButton");
		Style.registerTargetClassName(serviceSelectionBtn, ".toggleButton");
		Style.registerTargetClassName(btnAdvancedFinancialFunctions, ".toggleButton");
		Style.registerTargetClassName(saveSudowl, ".standardButton");
		Style.registerTargetClassName(pptTrainingBtn, ".standardButton");
		Style.registerTargetClassName(htmlTrainingBtn, ".standardButton");
		CSSApplication css = new CSSApplication(getContentPane());

		scrollPane_1.getVerticalScrollBar().setUI(new NewScrollBarUI());
		scrollPane_2.getVerticalScrollBar().setUI(new NewScrollBarUI());
		scrollPane_3.getVerticalScrollBar().setUI(new NewScrollBarUI());
		scrollPane_4.getVerticalScrollBar().setUI(new NewScrollBarUI());
		scrollPane_6.getVerticalScrollBar().setUI(new NewScrollBarUI());
		scrollPane_7.getVerticalScrollBar().setUI(new NewScrollBarUI());
		scrollPane_8.getVerticalScrollBar().setUI(new NewScrollBarUI());
		scrollPane_9.getVerticalScrollBar().setUI(new NewScrollBarUI());
		scrollPane_10.getVerticalScrollBar().setUI(new NewScrollBarUI());
		scrollPane_11.getVerticalScrollBar().setUI(new NewScrollBarUI());
		filterSliders.getVerticalScrollBar().setUI(new NewScrollBarUI());
	}
}
