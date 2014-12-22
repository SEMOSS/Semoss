/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.ui.components.playsheets;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.ComponentOrientation;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.beans.PropertyVetoException;
import java.util.ArrayList;

import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.Painter;
import javax.swing.ScrollPaneConstants;
import javax.swing.UIDefaults;
import javax.swing.UIManager;
import javax.swing.border.LineBorder;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.NewScrollBarUI;
import prerna.ui.components.ParamComboBox;
import prerna.ui.helpers.EntityFillerForSubClass;
import prerna.ui.main.listener.impl.GridPlaySheetListener;
import prerna.ui.main.listener.impl.PlaySheetListener;
import prerna.ui.main.listener.impl.RegressionAnalysisButtonListener;
import prerna.ui.main.listener.impl.RegressionDepVarListener;
import prerna.ui.main.listener.impl.RegressionIndepVarDeleteListener;
import prerna.ui.main.listener.impl.RegressionIndepVarListener;
import prerna.ui.swing.custom.CustomButton;
import prerna.ui.swing.custom.ProgressPainter;
import prerna.util.CSSApplication;
import aurelienribon.ui.css.Style;

import com.hp.hpl.jena.query.ResultSet;

/**
 */
public class RegressionAnalysisPlaySheet extends AbstractRDFPlaySheet{
	private static final Logger logger = LogManager.getLogger(RegressionAnalysisPlaySheet.class.getName());
	public RegressionAnalysisPlaySheet() {
	}

	public JTabbedPane jTab = new JTabbedPane();
	JPanel cheaterPanel = new JPanel();

	JList<String> possibleInputList;
	JTextField depVarTextField;
	JList<String> indepVarList;

	RegressionDepVarListener regDepVarListener;
	RegressionIndepVarListener regIndepVarListener;
	RegressionIndepVarDeleteListener regIndepVarDeleteListener;
	RegressionAnalysisButtonListener regressionAnalysisBtnList;

	JButton selDepVarBtn;
	JButton selIndVarBtn;
	JButton deselIndVarBtn;
	JButton runRegressionAnalysisBtn;
	ParamComboBox nodeSelectorCombo;
	EntityFillerForSubClass entityFillerSC;

	boolean extend = false;
	boolean append = false;
	protected ResultSet rs = null;
	public ArrayList <String> possibleList;
	public SesameJenaSelectWrapper wrapper;

	/**
	 * Method createView.
	 * Creates the UI components necessary for the regression analysis play sheet
	 * and runs the initial query to pull all the properties that regression can be performed on.
	 */
	@Override
	public void createView() {


		addPanel();

		possibleList = new ArrayList();
		DefaultListModel listModel = new DefaultListModel();
		for(String listElement : possibleList)
		{
			listModel.addElement(listElement);
		}
		possibleInputList.setModel(listModel);
		ArrayList<JComboBox> boxes = new ArrayList<JComboBox>();
		boxes.add(nodeSelectorCombo);

		entityFillerSC = new EntityFillerForSubClass();
		entityFillerSC.boxes = boxes;
		entityFillerSC.engine = engine;
		entityFillerSC.parent = "Concept";
		Thread aThread = new Thread(entityFillerSC);
		aThread.start();

	}

	public void resetInputList()
	{

		DefaultListModel listModel = (DefaultListModel)possibleInputList.getModel();
		listModel.removeAllElements();

		depVarTextField.setText("");
		DefaultListModel indVarListModel  = (DefaultListModel)indepVarList.getModel();
		indVarListModel.removeAllElements();
		possibleList = new ArrayList();

		wrapper = new SesameJenaSelectWrapper();
		if(engine!= null && rs == null){
			String selected = (String) nodeSelectorCombo.getSelectedItem();
			String nodeType = nodeSelectorCombo.getURI(selected);
			query = "SELECT DISTINCT ?property WHERE{{?node <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <"+nodeType+"> ;} {?node ?property ?Value}{?property <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> ;}} ORDER BY ?property";

			wrapper.setQuery(query);
			updateProgressBar("10%...Querying RDF Repository", 10);
			wrapper.setEngine(engine);
			updateProgressBar("30%...Querying RDF Repository", 30);
			try{
				wrapper.executeQuery();	
			}
			catch (RuntimeException e)
			{
				UIDefaults nimbusOverrides = new UIDefaults();
				UIDefaults defaults = UIManager.getLookAndFeelDefaults();
				defaults.put("nimbusOrange",defaults.get("nimbusInfoBlue"));
				Painter red = new ProgressPainter(Color.WHITE, Color.RED);
				nimbusOverrides.put("ProgressBar[Enabled].foregroundPainter",red);
				jBar.putClientProperty("Nimbus.Overrides", nimbusOverrides);
				jBar.putClientProperty("Nimbus.Overrides.InheritDefaults", false);
				updateProgressBar("An error has occurred. Please check the query.", 100);
				return;
			}		

			updateProgressBar("60%...Processing RDF Statements	", 60);
		}
		else if (engine==null && rs!=null){
			wrapper.setResultSet(rs);
			wrapper.setEngineType(IEngine.ENGINE_TYPE.JENA);
		}
		String [] names = wrapper.getVariables();

		int count = 0;
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				SesameJenaSelectStatement sjss = wrapper.next();
				possibleList.add((String)sjss.getVar(names[0]));
				count++;
			}			

		} catch (RuntimeException e) {
			logger.fatal(e);
		}
		//listModel = new DefaultListModel();
		for(String listElement : possibleList)
		{
			listModel.addElement(listElement);
		}
		possibleInputList.setModel(listModel);
		possibleInputList.setVisibleRowCount(count+1);
		possibleInputList.repaint();

		updateProgressBar("80%...Creating Visualization", 80);
		updateProgressBar("100%...Table Generation Complete", 100);
	}


	/**
	 * Method setRs.
	 * @param rs ResultSet
	 */
	public void setRs(ResultSet rs) {
		this.rs = rs;
	}

	/**
	 * Method getVariable.
	 * @param varName String
	 * @param sjss SesameJenaSelectStatement

	 * @return Object */
	public Object getVariable(String varName, SesameJenaSelectStatement sjss){
		return sjss.getVar(varName);
	}

	/**
	 * Method refineView.
	 */
	@Override
	public void refineView() {
		// TODO Auto-generated method stub
		// this is easy
		// just use the filter to not show stuff I dont need to show
		// but this also means I need to create the vertex filter data etc. 
		//

	}
	/**
	 * Method overlayView.
	 */
	@Override
	public void overlayView() {
		//Fill
	}

	/**
	 * Method addPanel.
	 * Creates the main panel that will hold the list of properties to choose from
	 * and the dependent variables and regressors selected.
	 */
	public void addPanel()
	{	
		
		
		setWindow();
		try {
			JPanel mainPanel = new JPanel();
			PlaySheetListener psListener = new PlaySheetListener();
			this.addInternalFrameListener(psListener);
			logger.debug("Added the internal frame listener ");
			
			this.setContentPane(mainPanel);
			
			GridBagLayout gridBagLayout = new GridBagLayout();
			gridBagLayout.columnWidths = new int[]{728, 0};
			gridBagLayout.rowHeights = new int[]{607, 20, 0};
			gridBagLayout.columnWeights = new double[]{1.0, Double.MIN_VALUE};
			gridBagLayout.rowWeights = new double[]{1.0, 0.0, Double.MIN_VALUE};
			mainPanel.setLayout(gridBagLayout);//getContentPane().setLayout(gridBagLayout);

			
			cheaterPanel.setPreferredSize(new Dimension(800, 20));//probably change to 20
			GridBagLayout gbl_cheaterPanel = new GridBagLayout();
			gbl_cheaterPanel.columnWidths = new int[]{0, 0};
			gbl_cheaterPanel.rowHeights = new int[]{20, 0};
			gbl_cheaterPanel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
			gbl_cheaterPanel.rowWeights = new double[]{1.0, Double.MIN_VALUE};
			cheaterPanel.setLayout(gbl_cheaterPanel);			
			GridBagConstraints gbc_cheaterPanel = new GridBagConstraints();
			gbc_cheaterPanel.anchor = GridBagConstraints.NORTH;
			gbc_cheaterPanel.fill = GridBagConstraints.HORIZONTAL;
			gbc_cheaterPanel.gridx = 0;
			gbc_cheaterPanel.gridy = 1;
			mainPanel.add(cheaterPanel,gbc_cheaterPanel);
			
			
			jBar.setStringPainted(true);
			jBar.setString("0%...Preprocessing");
			jBar.setValue(0);
			resetProgressBar();
			GridBagConstraints gbc_jBar = new GridBagConstraints();
			gbc_jBar.anchor = GridBagConstraints.NORTH;
			gbc_jBar.fill = GridBagConstraints.HORIZONTAL;
			gbc_jBar.gridx = 0;
			gbc_jBar.gridy = 1;
			mainPanel.add(jBar,gbc_jBar);
			
			GridBagConstraints gbc_jTab = new GridBagConstraints();
			gbc_jTab.anchor = GridBagConstraints.NORTH;
			gbc_jTab.fill = GridBagConstraints.BOTH;
			gbc_jTab.gridx = 0;
			gbc_jTab.gridy = 0;
			mainPanel.add(jTab,gbc_jTab);
			
			
			JPanel regPanel = new JPanel();
			GridBagConstraints gbc_regPanel = new GridBagConstraints();
			gbc_regPanel.fill = GridBagConstraints.BOTH;
			gbc_regPanel.gridx = 0;
			gbc_regPanel.gridy = 0;
			jTab.insertTab("Select Variables", null, regPanel, null, 0);
			GridBagLayout gbl_regPanel = new GridBagLayout();
			gbl_regPanel.columnWidths = new int[]{0, 0};
			gbl_regPanel.rowHeights = new int[]{0, 0};
			gbl_regPanel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
			gbl_regPanel.rowWeights = new double[]{1.0, Double.MIN_VALUE};
			regPanel.setLayout(gbl_regPanel);
	
			JLabel nodeTypeLabel = new JLabel("Select Node Type:");
			nodeTypeLabel.setFont(new Font("Tahoma", Font.BOLD, 12));
			GridBagConstraints gbc_nodeTypeLabel = new GridBagConstraints();
			gbc_nodeTypeLabel.insets= new Insets(10,10,0,0);
			gbc_nodeTypeLabel.anchor= GridBagConstraints.WEST;
			//possibleListLabel.fill= GridBagConstraints.BOTH;
			gbc_nodeTypeLabel.gridx = 0;
			gbc_nodeTypeLabel.gridy = 0;
			regPanel.add(nodeTypeLabel,gbc_nodeTypeLabel);
	
			nodeSelectorCombo = new ParamComboBox(new String[0]);
			nodeSelectorCombo.setName("NodeType");
			nodeSelectorCombo.setFont(new Font("Tahoma", Font.PLAIN, 11));
			nodeSelectorCombo.setBackground(Color.GRAY);
			GridBagConstraints gbc_nodeSelectorCombo = new GridBagConstraints();
			gbc_nodeSelectorCombo.fill = GridBagConstraints.HORIZONTAL;
			gbc_nodeSelectorCombo.insets = new Insets(0, 0, 5, 5);
			gbc_nodeSelectorCombo.gridx = 0;
			gbc_nodeSelectorCombo.gridy = 1;
			regPanel.add(nodeSelectorCombo, gbc_nodeSelectorCombo);
			nodeSelectorCombo.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e) {
					resetInputList();
				}
			});
			
			JLabel possibleListLabel = new JLabel("Properties:");
			possibleListLabel.setFont(new Font("Tahoma", Font.BOLD, 12));
			GridBagConstraints gbc_possibleListLabel = new GridBagConstraints();
			gbc_possibleListLabel.insets= new Insets(10,10,0,0);
			gbc_possibleListLabel.anchor= GridBagConstraints.WEST;
			//possibleListLabel.fill= GridBagConstraints.BOTH;
			gbc_possibleListLabel.gridx = 0;
			gbc_possibleListLabel.gridy = 2;
			regPanel.add(possibleListLabel,gbc_possibleListLabel);
	
			possibleInputList = new JList<String>();
			possibleInputList.setFont(new Font("Tahoma", Font.PLAIN, 11));
			possibleInputList.setBorder(new LineBorder(Color.LIGHT_GRAY));
			possibleInputList.setComponentOrientation(ComponentOrientation.LEFT_TO_RIGHT);
			possibleInputList.setLayoutOrientation(JList.HORIZONTAL_WRAP);
			GridBagConstraints gbc_possibleInputList = new GridBagConstraints();
			gbc_possibleInputList.gridheight = 3;
			gbc_possibleInputList.anchor = GridBagConstraints.NORTH;
			gbc_possibleInputList.fill = GridBagConstraints.BOTH;
			gbc_possibleInputList.insets = new Insets(5, 10, 5, 10);
			gbc_possibleInputList.gridx = 0;
			gbc_possibleInputList.gridy = 3;
			JScrollPane listScrollPane = new JScrollPane(possibleInputList);
			listScrollPane.setPreferredSize(new Dimension(20, 200));
			listScrollPane.setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_NEVER);
			listScrollPane.getVerticalScrollBar().setUI(new NewScrollBarUI());
			regPanel.add(listScrollPane, gbc_possibleInputList);
	
			selDepVarBtn = new CustomButton("Select Dependent Variable");
			selDepVarBtn.setFont(new Font("Tahoma", Font.BOLD, 11));
			GridBagConstraints gbc_selDepVarBtn = new GridBagConstraints();
			gbc_selDepVarBtn.insets = new Insets(10, 0, 5, 5);
			gbc_selDepVarBtn.gridx = 1;
			gbc_selDepVarBtn.gridy = 3;//
			regPanel.add(selDepVarBtn, gbc_selDepVarBtn);
			Style.registerTargetClassName(selDepVarBtn, ".standardButton");
			regDepVarListener = new RegressionDepVarListener();
			selDepVarBtn.addActionListener(regDepVarListener);
	
			JPanel indepVarButtonPanel = new JPanel();
			GridBagConstraints gbc_indepVarButtonPanel = new GridBagConstraints();
			gbc_indepVarButtonPanel.insets = new Insets(10, 0, 5, 5);
			gbc_indepVarButtonPanel.anchor = GridBagConstraints.CENTER;
			gbc_indepVarButtonPanel.gridheight = 2;
			gbc_indepVarButtonPanel.gridx = 1;
			gbc_indepVarButtonPanel.gridy = 4;//
			indepVarButtonPanel.setLayout(new GridBagLayout());
			regPanel.add(indepVarButtonPanel,gbc_indepVarButtonPanel);
	
			selIndVarBtn = new CustomButton("Select Regressors");
			selIndVarBtn.setFont(new Font("Tahoma", Font.BOLD, 11));
			selIndVarBtn.setAlignmentX(CENTER_ALIGNMENT);
	
	
	
			GridBagConstraints gbc_selIndVarBtn = new GridBagConstraints();
			gbc_selIndVarBtn.insets= new Insets(0,0,50,0);
			gbc_selIndVarBtn.gridx = 0;
			gbc_selIndVarBtn.gridy = 0;
			indepVarButtonPanel.add(selIndVarBtn,gbc_selIndVarBtn);
			Style.registerTargetClassName(selIndVarBtn, ".standardButton");
			regIndepVarListener = new RegressionIndepVarListener();
			selIndVarBtn.addActionListener(regIndepVarListener);
	
			deselIndVarBtn = new CustomButton("Deselect Regressors");
			deselIndVarBtn.setFont(new Font("Tahoma", Font.BOLD, 11));
			deselIndVarBtn.setAlignmentX(CENTER_ALIGNMENT);
	
			GridBagConstraints gbc_deselIndVarBtn = new GridBagConstraints();
			gbc_deselIndVarBtn.insets= new Insets(0,0,50,0);
			gbc_deselIndVarBtn.gridx = 0;
			gbc_deselIndVarBtn.gridy = 1;
			indepVarButtonPanel.add(deselIndVarBtn,gbc_deselIndVarBtn);
	
			Style.registerTargetClassName(deselIndVarBtn, ".standardButton");
			regIndepVarDeleteListener = new RegressionIndepVarDeleteListener();
			deselIndVarBtn.addActionListener(regIndepVarDeleteListener);
	
			JLabel depVariableLabel = new JLabel("Dependent Variable:");
			depVariableLabel.setFont(new Font("Tahoma", Font.BOLD, 12));
			GridBagConstraints gbc_depVariableLabel = new GridBagConstraints();
			gbc_depVariableLabel.insets= new Insets(10,10,0,0);
			gbc_depVariableLabel.anchor= GridBagConstraints.WEST;
			//possibleListLabel.fill= GridBagConstraints.BOTH;
			gbc_depVariableLabel.gridx = 2;
			gbc_depVariableLabel.gridy = 2;
			regPanel.add(depVariableLabel,gbc_depVariableLabel);
	
			depVarTextField = new JTextField(40);
			depVarTextField.setFont(new Font("Tahoma", Font.PLAIN, 11));
			depVarTextField.setBorder(new LineBorder(Color.LIGHT_GRAY));
			depVarTextField.setComponentOrientation(ComponentOrientation.LEFT_TO_RIGHT);
			depVarTextField.setEditable(false);
			depVarTextField.setPreferredSize(new Dimension(20,40));
			GridBagConstraints gbc_depVarTextField = new GridBagConstraints();
			gbc_depVarTextField.gridheight = 1;
			gbc_depVarTextField.anchor = GridBagConstraints.NORTH;
			gbc_depVarTextField.fill = GridBagConstraints.HORIZONTAL;
			gbc_depVarTextField.insets = new Insets(8, 10, 5, 10);
			gbc_depVarTextField.gridx = 2;
			gbc_depVarTextField.gridy = 3;//
			regPanel.add(depVarTextField, gbc_depVarTextField);
	
			JLabel indepVariableLabel = new JLabel("Regressors:");
			indepVariableLabel.setFont(new Font("Tahoma", Font.BOLD, 12));
			GridBagConstraints gbc_indepVariableLabel = new GridBagConstraints();
			gbc_indepVariableLabel.insets= new Insets(10,10,0,0);
			gbc_indepVariableLabel.anchor= GridBagConstraints.WEST;
			//possibleListLabel.fill= GridBagConstraints.BOTH;
			gbc_indepVariableLabel.gridx = 2;
			gbc_indepVariableLabel.gridy = 4;
			regPanel.add(indepVariableLabel,gbc_indepVariableLabel);
	
			indepVarList = new JList<String>();
			indepVarList.setFont(new Font("Tahoma", Font.PLAIN, 11));
			indepVarList.setBorder(new LineBorder(Color.LIGHT_GRAY));
			indepVarList.setComponentOrientation(ComponentOrientation.LEFT_TO_RIGHT);
			indepVarList.setLayoutOrientation(JList.HORIZONTAL_WRAP);
			GridBagConstraints gbc_indepVarList = new GridBagConstraints();
			//gbc_indepVarList.gridheight = 2;
			gbc_indepVarList.anchor = GridBagConstraints.NORTH;
			gbc_indepVarList.fill = GridBagConstraints.BOTH;
			gbc_indepVarList.insets = new Insets(5, 10, 5, 10);
			gbc_indepVarList.gridx = 2;
			gbc_indepVarList.gridy = 5;//
			JScrollPane indepVarListScrollPane = new JScrollPane(indepVarList);
			indepVarListScrollPane.setPreferredSize(new Dimension(20, 460));
			indepVarListScrollPane.setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_NEVER);
			indepVarListScrollPane.getVerticalScrollBar().setUI(new NewScrollBarUI());
			regPanel.add(indepVarListScrollPane, gbc_indepVarList);
			DefaultListModel indepVarListModel = new DefaultListModel();
			indepVarList.setModel(indepVarListModel);
			indepVarList.repaint();
	
			runRegressionAnalysisBtn = new CustomButton("Run");
			runRegressionAnalysisBtn.setFont(new Font("Tahoma", Font.BOLD, 11));
			GridBagConstraints gbc_runRegressionAnalysisBtn = new GridBagConstraints();
			gbc_runRegressionAnalysisBtn.insets = new Insets(10, 0, 5, 5);
			gbc_runRegressionAnalysisBtn.gridx = 2;
			gbc_runRegressionAnalysisBtn.gridy = 7;
			regPanel.add(runRegressionAnalysisBtn, gbc_runRegressionAnalysisBtn);
			Style.registerTargetClassName(runRegressionAnalysisBtn, ".standardButton");
			regressionAnalysisBtnList = new RegressionAnalysisButtonListener();
			runRegressionAnalysisBtn.addActionListener(regressionAnalysisBtnList);
			
			CSSApplication css = new CSSApplication(mainPanel);
			
			pane.add(this);
			
			this.pack();
			this.setVisible(true);
			this.setSelected(false);
			this.setSelected(true);
			logger.debug("Added the main pane");
			
		} catch (PropertyVetoException e) {
			e.printStackTrace();
		}

	}
	/**
	 * Method addToMainPane.
	 * Adds the pane to the JInternalFrame.
	 * @param pane JComponent
	 * 
	 */
	protected void addToMainPane(JComponent pane) {

		pane.add((Component)this);

		logger.info("Adding Main Panel Complete");
	}
	/**
	 * Method showAll.
	 * Displays all the UI components.
	 */
	public void showAll() {
		this.pack();
		this.setVisible(true);

	}

	@Override
	public void createData() {
		// TODO Auto-generated method stub

	}

	@Override
	public Object getData() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void runAnalytics() {
		// TODO Auto-generated method stub

	}
}
