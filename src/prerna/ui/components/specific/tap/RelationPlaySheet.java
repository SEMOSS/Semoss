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
package prerna.ui.components.specific.tap;

import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.beans.PropertyVetoException;
import java.util.Vector;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComponent;
import javax.swing.JDesktopPane;
import javax.swing.JFrame;
import javax.swing.JInternalFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.JTextField;
import javax.swing.JToggleButton;
import javax.swing.ListSelectionModel;
import javax.swing.border.BevelBorder;
import javax.swing.event.InternalFrameEvent;

import aurelienribon.ui.css.Style;
import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.BrowserGraphPanel;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.helpers.EntityFiller;
import prerna.ui.main.listener.impl.PlaySheetListener;
import prerna.ui.main.listener.specific.tap.AdvParamListener;
import prerna.ui.main.listener.specific.tap.CapCheckBoxSelectorListener;
import prerna.ui.main.listener.specific.tap.CheckBoxSelectorListener;
import prerna.ui.main.listener.specific.tap.OptFunctionRadioBtnListener;
import prerna.ui.main.listener.specific.tap.SysOptBtnListener;
import prerna.ui.main.listener.specific.tap.UpdateDataBLUListListener;
import prerna.ui.swing.custom.CustomButton;
import prerna.ui.swing.custom.SelectScrollList;
import prerna.ui.swing.custom.ToggleButton;
import prerna.util.CSSApplication;
import prerna.util.Constants;
import prerna.util.DIHelper;


/**
 * This is the playsheet used to examine whether a relationship exists between two variables.
 */
public class RelationPlaySheet extends JInternalFrame implements IPlaySheet {
	
	//toggles to show the system Functionality and Capability Functionality panels
	public JToggleButton showSystemSelectBtn, showSystemCapSelectBtn;
	//panel that holds the system,data,blu and capability selectors if needed
	public JPanel systemDataBLUSelectPanel;
	//individual components of the panel
	public DHMSMSystemSelectPanel systemSelectPanel;
	// public DHMSMCapabilitySelectPanel capabilitySelectPanel;
	public DHMSMDataBLUSelectPanel dataBLUSelectPanel;
			
	//toggle to show the data/blu panel (dataBLUSelectPanel) within the systemDataBLUSelectPanel
	//public JToggleButton updateDataBLUPanelButton;
	
	public IEngine engine;
	
	JScrollPane ctlScrollPane;
	JPanel ctlPanel;
	JPanel displayPanel;
	
	public JTabbedPane tabbedPane;
	public JPanel specificFuncAlysPanel;
	
	/**
	 * Constructor for RelationPlaySheet.
	 */
	public RelationPlaySheet()
	{
		// creates user interface
		this.setClosable(true);
		this.setMaximizable(true);
		this.setIconifiable(true);
		this.setResizable(true);
		this.setPreferredSize(new Dimension(800, 600));
	}
	
	public String[] makeListFromQuery(String type, String query)
	{
		EntityFiller filler = new EntityFiller();
		filler.engineName = engine.getEngineName();
		filler.type = "Capability";
		filler.setExternalQuery(query);
		filler.run();
		Vector names = filler.nameVector;
		String[] listArray=new String[names.size()];
		for (int i = 0;i<names.size();i++)
		{
			listArray[i]=(String) names.get(i);
		}
		return listArray;
	}

	public void createGenericParamPanel()
	{
		ctlScrollPane = new JScrollPane();		
		ctlPanel = new JPanel();
		ctlScrollPane.setViewportView(ctlPanel);
		GridBagLayout gbl_ctlPanel = new GridBagLayout();
		gbl_ctlPanel.columnWidths = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0};
		gbl_ctlPanel.rowHeights = new int[]{0, 0, 0, 0, 0, 0, 0};
		gbl_ctlPanel.columnWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0};
		gbl_ctlPanel.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		ctlPanel.setLayout(gbl_ctlPanel);
		
		systemDataBLUSelectPanel = new JPanel();
		systemDataBLUSelectPanel.setBorder(new BevelBorder(BevelBorder.LOWERED, null, null, null, null));
		
		GridBagConstraints gbc_systemDataBLUSelectPanel = new GridBagConstraints();
		gbc_systemDataBLUSelectPanel.gridheight = 6;
		gbc_systemDataBLUSelectPanel.fill = GridBagConstraints.BOTH;
		gbc_systemDataBLUSelectPanel.gridx = 8;
		gbc_systemDataBLUSelectPanel.gridy = 0;
		ctlPanel.add(systemDataBLUSelectPanel, gbc_systemDataBLUSelectPanel);
		
		GridBagLayout gbl_systemDataBLUSelectPanel = new GridBagLayout();
		gbl_systemDataBLUSelectPanel.columnWidths = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0};
		gbl_systemDataBLUSelectPanel.rowHeights = new int[]{0, 0, 0, 0, 0, 0, 0};
		gbl_systemDataBLUSelectPanel.columnWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		gbl_systemDataBLUSelectPanel.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		systemDataBLUSelectPanel.setLayout(gbl_systemDataBLUSelectPanel);
		
		systemSelectPanel = new DHMSMSystemSelectPanel();
		GridBagConstraints gbc_systemSelectPanel = new GridBagConstraints();
		gbc_systemSelectPanel.gridheight = 6;
		gbc_systemSelectPanel.fill = GridBagConstraints.BOTH;
		gbc_systemSelectPanel.gridx = 0;
		gbc_systemSelectPanel.gridy = 0;
		systemDataBLUSelectPanel.add(systemSelectPanel, gbc_systemSelectPanel);
		systemSelectPanel.addElements();
		
//		capabilitySelectPanel = new DHMSMCapabilitySelectPanel();
//		GridBagConstraints gbc_capabilitySelectPanel = new GridBagConstraints();
//		gbc_capabilitySelectPanel.gridheight = 6;
//		gbc_capabilitySelectPanel.fill = GridBagConstraints.BOTH;
//		gbc_capabilitySelectPanel.gridx = 1;
//		gbc_capabilitySelectPanel.gridy = 0;
//		systemDataBLUSelectPanel.add(capabilitySelectPanel, gbc_capabilitySelectPanel);
//		capabilitySelectPanel.addElements();
		
//		updateDataBLUPanelButton = new ToggleButton("View Data/BLU");
//		updateDataBLUPanelButton.setFont(new Font("Tahoma", Font.BOLD, 11));
//		Style.registerTargetClassName(updateDataBLUPanelButton,  ".toggleButton");
//		
//		GridBagConstraints gbc_updateDataBLUPanelButton = new GridBagConstraints();
//		gbc_updateDataBLUPanelButton.anchor = GridBagConstraints.WEST;
//		gbc_updateDataBLUPanelButton.gridheight = 2;
//		gbc_updateDataBLUPanelButton.insets = new Insets(10, 0, 5, 5);
//		gbc_updateDataBLUPanelButton.gridx = 2;
//		gbc_updateDataBLUPanelButton.gridy = 0;
//		systemDataBLUSelectPanel.add(updateDataBLUPanelButton, gbc_updateDataBLUPanelButton);
		
		dataBLUSelectPanel = new DHMSMDataBLUSelectPanel();
		GridBagConstraints gbc_dataBLUSelectPanel = new GridBagConstraints();
		gbc_dataBLUSelectPanel.gridheight = 6;
		gbc_dataBLUSelectPanel.fill = GridBagConstraints.BOTH;
		gbc_dataBLUSelectPanel.gridx = 3;
		gbc_dataBLUSelectPanel.gridy = 0;
		systemDataBLUSelectPanel.add(dataBLUSelectPanel, gbc_dataBLUSelectPanel);
		dataBLUSelectPanel.addElements(systemSelectPanel);
		
		final JComponent contentPane = (JComponent) this.getContentPane();
		contentPane.addMouseListener(new MouseAdapter() {  

			@Override  
			public void mouseClicked(MouseEvent e) {  
				maybeShowPopup(e);  
			}  

			@Override  
			public void mousePressed(MouseEvent e) {  
				maybeShowPopup(e);  
			}  

			@Override  
			public void mouseReleased(MouseEvent e) {  
				maybeShowPopup(e);  
			}  

			private void maybeShowPopup(MouseEvent e) {  
				if (e.isPopupTrigger()) {
				}  
			}  
		});

	}

	public void createListeners()
	{
		UpdateDataBLUListListener updateDataBLUListener = new UpdateDataBLUListListener();
		updateDataBLUListener.setEngine(engine);
		updateDataBLUListener.setUpDHMSMHelper();
		updateDataBLUListener.setComponents(systemSelectPanel,null,dataBLUSelectPanel,showSystemSelectBtn);
		updateDataBLUListener.setUpdateButtons(dataBLUSelectPanel.updateProvideDataBLUButton,dataBLUSelectPanel.updateConsumeDataBLUButton,dataBLUSelectPanel.updateComplementDataBLUButton);
		dataBLUSelectPanel.updateProvideDataBLUButton.addActionListener(updateDataBLUListener);
		dataBLUSelectPanel.updateConsumeDataBLUButton.addActionListener(updateDataBLUListener);
		dataBLUSelectPanel.updateComplementDataBLUButton.addActionListener(updateDataBLUListener);
	}

//	public void addOptimizationBtnListener(JButton btnRunOptimization)
//	{
//		SysOptBtnListener obl = new SysOptBtnListener();
//		obl.setOptPlaySheet(this);
//		btnRunOptimization.addActionListener(obl);
//	}
	
	public void createGenericDisplayPanel()
	{	
		displayPanel = new JPanel();

		GridBagLayout gbl_displayPanel = new GridBagLayout();
		gbl_displayPanel.columnWidths = new int[]{0, 0};
		gbl_displayPanel.rowHeights = new int[]{0, 0};
		gbl_displayPanel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_displayPanel.rowWeights = new double[]{1.0, Double.MIN_VALUE};
		displayPanel.setLayout(gbl_displayPanel);

		tabbedPane = new JTabbedPane(JTabbedPane.BOTTOM);
		GridBagConstraints gbc_tabbedPane = new GridBagConstraints();
		gbc_tabbedPane.fill = GridBagConstraints.BOTH;
		gbc_tabbedPane.gridx = 0;
		gbc_tabbedPane.gridy = 0;
		displayPanel.add(tabbedPane, gbc_tabbedPane);
		
		specificFuncAlysPanel = new JPanel();
		tabbedPane.addTab("Functionality Analysis", null, specificFuncAlysPanel, null);
		GridBagLayout gbl_specificFuncAlysPanel = new GridBagLayout();
		gbl_specificFuncAlysPanel.columnWidths = new int[]{0, 0};
		gbl_specificFuncAlysPanel.rowHeights = new int[]{0, 0};
		gbl_specificFuncAlysPanel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_specificFuncAlysPanel.rowWeights = new double[]{1.0, Double.MIN_VALUE};
		specificFuncAlysPanel.setLayout(gbl_specificFuncAlysPanel);
	}
	
	public void createUI()
	{
		PlaySheetListener psListener = new PlaySheetListener();
		this.addInternalFrameListener(psListener);
		
		createGenericParamPanel();
		createGenericDisplayPanel();
//		createListeners();

		GridBagLayout gridBagLayout = new GridBagLayout();
		gridBagLayout.columnWidths = new int[]{723, 0};
		gridBagLayout.rowHeights = new int[]{571, 0};
		gridBagLayout.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gridBagLayout.rowWeights = new double[]{1.0, Double.MIN_VALUE};
		getContentPane().setLayout(gridBagLayout);

		JPanel panel = new JPanel();
		GridBagLayout gbl_panel = new GridBagLayout();
		gbl_panel.columnWidths = new int[]{0, 0};
		gbl_panel.rowHeights = new int[]{0, 0};
		gbl_panel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_panel.rowWeights = new double[]{1.0, Double.MIN_VALUE};
		panel.setLayout(gbl_panel);
		GridBagConstraints gbc_optimizationHelpScrollPane = new GridBagConstraints();
		gbc_optimizationHelpScrollPane.fill = GridBagConstraints.BOTH;
		gbc_optimizationHelpScrollPane.gridx = 0;
		gbc_optimizationHelpScrollPane.gridy = 0;		
		getContentPane().add(panel, gbc_optimizationHelpScrollPane);
		
		JSplitPane splitPane = new JSplitPane();
		splitPane.setOrientation(JSplitPane.VERTICAL_SPLIT);
		GridBagConstraints gbc_splitPane = new GridBagConstraints();
		gbc_splitPane.fill = GridBagConstraints.BOTH;
		gbc_splitPane.gridx = 0;
		gbc_splitPane.gridy = 0;
		panel.add(splitPane, gbc_splitPane);
		splitPane.setDividerLocation(210);
		splitPane.setContinuousLayout(true);
		splitPane.setOneTouchExpandable(true);
		splitPane.setLeftComponent(ctlScrollPane);
		splitPane.setRightComponent(displayPanel);

		CSSApplication css = new CSSApplication(getContentPane());
	}
	
	public void showAll()
	{
		JDesktopPane pane = (JDesktopPane) DIHelper.getInstance().getLocalProp(
				Constants.DESKTOP_PANE);
		pane.add(this);

		this.setVisible(true);

		this.pack();

		try {
			this.setMaximum(true);
		} catch (PropertyVetoException e) {
			e.printStackTrace();
		}
		JFrame frame2 = (JFrame) DIHelper.getInstance().getLocalProp(
				Constants.MAIN_FRAME);
		frame2.repaint();
	}
	
	public void displayCheckBoxError(){
		JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
		JOptionPane.showMessageDialog(playPane, "Mozilla15 engine doesn't support the current environment. Please switch to 32-bit Java.", "Error", JOptionPane.ERROR_MESSAGE);

	}
	
	@Override
	public void createView() {
		try{
			createUI();
			showAll();
		}catch(Exception e){
			displayCheckBoxError();
			PlaySheetListener psListener = (PlaySheetListener)this.getInternalFrameListeners()[0];
			psListener.internalFrameClosed(new InternalFrameEvent(this,0));
			return;
		}
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void setQuery(String query) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public String getQuery() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void setJDesktopPane(JComponent pane) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void setQuestionID(String id) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public String getQuestionID() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void refineView() {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void overlayView() {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void setRDFEngine(IEngine engine) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public IEngine getRDFEngine() {
		// TODO Auto-generated method stub
		return null;
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
