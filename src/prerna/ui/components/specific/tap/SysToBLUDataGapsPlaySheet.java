/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.ui.components.specific.tap;

import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.beans.PropertyVetoException;
import java.util.Hashtable;

import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JDesktopPane;
import javax.swing.JFrame;
import javax.swing.JInternalFrame;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.border.BevelBorder;
import javax.swing.event.InternalFrameEvent;

import prerna.engine.api.IEngine;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.main.listener.impl.PlaySheetListener;
import prerna.ui.main.listener.specific.tap.SystToBLUHeatmapBtnListener;
import prerna.ui.swing.custom.CustomButton;
import prerna.util.CSSApplication;
import prerna.util.Constants;
import prerna.util.DIHelper;
import aurelienribon.ui.css.Style;



/**
 * This is the playsheet used to examine whether a relationship exists between two variables.
 */
public class SysToBLUDataGapsPlaySheet extends JInternalFrame implements IPlaySheet {
	
//	//toggles to show the system Functionality and Capability Functionality panels
//	public JToggleButton showSystemSelectBtn, showSystemCapSelectBtn;
	public JPanel systemDataBLUSelectPanel;
	//individual components of the panel
	public DHMSMSystemSelectPanel systemSelectPanel;
	// public DHMSMCapabilitySelectPanel capabilitySelectPanel;
	public DHMSMBLUSelectPanel dataSelectPanel;
	
	public IEngine engine;
	
	public JScrollPane ctlScrollPane;
	public JPanel ctlPanel;
	public JPanel displayPanel;
	
	public JTabbedPane tabbedPane;
	public JPanel heatMapPanel;
	public JPanel HeatPanel = new JPanel();
	
	/**
	 * Constructor for SysToBLUDataGapsPlaySheet.
	 */
	public SysToBLUDataGapsPlaySheet()
	{
		// creates user interface
		this.setClosable(true);
		this.setMaximizable(true);
		this.setIconifiable(true);
		this.setResizable(true);
		this.setPreferredSize(new Dimension(800, 600));
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
		
		systemSelectPanel = new DHMSMSystemSelectPanel(engine);
		GridBagConstraints gbc_systemSelectPanel = new GridBagConstraints();
		gbc_systemSelectPanel.gridheight = 6;
		gbc_systemSelectPanel.fill = GridBagConstraints.BOTH;
		gbc_systemSelectPanel.gridx = 0;
		gbc_systemSelectPanel.gridy = 0;
		systemDataBLUSelectPanel.add(systemSelectPanel, gbc_systemSelectPanel);
		
		dataSelectPanel = new DHMSMBLUSelectPanel(engine,systemSelectPanel);
		GridBagConstraints gbc_dataSelectPanel = new GridBagConstraints();
		gbc_dataSelectPanel.gridheight = 6;
		gbc_dataSelectPanel.fill = GridBagConstraints.BOTH;
		gbc_dataSelectPanel.gridx = 3;
		gbc_dataSelectPanel.gridy = 0;
		systemDataBLUSelectPanel.add(dataSelectPanel, gbc_dataSelectPanel);
		
		Object hidePopupKey = new JComboBox().getClientProperty("doNotCancelPopup");  
		JButton btnHeatUpdate = new CustomButton("Update Heat Map");
		btnHeatUpdate.putClientProperty("doNotCancelPopup", hidePopupKey);

		btnHeatUpdate.setFont(new Font("Tahoma", Font.BOLD, 11));
		GridBagConstraints gbc_btnHeatUpdate = new GridBagConstraints();
		gbc_btnHeatUpdate.gridwidth = 4;
		gbc_btnHeatUpdate.insets = new Insets(10, 0, 5, 5);
		gbc_btnHeatUpdate.anchor = GridBagConstraints.WEST;
		gbc_btnHeatUpdate.gridx = 4;
		gbc_btnHeatUpdate.gridy = 5;
		ctlPanel.add(btnHeatUpdate, gbc_btnHeatUpdate);
		addHeatBtnListener(btnHeatUpdate);
		Style.registerTargetClassName(btnHeatUpdate,  ".createBtn");
		
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

	public void addHeatBtnListener(JButton btnHeatUpdate)
	{
		SystToBLUHeatmapBtnListener rbl = new SystToBLUHeatmapBtnListener();
		rbl.setPlaySheet(this);
		btnHeatUpdate.addActionListener(rbl);
	}
	
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
		
		HeatPanel = new JPanel();
		tabbedPane.addTab("Comparison Heat Map", null, HeatPanel,null);
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
		}catch(RuntimeException e){
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
		this.engine = engine;
		
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
	
	@Override
	public void setTitle(String title) {
		super.setTitle(title);
		this.title = title;
	}

	@Override
	public Object doMethod(String methodName, Hashtable argHash) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void processQueryData() {
		// TODO Auto-generated method stub
		
	}
	
	
}
