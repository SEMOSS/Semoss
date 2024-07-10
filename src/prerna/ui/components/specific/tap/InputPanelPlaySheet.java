/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.beans.PropertyVetoException;

import javax.swing.JDesktopPane;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.JTextArea;
import javax.swing.event.InternalFrameEvent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabaseEngine;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.TablePlaySheet;
import prerna.ui.main.listener.impl.PlaySheetListener;
import prerna.util.CSSApplication;
import prerna.util.Constants;
import prerna.util.DIHelper;


/**
 * This playsheet allows for a split screen view with input panel on top and display panel below.
 * Button on input panel runs an algorithm and updates display panel below with results.
 */
@SuppressWarnings("serial")
public class InputPanelPlaySheet extends TablePlaySheet implements IPlaySheet{
	
	private static final Logger classLogger = LogManager.getLogger(InputPanelPlaySheet.class);
	
	//View is split between the ctlScrollPane and displayPanel. 
	private JScrollPane ctlScrollPane;
	private JPanel displayPanel;
	
	//ctrlPanel is part of the input panel and is where all buttons/checkboxes/selections can be made by user
	protected JPanel ctlPanel;
	protected String titleText = "Optimization Input Parameters:";
	public JProgressBar progressBar;
	
	//tabbedPane is part of the displayPanel and is where all new additional display tabs can be added
	public JTabbedPane displayTabbedPane;
	protected String overallAnalysisTitle = "Overall Analysis";
	public JPanel overallAlysPanel;
	public JPanel overallAlysMetricsPanel, overallAlysChartPanel;
	public JTextArea consoleArea = new JTextArea();

	public IDatabaseEngine engine;//TODO might not need
	

	/**
	 * Constructor for InputPanelPlaySheet.
	 */
	public InputPanelPlaySheet() {
		
		this.setClosable(true);
		this.setMaximizable(true);
		this.setIconifiable(true);
		this.setResizable(true);
		this.setPreferredSize(new Dimension(800, 600));
	}
	
	@Override
	public void createData() {
		// TODO Auto-generated method stub
		
	}
	
	/**
	 * Creates the user interface of the playsheet.
	 * Calls functions to create param panel and tabbed display panel
	 * Stitches the param and display panels together.
	 */
	protected void createUI() {
		PlaySheetListener psListener = new PlaySheetListener();
		this.addInternalFrameListener(psListener);
		
		createParamPanel();

		createDisplayPanel();

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

		new CSSApplication(getContentPane());
	}
	
	/**
	 * Sets up the Param panel at the top of the split pane.
	 * ctlPanel is protected so any extending classes can add additional components.
	 * progressBar can be updated externally to show progress of algorithm
	 */
	protected void createParamPanel() {
		ctlScrollPane = new JScrollPane();		
		ctlPanel = new JPanel();
		ctlScrollPane.setViewportView(ctlPanel);
		GridBagLayout gbl_ctlPanel = new GridBagLayout();
		gbl_ctlPanel.columnWidths = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0};
		gbl_ctlPanel.rowHeights = new int[]{0, 0, 0, 0, 0, 0, 0};
		gbl_ctlPanel.columnWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0};
		gbl_ctlPanel.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		ctlPanel.setLayout(gbl_ctlPanel);

		JLabel titleLbl = new JLabel(titleText);
		titleLbl.setFont(new Font("Tahoma", Font.BOLD, 12));
		GridBagConstraints gbc_titleLbl = new GridBagConstraints();
		gbc_titleLbl.gridwidth = 7;
		gbc_titleLbl.anchor = GridBagConstraints.WEST;
		gbc_titleLbl.insets = new Insets(10, 0, 5, 5);
		gbc_titleLbl.gridx = 1;
		gbc_titleLbl.gridy = 0;
		ctlPanel.add(titleLbl, gbc_titleLbl);

		progressBar = new JProgressBar();
		progressBar.setFont(new Font("Tahoma", Font.BOLD, 13));
		GridBagConstraints gbc_progressBar = new GridBagConstraints();
		gbc_progressBar.anchor = GridBagConstraints.SOUTH;
		gbc_progressBar.fill = GridBagConstraints.HORIZONTAL;
		gbc_progressBar.gridwidth = 2;
		gbc_progressBar.insets = new Insets(0, 0, 0, 5);
		gbc_progressBar.gridx = 6;
		gbc_progressBar.gridy = 0;
		ctlPanel.add(progressBar, gbc_progressBar);
		progressBar.setVisible(false);		
	}

	/**
	 * Sets up the display panel at the bottom of the split pane.
	 * tabbedPane is protected so any extending classes can add additional tabs for disply
	 * overallAlysPanel is the original tab in the tabbedPane.
	 * Extensions should populate this with high level metric labels and the graphs
	 */
	protected void createDisplayPanel() {
		
		displayPanel = new JPanel();

		GridBagLayout gbl_displayPanel = new GridBagLayout();
		gbl_displayPanel.columnWidths = new int[]{0, 0};
		gbl_displayPanel.rowHeights = new int[]{0, 0};
		gbl_displayPanel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_displayPanel.rowWeights = new double[]{1.0, Double.MIN_VALUE};
		displayPanel.setLayout(gbl_displayPanel);

		displayTabbedPane = new JTabbedPane(JTabbedPane.BOTTOM);
		GridBagConstraints gbc_tabbedPane = new GridBagConstraints();
		gbc_tabbedPane.fill = GridBagConstraints.BOTH;
		gbc_tabbedPane.gridx = 0;
		gbc_tabbedPane.gridy = 0;
		displayPanel.add(displayTabbedPane, gbc_tabbedPane);

		overallAlysPanel = new JPanel();
		JScrollPane jsPane = new JScrollPane(overallAlysPanel);
		overallAlysPanel.setBackground(Color.WHITE);
		displayTabbedPane.addTab(overallAnalysisTitle, null, jsPane, null);
		
		GridBagLayout gbl_overallAlysPanel = new GridBagLayout();
		gbl_overallAlysPanel.columnWidths = new int[]{0, 0, 0};
		gbl_overallAlysPanel.rowHeights = new int[]{0, 0, 0};
		gbl_overallAlysPanel.columnWeights = new double[]{0.0, 1.0, Double.MIN_VALUE};
		gbl_overallAlysPanel.rowWeights = new double[]{0.0, 1.0, Double.MIN_VALUE};
		overallAlysPanel.setLayout(gbl_overallAlysPanel);
		overallAlysPanel.setBackground(Color.WHITE);
		
		overallAlysMetricsPanel = new JPanel();
		overallAlysMetricsPanel.setBackground(Color.WHITE);
		GridBagConstraints gbc_panel_1 = new GridBagConstraints();
		gbc_panel_1.insets = new Insets(10, 0, 5, 0);
		gbc_panel_1.fill = GridBagConstraints.BOTH;
		gbc_panel_1.gridx = 1;
		gbc_panel_1.gridy = 0;
		overallAlysPanel.add(overallAlysMetricsPanel, gbc_panel_1);
		
		GridBagLayout gbl_panel_1 = new GridBagLayout();
		gbl_panel_1.columnWidths = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0};
		gbl_panel_1.rowHeights = new int[]{0, 0, 0};
		gbl_panel_1.columnWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, Double.MIN_VALUE};
		gbl_panel_1.rowWeights = new double[]{0.0, 0.0, Double.MIN_VALUE};
		overallAlysMetricsPanel.setLayout(gbl_panel_1);

		JPanel consolePanel = new JPanel();
		displayTabbedPane.addTab("Console", null, consolePanel, null);
		GridBagLayout gbl_consolePanel = new GridBagLayout();
		gbl_consolePanel.columnWidths = new int[]{0, 0};
		gbl_consolePanel.rowHeights = new int[]{0, 0};
		gbl_consolePanel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_consolePanel.rowWeights = new double[]{1.0, Double.MIN_VALUE};
		consolePanel.setLayout(gbl_consolePanel);

		JScrollPane scrollPane_1 = new JScrollPane();
		scrollPane_1.setPreferredSize(new Dimension(800,400));
		consolePanel.setBackground(Color.WHITE);
		GridBagConstraints gbc_scrollPane_1 = new GridBagConstraints();
		gbc_scrollPane_1.fill = GridBagConstraints.BOTH;
		gbc_scrollPane_1.gridx = 0;
		gbc_scrollPane_1.gridy = 0;
		consolePanel.add(scrollPane_1, gbc_scrollPane_1);

		consoleArea = new JTextArea();
		scrollPane_1.setViewportView(consoleArea);
	}

	/**
	 * Sets graphs to be visible after algorithm is run.
	 * Generally all graphs will be on the overall analysis tab.
	 * @param visible
	 */
	public void setGraphsVisible(boolean visible) {}
	
	/**
	 * Clears panels within the playsheet.
	 * Called whenever the algorithm is rerun so no previous results left over.
	 */
	public void clearPanels() {}
	
	/**
	 * Clears graphs within the playsheets.
	 * Called whenever the algorithm is rerun so no previous results left over.
	 */
	public void clearGraphs() {}
	
	/**
	 * Sets N/A or $0 for values in optimizations.
	 * Called whenever the algorithm is rerun so no previous results left over.
	 */
	public void clearLabels() {}

	/**
	 * Adds a desktop pane and shows all properties in the main frame.
	 */
	public void showAll() {
		JDesktopPane pane = (JDesktopPane) DIHelper.getInstance().getLocalProp(
				Constants.DESKTOP_PANE);
		pane.add(this);

		this.setVisible(true);

		this.pack();

		try {
			this.setMaximum(true);
		} catch (PropertyVetoException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		JFrame frame2 = (JFrame) DIHelper.getInstance().getLocalProp(
				Constants.MAIN_FRAME);
		frame2.repaint();
	}

	/**
	 * Creates user interface and shows all properties as long as the browser is supported.
	 */
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

	/**
	 * Sets the RDF engine for the play sheet to run its query against. 
	 * @param engine 	Set engine.
	 */
	@Override
	public void setRDFEngine(IDatabaseEngine engine) {
		this.engine = engine;
	}

	/**
	 * Displays an error message in the pane. TODO make more clear if errors with calculation
	 */
	protected void displayCheckBoxError() {
		JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
		JOptionPane.showMessageDialog(playPane, "Error showing the playsheet.", "Error", JOptionPane.ERROR_MESSAGE);
	}

}
