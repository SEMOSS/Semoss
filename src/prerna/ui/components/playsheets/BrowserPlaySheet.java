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
package prerna.ui.components.playsheets;

import java.awt.BorderLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.beans.PropertyVetoException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;

import javax.swing.JDesktopPane;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.engine.api.IDatabaseEngine;
import prerna.ui.components.ChartControlPanel;
import prerna.ui.main.listener.impl.BrowserPlaySheetListener;

/**
 * The BrowserPlaySheet creates an instance of a browser to utilize the D3 Javascript library to create visualizations.
 */
public class BrowserPlaySheet extends TablePlaySheet {
	
	private static final Logger logger = LogManager.getLogger(BrowserPlaySheet.class.getName());
	public Boolean empty = false;
//	public Browser browser;
//	public BrowserView browserView;
	public String fileName;
	JSplitPane splitPane;
	protected JTabbedPane jTab;
	public Hashtable output = null;
	protected ChartControlPanel controlPanel;
	protected Hashtable dataHash = new Hashtable();
	
	/**
	 * Method getControlPanel. Gets the current Control Panel.
	 * 
	 * @return ChartControlPanel - the current Control Panel
	 */
	public ChartControlPanel getControlPanel() {
		return controlPanel;
	}
	
	/**
	 * Method callIt. Converts a given Hashtable to a Json and passes it to the browser.
	 * 
	 * @param table
	 *            Hashtable - the correctly formatted data from the SPARQL query results.
	 */
	public void callIt(Hashtable table) {
		output = table;
		Gson gson = new GsonBuilder().disableHtmlEscaping().serializeSpecialFloatingPointValues().create();
		logger.info("Converted " + gson.toJson(table));
		logger.info("Converted gson");
		System.out.println(">>>>>>>>>>>>>>> " + gson.toJson(table));
		
		// browser.executeJavaScript("start('" + gson.toJson(table) + "');");
		// create variable val to ensure JXBrowser has the data before trying to paint
		String startStr = "start('" + gson.toJson(table) + "');";
		startStr = startStr.replaceAll("\\\\", "\\\\\\\\");
		System.out.println(startStr);
//		JSValue val = browser.executeJavaScriptAndReturnValue(startStr);
		
//		String remoteDebuggingURL = browser.getRemoteDebuggingURL();
//		System.out.println(">>>>>>>>>>>>>>> REMOTE DEBUGGING URL: " +  remoteDebuggingURL);
	}
	
	/**
	 * This is the function that is used to create the first view of any play sheet. It often uses a lot of the variables previously set on the play
	 * sheet, such as {@link #setQuery(String)}, {@link #setJDesktopPane(JDesktopPane)}, {@link #setRDFEngine(IDatabaseEngine)}, and {@link #setTitle(String)}
	 * so that the play sheet is displayed correctly when the view is first created. It generally creates the model for visualization from the
	 * specified engine, then creates the visualization, and finally displays it on the specified desktop pane
	 * 
	 * This is the function called by the PlaysheetCreateRunner. PlaysheetCreateRunner is the runner used whenever a play sheet is to first be
	 * created, most notably in ProcessQueryListener.
	 */
	@Override
	public void createView() {
		super.createView();
		// BrowserServices.getInstance().setPromptService(new SilentPromptService());
//		LoggerProvider.getBrowserLogger().setLevel(Level.OFF);
//		LoggerProvider.getIPCLogger().setLevel(Level.OFF);
//		LoggerProvider.getChromiumProcessLogger().setLevel(Level.OFF);
//		if (browser == null) {
//			empty = true;
//			return;
//		}
		// browser.getView().getComponent().addMouseListener(new MouseAdapter() {
		// @Override
		// public void mouseWheelMoved(MouseWheelEvent e) {
		// double zoomLevel = browser.getZoomLevel();
		// if(e.getPreciseWheelRotation()<=0.0&&zoomLevel <4.0)
		// {
		// zoomLevel=zoomLevel+0.1;
		// }
		// else if (e.getPreciseWheelRotation()<=0.0&&zoomLevel >0.25)
		// {
		// zoomLevel=zoomLevel-0.1;
		// }
		// browser.setZoomLevel(zoomLevel);
		//
		// }
		// });
//		browserView.addKeyListener(new BrowserZoomKeyListener(browser));
//		browser.loadURL(fileName);
//		
//		while (browser.isLoading()) {
//			try {
//				TimeUnit.MILLISECONDS.sleep(50);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
		empty = false;
		if (dataHash.get("dataSeries") instanceof HashSet) {
			HashSet dataSeries = (HashSet) dataHash.get("dataSeries");
			if (dataSeries == null || dataSeries.size() == 0) {
				empty = true;
				return;
			}
		}
		
		callIt(dataHash);
	}
	
//	@Override
//	public void createData() {
//		super.createData();
//	}
	
	/**
	 * Method refreshView. Refreshes the view and re-populates the play sheet.
	 */
	public void refreshView() {
		empty = false;
		if (dataHash.get("dataSeries") instanceof HashSet) {
			HashSet dataSeries = (HashSet) dataHash.get("dataSeries");
			if (dataSeries == null || dataSeries.size() == 0) {
				empty = true;
				return;
			}
		}
		callIt(dataHash);
	}
	
	/**
	 * Method createCustomView.
	 * 
	 * @see createView()
	 */
	public void createCustomView() {
		super.createView();
	}
	
	/**
	 * Method processQueryData. Processes the data from the SPARQL query into an appropriate format for the specific play sheet.
	 * 
	 * @return Hashtable - the data from the SPARQL query results, formatted accordingly.
	 */
	public void processQueryData() {
		// to be overridden in specific playsheets
	}
	
	/**
	 * Method isEmpty.
	 * 
	 * @return Boolean
	 */
	public Boolean isEmpty() {
		return empty;
	}
	
	/**
	 * Method addPanel. Creates a panel and adds the table to the panel.
	 */
	@Override
	public void addPanel() {
//		browser = new Browser();
//		browserView = new BrowserView(browser);
		try {
			table = new JTable();
			this.jTab = new JTabbedPane();
			JPanel mainPanel = new JPanel();
			setWindow();
			createControlPanel();
			
			splitPane = new JSplitPane();
			splitPane.setEnabled(false);
			splitPane.setOneTouchExpandable(true);
			splitPane.setOrientation(JSplitPane.VERTICAL_SPLIT);
			splitPane.setLeftComponent(controlPanel);
			splitPane.setRightComponent(this.jTab);
			this.jTab.addTab("Visualization", mainPanel);
			
			BrowserPlaySheetListener psListener = new BrowserPlaySheetListener();
			this.addInternalFrameListener(psListener);
			this.setContentPane(splitPane);
			GridBagLayout gbl_mainPanel = new GridBagLayout();
			gbl_mainPanel.columnWidths = new int[] { 0, 0 };
			gbl_mainPanel.rowHeights = new int[] { 0, 0 };
			gbl_mainPanel.columnWeights = new double[] { 1.0, Double.MIN_VALUE };
			gbl_mainPanel.rowWeights = new double[] { 1.0, Double.MIN_VALUE };
			mainPanel.setLayout(gbl_mainPanel);
			
			// this.controlPanel.setBrowser(this.browser);
			
			// callIt(table);
			JPanel panel = new JPanel();
			panel.setLayout(new BorderLayout());
//			panel.add(browserView, BorderLayout.CENTER);
			GridBagConstraints gbc_scrollPane = new GridBagConstraints();
			gbc_scrollPane.fill = GridBagConstraints.BOTH;
			gbc_scrollPane.gridx = 0;
			gbc_scrollPane.gridy = 0;
			mainPanel.add(panel, gbc_scrollPane);
			
			updateProgressBar("0%...Preprocessing", 0);
			resetProgressBar();
			JPanel barPanel = new JPanel();
			
			GridBagConstraints gbc_barPanel = new GridBagConstraints();
			gbc_barPanel.fill = GridBagConstraints.BOTH;
			gbc_barPanel.gridx = 0;
			gbc_barPanel.gridy = 1;
			mainPanel.add(barPanel, gbc_barPanel);
			barPanel.setLayout(new BorderLayout(0, 0));
			barPanel.add(jBar, BorderLayout.CENTER);
			
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
	
	public void addPanelAsTab(String tabName) {
//		browser = new Browser();
//		browserView = new BrowserView(browser);
		try {
			table = new JTable();
			
			JPanel mainPanel = new JPanel();
			this.setContentPane(mainPanel);
			jTab.addTab(tabName, mainPanel);
			jTab.setSelectedIndex(jTab.getTabCount() - 1);
			
			BrowserPlaySheetListener psListener = new BrowserPlaySheetListener();
			this.addInternalFrameListener(psListener);
			
			mainPanel.setLayout(new BorderLayout());
//			mainPanel.add(browserView, BorderLayout.CENTER);
			
			updateProgressBar("0%...Preprocessing", 0);
			resetProgressBar();
			
			this.pack();
			this.setVisible(true);
			this.setSelected(false);
			this.setSelected(true);
			// LOGGER.debug("Added the main pane");
		} catch (PropertyVetoException e) {
			e.printStackTrace();
		}
	}
	
	public void createControlPanel() {
		controlPanel = new ChartControlPanel();
		controlPanel.addExportButton(0);
		
		this.controlPanel.setPlaySheet(this);
	}
	
	/**
	 * Method getBrowser. Gets the current browser.
	 * @return Browser - the current browser.
	 */
//	public Browser getBrowser() {
//		return this.browser;
//	}
	
	/**
	 * Getter for the browser view
	 * @return
	 */
//	public BrowserView getBrowserView() {
//		return this.browserView;
//	}
	
	public void setJTab(JTabbedPane jTab) {
		this.jTab = jTab;
	}
	
	public void setJBar(JProgressBar jBar) {
		this.jBar = jBar;
	}
	
	// TODO: Remove if economic analysis creates its own dashboard or if its column chart is made into a separate playsheet
	public void setDataHash(Hashtable dataHash) {
		this.dataHash = dataHash;
	}
	
	@Override
	public Map<String, Object> getDataMakerOutput(String... selectors){
		Map<String, Object> data = super.getDataMakerOutput();
		if(this.dataHash != null && !this.dataHash.isEmpty()) {
			data.put("specificData", this.dataHash);
		}
		return data;
	}
}