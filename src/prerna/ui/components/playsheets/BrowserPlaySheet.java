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
package prerna.ui.components.playsheets;

import java.awt.BorderLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.beans.PropertyVetoException;
import java.util.HashSet;
import java.util.Hashtable;

import javax.swing.JDesktopPane;
import javax.swing.JPanel;
import javax.swing.JSplitPane;
import javax.swing.JTable;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.ChartControlPanel;
import prerna.ui.main.listener.impl.GridPlaySheetListener;
import prerna.util.Constants;
import prerna.util.DIHelper;

import com.google.gson.Gson;
import com.teamdev.jxbrowser.Browser;
import com.teamdev.jxbrowser.BrowserFactory;
import com.teamdev.jxbrowser.BrowserType;

/**
 * The BrowserPlaySheet creates an instance of a browser to utilize the D3 Javascript library to create visualizations.
 */
public class BrowserPlaySheet extends BasicProcessingPlaySheet {

	public Boolean empty = false;
	public Browser browser = null;
	protected String fileName;
	JSplitPane splitPane;
	Hashtable output = null;
	ChartControlPanel controlPanel = new ChartControlPanel();
	
	/**
	 * Method getControlPanel.  Gets the current Control Panel.
	
	 * @return ChartControlPanel - the current Control Panel*/
	public ChartControlPanel getControlPanel()
	{
		return controlPanel;
	}
	
	/**
	 * Method callIt.  Converts a given Hashtable to a Json and passes it to the browser.
	 * @param table Hashtable - the correctly formatted data from the SPARQL query results.
	 */
	public void callIt(Hashtable table)
	{
		output = table;
		Gson gson = new Gson();
		//logger.info("Converted " + gson.toJson(table));
		logger.info("Converted gson");

		browser.executeScript("start('" + gson.toJson(table) + "');");
	}

	/**
	 * This is the function that is used to create the first view 
	 * of any play sheet.  It often uses a lot of the variables previously set on the play sheet, such as {@link #setQuery(String)},
	 * {@link #setJDesktopPane(JDesktopPane)}, {@link #setRDFEngine(IEngine)}, and {@link #setTitle(String)} so that the play 
	 * sheet is displayed correctly when the view is first created.  It generally creates the model for visualization from 
	 * the specified engine, then creates the visualization, and finally displays it on the specified desktop pane
	 * 
	 * This is the function called by the PlaysheetCreateRunner.  PlaysheetCreateRunner is the runner used whenever a play 
	 * sheet is to first be created, most notably in ProcessQueryListener.
	 */
	@Override
	public void createView()
	{
		super.createView();
		if(rs==null)
			return;
		//BrowserServices.getInstance().setPromptService(new SilentPromptService());
		browser.navigate(fileName);
		browser.waitReady();
		empty = false;
		Hashtable dataHash = new Hashtable();
		dataHash = processQueryData();
		if(dataHash.get("dataSeries") instanceof HashSet)
		{
			HashSet dataSeries = (HashSet)dataHash.get("dataSeries");
			if(dataSeries == null || dataSeries.size()==0)
			{
				empty = true;
				return;
			}
		}                                                                      
		callIt(dataHash);
	}
	
	/**
	 * Method refreshView.  Refreshes the view and re-populates the play sheet.
	 */
	public void refreshView()
	{
		empty = false;
		Hashtable dataHash = new Hashtable();
		dataHash = processQueryData();
		if(dataHash.get("dataSeries") instanceof HashSet)
		{
			HashSet dataSeries = (HashSet)dataHash.get("dataSeries");
			if(dataSeries == null || dataSeries.size()==0)
			{
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
	public void createCustomView()
	{
		super.createView();
	}
	
	/**
	 * Method processQueryData. Processes the data from the SPARQL query into an appropriate format for the specific play sheet.
	
	 * @return Hashtable - the data from the SPARQL query results, formatted accordingly.*/
	public Hashtable processQueryData(){
		return new Hashtable();
	}
	
	/**
	 * Method isEmpty.
	
	 * @return Boolean */
	public Boolean isEmpty(){
		return empty;
	}
	
	/**
	 * Method addPanel. Creates a panel and adds the table to the panel.
	 */
	@Override
	public void addPanel()
	{
		try {
			table = new JTable();
			JPanel mainPanel = new JPanel();
			setWindow();
			
			splitPane = new JSplitPane();
			splitPane.setEnabled(false);
			splitPane.setOneTouchExpandable(true);
			splitPane.setOrientation(JSplitPane.VERTICAL_SPLIT);
			splitPane.setLeftComponent(controlPanel);
			splitPane.setRightComponent(mainPanel);

			this.addInternalFrameListener(GridPlaySheetListener.getInstance());
			this.setContentPane(splitPane);
			GridBagLayout gbl_mainPanel = new GridBagLayout();
			gbl_mainPanel.columnWidths = new int[]{0, 0};
			gbl_mainPanel.rowHeights = new int[]{0, 0};
			gbl_mainPanel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
			gbl_mainPanel.rowWeights = new double[]{1.0, Double.MIN_VALUE};
			mainPanel.setLayout(gbl_mainPanel);

			if(DIHelper.getInstance().getProperty(Constants.BROWSER_TYPE).equalsIgnoreCase("Mozilla15"))
				browser = BrowserFactory.createBrowser(BrowserType.Mozilla15);
			else if (DIHelper.getInstance().getProperty(Constants.BROWSER_TYPE).equalsIgnoreCase("IE"))
				browser = BrowserFactory.createBrowser(BrowserType.IE);
			else if (DIHelper.getInstance().getProperty(Constants.BROWSER_TYPE).equalsIgnoreCase("Mozilla"))
				browser = BrowserFactory.createBrowser(BrowserType.Mozilla);
			else if(DIHelper.getInstance().getProperty(Constants.BROWSER_TYPE).equalsIgnoreCase("Safari"))
				browser = BrowserFactory.createBrowser(BrowserType.Safari);
			
			this.controlPanel.setPlaySheet(this);
//			this.controlPanel.setBrowser(this.browser);
			
			//callIt(table);
			JPanel panel = new JPanel();
			panel.setLayout(new BorderLayout());
			panel.add(browser.getComponent(), BorderLayout.CENTER);
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
		}
		catch (PropertyVetoException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Method getBrowser.  Gets the current browser.
	
	 * @return Browser - the current browser. */
	public Browser getBrowser() {
		return this.browser;
	}
}
