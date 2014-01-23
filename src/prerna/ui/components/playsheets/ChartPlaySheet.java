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
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.util.Hashtable;

import javax.swing.JComboBox;
import javax.swing.JPanel;

import prerna.ui.components.api.IPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;

import com.google.gson.Gson;
import com.teamdev.jxbrowser.Browser;
import com.teamdev.jxbrowser.BrowserFactory;
import com.teamdev.jxbrowser.BrowserType;

/**
 * This will have references to the following a. Internal Frame that needs to be displayed b. The panel of
 * parameters c. The composed SPARQL Query d. Perspective selected e. The question selected by the user f. Filter
 * criteria including slider values
 */
public class ChartPlaySheet extends GraphPlaySheet implements IPlaySheet, Runnable{

	Browser browser = null;

	/**
	 * Constructor for ChartPlaySheet.
	 */
	public ChartPlaySheet()
	{
		super();
	}
	
	/**
	 * Method addInitialPanel.  Creates the listener and adds the frame.
	 */
	public void addInitialPanel()
	{
		setWindow();
		// JInternalFrame frame = new JInternalFrame(title, true, true, true, true);
		// frame.setPreferredSize(new Dimension(400,600));
		cheaterPanel.setPreferredSize(new Dimension(800, 20));
		GridBagLayout gbl_cheaterPanel = new GridBagLayout();
		gbl_cheaterPanel.columnWidths = new int[]{0, 0};
		gbl_cheaterPanel.rowHeights = new int[]{60, 0};
		gbl_cheaterPanel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_cheaterPanel.rowWeights = new double[]{1.0, Double.MIN_VALUE};
		cheaterPanel.setLayout(gbl_cheaterPanel);

		
		jBar.setStringPainted(true);
		jBar.setString("0%...Preprocessing");
		jBar.setValue(0);
		GridBagConstraints gbc_jBar = new GridBagConstraints();
		gbc_jBar.anchor = GridBagConstraints.NORTH;
		gbc_jBar.fill = GridBagConstraints.HORIZONTAL;
		gbc_jBar.gridx = 0;
		gbc_jBar.gridy = 1;
		cheaterPanel.add(jBar, gbc_jBar);
		
		this.getContentPane().setPreferredSize(new Dimension(800,600));
		this.getContentPane().setLayout(new BorderLayout());
		this.getContentPane().add(cheaterPanel, BorderLayout.PAGE_END);
	}

	
	/**
	 * Method createVisualizer.  Creats the JXBrowser, passes the converted JSON to the JXBrowswer, loads the sheet and calls the method to execute.
	 */
	protected void createVisualizer() {
		
		
		// need to figure out a way to pick the HTML file to load
		JComboBox questionList = (JComboBox)DIHelper.getInstance().getLocalProp(Constants.QUESTION_LIST_FIELD);
		String id = DIHelper.getInstance().getIDForQuestion(questionList.getSelectedItem() + "");
		String fileName = id + "_" + Constants.HTML;
		String htmlFileName = DIHelper.getInstance().getProperty(fileName);
		
		  if(DIHelper.getInstance().getProperty(Constants.BROWSER_TYPE).equalsIgnoreCase("Mozilla15"))
			  browser = BrowserFactory.createBrowser(BrowserType.Mozilla15);
		  else if (DIHelper.getInstance().getProperty(Constants.BROWSER_TYPE).equalsIgnoreCase("IE"))
			  browser = BrowserFactory.createBrowser(BrowserType.IE);
		  else if (DIHelper.getInstance().getProperty(Constants.BROWSER_TYPE).equalsIgnoreCase("Mozilla"))
			  browser = BrowserFactory.createBrowser(BrowserType.Mozilla);
		  else if(DIHelper.getInstance().getProperty(Constants.BROWSER_TYPE).equalsIgnoreCase("Safari"))
			  browser = BrowserFactory.createBrowser(BrowserType.Safari);
		  
		String workingDir = System.getProperty("user.dir");
		
	    browser.navigate("file://" + workingDir + htmlFileName);
		browser.waitReady();
		callIt();
		
		
	}
	
	
	/**
	 * Method callIt.  Converts the data to JSON and executes the script in the browser.
	 */
	public void callIt()
	{
		Hashtable nodeHash = filterData.typeHash;
		Hashtable edgeHash = filterData.edgeTypeHash;

		Hashtable newHash = new Hashtable();
		newHash.put("Nodes", nodeHash);
		//newHash.put("Edges", edgeHash);
		Gson gson = new Gson();
		logger.info("Converted " + gson.toJson(newHash));
	    browser.executeScript("start('" + gson.toJson(newHash) + "');");
	}

	/**
	 * Method addPanel.  Adds the JXBrowser.
	 */
	protected void addPanel() 
	{	
		JPanel panel = new JPanel();
	    panel.setLayout(new BorderLayout());
	    panel.add(browser.getComponent(), BorderLayout.CENTER);
	    this.getContentPane().add(panel);
		logger.info("Add Panel Complete >>>>>");
	}

	
	/**
	 * Method createLayout.  Creates the layout for the graph.
	
	 * @return boolean - True if the layout is created, false if it is not.*/
	public boolean createLayout() {
		return true;
	}
}
