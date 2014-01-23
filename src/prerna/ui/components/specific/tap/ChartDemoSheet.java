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

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.util.Hashtable;

import javax.swing.JDesktopPane;
import javax.swing.JFrame;
import javax.swing.JPanel;

import prerna.ui.components.playsheets.BrowserPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;

import com.google.gson.Gson;
import com.teamdev.jxbrowser.BrowserFactory;
import com.teamdev.jxbrowser.BrowserServices;
import com.teamdev.jxbrowser.BrowserType;
import com.teamdev.jxbrowser.prompt.SilentPromptService;

/**
 * This is a demo sheet to create future chart playsheets.
 */
public class ChartDemoSheet extends BrowserPlaySheet{

	/**
	 * Constructor for ChartDemoSheet.
	 */
	public ChartDemoSheet()
	{
		//createUI();
		this.setClosable(true);
		this.setMaximizable(true);
		this.setIconifiable(true);
		this.setResizable(true);
		this.setPreferredSize(new Dimension(800, 600));
		



	}
	/**
	 * Method createUI.
	 */
	public void createUI()
	{

		if(DIHelper.getInstance().getProperty(Constants.BROWSER_TYPE).equalsIgnoreCase("Mozilla15"))
			  browser = BrowserFactory.createBrowser(BrowserType.Mozilla15);
		  else if (DIHelper.getInstance().getProperty(Constants.BROWSER_TYPE).equalsIgnoreCase("IE"))
			  browser = BrowserFactory.createBrowser(BrowserType.IE);
		  else if (DIHelper.getInstance().getProperty(Constants.BROWSER_TYPE).equalsIgnoreCase("Mozilla"))
			  browser = BrowserFactory.createBrowser(BrowserType.Mozilla);
		  else if(DIHelper.getInstance().getProperty(Constants.BROWSER_TYPE).equalsIgnoreCase("Safari"))
			  browser = BrowserFactory.createBrowser(BrowserType.Safari);
		 
		BrowserServices.getInstance().setPromptService(new SilentPromptService());
		String workingDir = System.getProperty("user.dir");
		browser.navigate("file://" + workingDir + "/html/MHS-RDFSemossCharts/app/singlechartgrid.html");
		browser.waitReady();
	       
		Hashtable table = createTableData();
		callIt(table);
		JPanel panel = new JPanel();
	    panel.setLayout(new BorderLayout());
	    panel.add(browser.getComponent(), BorderLayout.CENTER);
	    this.getContentPane().add(panel);
	}
	
	/**
	 * Method callIt.
	 * @param table Hashtable
	 */
	public void callIt(Hashtable table)
	{


		//Hashtable newHash = new Hashtable();
		//newHash.put("Nodes", table);
		//newHash.put("Edges", edgeHash);
		Hashtable table2 = new Hashtable();
		Gson gson = new Gson();
		logger.info("Converted " + gson.toJson(table));

		//webBrowser.executeJavascript("helloWorld('" + gson.toJson(newHash) + "');"); //Please tell me this is awesome !!!!!!');");
		//Please tell me this is awesome !!!!!!');");
		browser.executeScript("start('" + gson.toJson(table) + "');");
	}
	
	
	/**
	 * Method createTableData.
	
	 * @return Hashtable */
	public Hashtable createTableData()
	{
		Object[][] data = new Object[14][4];
		Hashtable table = new Hashtable();
		Hashtable dataHash = new Hashtable();
		table.put("type",  "scatter");
		table.put("title",  "Tasks to Cloud");
		table.put("yAxisTitle", "Process Integration");

		table.put("xAxisTitle", "Process Maturity");
		double range = 0.99-0.779;
		double low = 0.779;
		data[0][0]=(0.89-low)/range;
		data[0][1]=0.58;
		data[0][2]=0;
		data[0][3]="Document and Archive Data to Enhance Continuity of Care and Promote Ongoing Review and Analysis";
		data[1][0]=(0.95-low)/range;
		data[1][1]=0.53;
		data[1][2]=0;
		data[1][3]="Multidisciplinary Teams Reflecting Clinical Requirements of a Local Patient Population‘s Needs";
		data[2][0]=(0.79-low)/range;
		data[2][1]=0.75;
		data[2][2]=0;
		data[2][3]="Determine the diagnosis, prognosis, and appropriate treatment plan related to impairments, disability or other health-related conditions that result in functional limitations.";
		data[3][0]=(0.779-low)/range;
		data[3][1]=0.74;
		data[3][2]=0;
		data[3][3]="Perform diagnostic test or procedure";
		data[4][0]=(0.942-low)/range;
		data[4][1]=0.66;
		data[4][2]=0;
		data[4][3]="Triage and Treatment";
		data[5][0]=(0.962-low)/range;
		data[5][1]=0.60;
		data[5][2]=0;
		data[5][3]="Completely document patient care delivered and have information included in patient medical record";
		data[6][0]=(0.99-low)/range;
		data[6][1]=0.67;
		data[6][2]=0;
		data[6][3]="Document/ Manage Medical and Non-medical Information";
		data[7][0]=(0.966-low)/range;
		data[7][1]=0.64;
		data[7][2]=0;
		data[7][3]="Educate targeted populations and the healthcare givers who serve these populations on the need for preventive health counseling (this may overlap with community health education)";
		data[8][0]=(0.977-low)/range;
		data[8][1]=0.42;
		data[8][2]=0;
		data[8][3]="Maintain a licensed professional staff";
		data[9][0]=(0.967-low)/range;
		data[9][1]=0.75;
		data[9][2]=0;
		data[9][3]="Provide Support for Human Specimen Source";
		data[10][0]=(0.973-low)/range;
		data[10][1]=0.748;
		data[10][2]=0;
		data[10][3]="Provide Support for Veterinary Specimen Sources";
		data[11][0]=(0.79-low)/range;
		data[11][1]=0.748;
		data[11][2]=0;
		data[11][3]="Perform water safety inspections";
		data[12][0]=(0.78-low)/range;
		data[12][1]=0.764;
		data[12][2]=0;
		data[12][3]="Investigate outbreaks";
		data[13][0]=(0.79-low)/range;
		data[13][1]=0.725;
		data[13][2]=0;
		data[13][3]="Perform food safety inspections";
		dataHash.put("Tasks", data);
		table.put("dataSeries", dataHash);
		
		return table;
	}
	
	/**
	 * Method showAll.
	 */
	public void showAll()
	{
		JDesktopPane pane = (JDesktopPane) DIHelper.getInstance().getLocalProp(
				Constants.DESKTOP_PANE);
		pane.add(this);
		
		this.setVisible(true);

		this.pack();
		
		JFrame frame2 = (JFrame) DIHelper.getInstance().getLocalProp(
				Constants.MAIN_FRAME);
		frame2.repaint();
	}
	
	/**
	 * Method createView.
	 */
	@Override
	public void createView() {
		createUI();
		showAll();
	}

}
