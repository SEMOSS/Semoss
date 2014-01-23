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


import java.awt.BorderLayout;

import javax.swing.JButton;
import javax.swing.JInternalFrame;

import org.apache.log4j.Logger;

import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.ui.main.listener.impl.ChartPullDataListener;
import prerna.util.Constants;
import prerna.util.DIHelper;

import com.teamdev.jxbrowser.Browser;
import com.teamdev.jxbrowser.BrowserFactory;
import com.teamdev.jxbrowser.BrowserType;
import com.teamdev.jxbrowser.events.NavigationEvent;
import com.teamdev.jxbrowser.events.NavigationFinishedEvent;
import com.teamdev.jxbrowser.events.NavigationListener;


/**
 * This class is used in chart listeners to create the appropriate browser and pull the appropriate data for a playsheet.
 */
public class BrowserTabSheet3 extends JInternalFrame implements Runnable{

	  protected static final String LS = System.getProperty("line.separator");
	  
	  ChartPullDataListener cp = null;
	  GraphPlaySheet ps = null;
	  Logger logger = Logger.getLogger(getClass());
	  public Browser browser = null;
	  public JButton pullData;
	  
	  /**
	   * Constructor for BrowserTabSheet3.
	   * @param fileName 	File name to be navigated to in the browser.
	   * @param ps			Playsheet being called.
	   */
	  public BrowserTabSheet3(String fileName, GraphPlaySheet ps) {
		super("Charts", true, true, true, true);
		this.ps = ps;
		   // super(new BorderLayout());
			  if(DIHelper.getInstance().getProperty(Constants.BROWSER_TYPE).equalsIgnoreCase("Mozilla15"))
				  browser = BrowserFactory.createBrowser(BrowserType.Mozilla15);
			  else if (DIHelper.getInstance().getProperty(Constants.BROWSER_TYPE).equalsIgnoreCase("IE"))
				  browser = BrowserFactory.createBrowser(BrowserType.IE);
			  else if (DIHelper.getInstance().getProperty(Constants.BROWSER_TYPE).equalsIgnoreCase("Mozilla"))
				  browser = BrowserFactory.createBrowser(BrowserType.Mozilla);
			  else if(DIHelper.getInstance().getProperty(Constants.BROWSER_TYPE).equalsIgnoreCase("Safari"))
				  browser = BrowserFactory.createBrowser(BrowserType.Safari);
			  
	       String workingDir = System.getProperty("user.dir");
	        //webBrowser.navigate("file://" + workingDir + "/SimpleTest.html");
	       //browser.
	       cp = new ChartPullDataListener();
			cp.setBrowser(browser);
			cp.setPlaySheet(ps);
	       
	       browser.addNavigationListener(new NavigationListener() {
	    	    public void navigationStarted(NavigationEvent event) {
	    	    	logger.info("event.getUrl() = " + event.getUrl());
	    	    }

	    	    public void navigationFinished(NavigationFinishedEvent event) {
	    	    	logger.info("event.getStatusCode() = " + event.getStatusCode());
	    	        setPlaySheet(null);
	    	        runCallIt();
	    	        //cp.callIt();
	    	    }
	    	});
	       
			

	       
	    browser.navigate("file://" + workingDir + fileName);
	   // browser.registerFunction("MyFunction", new SPARQLExecuteFunction());
	    
	    setLayout(new BorderLayout());
	    pullData = new JButton("Pull new data");
	    pullData.addActionListener(cp);

	    add(pullData, BorderLayout.NORTH);
	    add(browser.getComponent(), BorderLayout.CENTER);
	  }

	  /**
	   * Sets the playsheet.
	   * @param ps 	Graph playsheet.
	   */
	  public void setPlaySheet(GraphPlaySheet ps)
	  {
		  this.ps = ps;
	  }
	  
	  /**
	   * Sets the thread and begins execution of the thread. 
	   */
	  public void runCallIt()
	  {
		  Thread thread = new Thread(this);
		  thread.start();
	  }

	  /**
	   * Converts hashtable into JSON format in chart pull data listener.
	   */
	  @Override
	public void run() {
		  cp.callIt();
		  pullData.setText("Basic Pull Complete");
		  //browser.executeScript("alert(MyFunction('PK'));");
		  
	  }
	}
