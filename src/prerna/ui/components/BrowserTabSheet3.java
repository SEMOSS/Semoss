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
package prerna.ui.components;


import java.awt.BorderLayout;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import javax.swing.JButton;
import javax.swing.JInternalFrame;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.teamdev.jxbrowser.chromium.Browser;
import com.teamdev.jxbrowser.chromium.BrowserFactory;
import com.teamdev.jxbrowser.chromium.LoggerProvider;

import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.ui.main.listener.impl.ChartPullDataListener;
import prerna.util.Constants;
import prerna.util.DIHelper;



/**
 * This class is used in chart listeners to create the appropriate browser and pull the appropriate data for a playsheet.
 */
public class BrowserTabSheet3 extends JInternalFrame implements Runnable{

	  protected static final String LS = System.getProperty("line.separator");
	  
	  ChartPullDataListener cp = null;
	  GraphPlaySheet ps = null;
	  static final Logger logger = LogManager.getLogger(BrowserTabSheet3.class.getName());
	  public Browser browser = BrowserFactory.create();
	  public JButton pullData;
	  
	  /**
	   * Constructor for BrowserTabSheet3.
	   * @param fileName 	File name to be navigated to in the browser.
	   * @param ps			Playsheet being called.
	   */
	  public BrowserTabSheet3(String fileName, GraphPlaySheet ps) {
		super("Charts", true, true, true, true);
		this.ps = ps;
		
	       String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
	        //webBrowser.navigate("file://" + workingDir + "/SimpleTest.html");
	       //browser.
	       cp = new ChartPullDataListener();
			cp.setBrowser(browser);
			cp.setPlaySheet(ps);
	       
			 LoggerProvider.getBrowserLogger().setLevel(Level.OFF);
			 LoggerProvider.getIPCLogger().setLevel(Level.OFF);
			 LoggerProvider.getChromiumProcessLogger().setLevel(Level.OFF);
	       
	    browser.loadURL("file://" + workingDir + fileName);
	   // browser.registerFunction("MyFunction", new SPARQLExecuteFunction());
		while (browser.isLoading()) {
		    try {
				TimeUnit.MILLISECONDS.sleep(50);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
        setPlaySheet(null);
        runCallIt();
	    
	    setLayout(new BorderLayout());
	    pullData = new JButton("Pull new data");
	    pullData.addActionListener(cp);

	    add(pullData, BorderLayout.NORTH);
	    add(browser.getView().getComponent(), BorderLayout.CENTER);
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
