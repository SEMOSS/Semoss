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
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import javax.swing.JInternalFrame;

import org.apache.log4j.Logger;

import com.teamdev.jxbrowser.chromium.Browser;
import com.teamdev.jxbrowser.chromium.BrowserFactory;
import com.teamdev.jxbrowser.chromium.LoggerProvider;
import com.teamdev.jxbrowser.chromium.events.LoadListener;

import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;



/**
 * This class creates the tab sheet in the browser window containing the full address.
 * It is used in the node editor listener to set the appropriate address, listener, and playsheet for navigation.
 */
public class BrowserTabSheetFullAddress extends JInternalFrame implements Runnable{

	  protected static final String LS = System.getProperty("line.separator");
	  
	  LoadListener navListener = null;
	  
	  GraphPlaySheet ps = null;
	  Logger logger = Logger.getLogger(getClass());
	  public Browser browser = null;
	  String fileName;
	  
	  /**
	   * Constructor for BrowserTabSheetFullAddress.
	   * Creates the appropriate browser window and sets properties for the JInternalFrame.
	   */
	  public BrowserTabSheetFullAddress() {
		super("Charts", true, true, true, true);
		browser = BrowserFactory.create();
		 LoggerProvider.getBrowserLogger().setLevel(Level.OFF);
		 LoggerProvider.getIPCLogger().setLevel(Level.OFF);
		 LoggerProvider.getChromiumProcessLogger().setLevel(Level.OFF);
	  }
			  
	  /**
	   * Used to navigate the browser window.
	   */
	  public void navigate(){

		browser.addLoadListener(navListener);
	    browser.loadURL(fileName);
        //browser.executeScript("resolve: RdfEditCtrl.resolve");

	    setLayout(new BorderLayout());
	   
	    add(browser.getView().getComponent(), BorderLayout.CENTER);
	  }
	  
	  /**
	   * Sets the file name to be explored in the browser.
	   * @param fileName	File name.
	   */
	  public void setFileName(String fileName){
		  this.fileName = fileName;
	  }

	  /**
	   * Sets the appropriate playsheet.
	   * @param ps 	Graph playsheet.
	   */
	  public void setPlaySheet(GraphPlaySheet ps)
	  {
		  this.ps = ps;
	  }
	  
	  /**
	   * Sets the navigation listener.
	   * @param navListener 	Navigation listener.
	   */
	  public void setNavListener(LoadListener navListener){
		  this.navListener = navListener;
	  }
	  


	/**
	 * Executes when a thread is started.
	 */
	@Override
	public void run() {
		navigate();
	}
}
