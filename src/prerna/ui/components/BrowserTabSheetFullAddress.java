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
import java.util.logging.Level;

import javax.swing.JInternalFrame;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ui.components.playsheets.GraphPlaySheet;

import com.teamdev.jxbrowser.chromium.Browser;
import com.teamdev.jxbrowser.chromium.LoggerProvider;
import com.teamdev.jxbrowser.chromium.events.LoadListener;
import com.teamdev.jxbrowser.chromium.swing.BrowserView;



/**
 * This class creates the tab sheet in the browser window containing the full address.
 * It is used in the node editor listener to set the appropriate address, listener, and playsheet for navigation.
 */
public class BrowserTabSheetFullAddress extends JInternalFrame implements Runnable{

	protected static final String LS = System.getProperty("line.separator");
	LoadListener navListener = null;
	GraphPlaySheet ps = null;
	static final Logger logger = LogManager.getLogger(BrowserTabSheetFullAddress.class.getName());
	public Browser browser;
	public BrowserView browserView;
	String fileName;

	/**
	 * Constructor for BrowserTabSheetFullAddress.
	 * Creates the appropriate browser window and sets properties for the JInternalFrame.
	 */
	public BrowserTabSheetFullAddress() {
		super("Charts", true, true, true, true);
		browser = new Browser();
		browserView = new BrowserView(browser);
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
		add(browserView, BorderLayout.CENTER);
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
