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
package prerna.ui.main;

import java.awt.Color;
import java.awt.Insets;
import java.io.File;

import javax.swing.UIDefaults;
import javax.swing.UIManager;
import javax.swing.UIManager.LookAndFeelInfo;
import javax.swing.plaf.ColorUIResource;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.PlayPane;
import prerna.util.AbstractFileWatcher;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

import com.ibm.icu.util.StringTokenizer;

/**
 * The Starter class is run to start the SEMOSS application.  This launches the Splash Screen and the base user interface.
 */
public class Starter {
	Object monitor = new Object();
	private static String govWarningString ="You are accessing a U.S. Government (USG) Information System (IS) that is provided for USG-authorized use only."
			+"\n"
			+"\n"
			+"By using this IS (which includes any device attached to this IS), you consent to the following conditions:"
			+"\n"
			+"\n"
			+"The USG routinely intercepts and monitors communications on this IS for purposes including, but not limited to, penetration testing, COMSEC monitoring, "
			+"\n"
			+"network operations and defense, personnel misconduct (PM), law enforcement (LE), and counterintelligence (CI) investigations."
			+"\n"
			+"\n"
			+"At any time, the USG may inspect and seize data stored on this IS."
			+"\n"
			+"\n"
			+"Communications using, or data stored on, this IS are not private, are subject to routine monitoring, interception, and search, and may be disclosed or used for any USG-authorized purpose."
			+"\n"
			+"\n"
			+"This IS includes security measures (e.g., authentication and access controls) to protect USG interests--not for your personal benefit or privacy."
			+"\n"
			+"\n"
			+"Notwithstanding the above, using this IS does not constitute consent to PM, LE or CI investigative searching or monitoring of the content of privileged "
			+"\n"
			+"communications, or work product, related to personal representation or services by attorneys, psychotherapists, or clergy, and their assistants. "
			+"\n"
			+"Such communications and work product are private and confidential. See User Agreement for details.";
	
	/**
	 * Method main. Starts the SEMOSS application.
	 *  read the properties file - DBCM_RDF_Map.Prop
	 * Creates the PlayPane
	 * 1. Read the perspective properties to get all the perspectives.
	 * 2. For each of the perspectives, read all the question numbers.
	 * 3. For each question, get the description.
	 * 4. Convert this into a 2 dimensional Hashtable Hash1 - Perspective Questions, Hash2 for each question description and layout classes
	 * 5. Set this information into the util DIHelper class
	 * 6. Populate the perspective combo-boxes with all the information retrieved on perspectives
	 * 7. Create the User Interface.
	 * @param args String[] - the Main method.
	 */
	public static void main(String [] args) throws Exception
	{
		Starter starter = new Starter();
		System.setProperty("file.separator", "/");
		String workingDir = System.getProperty("user.dir");
		String propFile = workingDir + "/RDF_Map.prop";
		Logger logger = Logger.getLogger(prerna.ui.main.Starter.class);
		//Object monitor = new Object(); // stupid object for being a monitor
		
		//logger.setLevel(Level.INFO);
		PropertyConfigurator.configure(workingDir + "/log4j.prop");
		
		// Nimbus me
		
		try {
			for (LookAndFeelInfo info : UIManager.getInstalledLookAndFeels()) {
			if ("Nimbus".equals(info.getName())) {
				logger.info("Got the nimbus");
				UIManager.setLookAndFeel(info.getClassName());
				//Pretty Colors.
				UIManager.put("nimbusSelectionBackground", new Color(67,144,212)); //Light blue for selection
				//UIManager.put("nimbusBase", new Color(102,161,210)); //Light blue for everything else
				//UIManager.put("control", new Color(225,225,225)); //Light gray for the top bars behind the tabs
				//UIManager.put("nimbusBlueGrey", new Color(150,150,150)); //Separator bar, and disabled fields.
				UIManager.put("controlDkShadow", new Color(100,100,100)); //Color of scroll icons arrows
				UIManager.put("controlHighlight", new Color(100,100,100)); //Color of scroll icons highlights 
				UIManager.put("ProgressBar.repaintInterval", new Integer(100));//speed of indeterminate progress bar
				UIManager.put("ProgressBar.cycleTime", new Integer(1300));

				//UIManager.put("text", new Color(50,50,50)); //Color of text
				
				// comment this for nimbus
				//UIManager.setLookAndFeel ( WebLookAndFeel.class.getCanonicalName () );	
				UIDefaults defaults = UIManager.getLookAndFeelDefaults();
				
				UIDefaults tabPaneDefaults = new UIDefaults();
			    	tabPaneDefaults.put("TabbedPane.background", new ColorUIResource(Color.red));
		  	    //UIUtils.setPreferredLookAndFeel();

				//defaults.put("nimbusOrange",defaults.get("nimbusInfoBlue"));
				//defaults.put("Button.background",  Color.white);
				//defaults.put("TabbedPane.background", new Color(0,0,0));
				defaults.put("ToolTip.background", Color.LIGHT_GRAY);
				defaults.put("ToolTip[Enabled].backgroundPainter", null);
				defaults.put("ToolTip.contentMargins", new Insets(3,3,3,3));
				
				break;
				}
			}
		} catch (RuntimeException ignored) 
		{
			// handle exception
			logger.debug(ignored);
		} 

		// first get the engine file
		DIHelper.getInstance().loadCoreProp(propFile);
		if(DIHelper.getInstance().getProperty("SHOW_GOV_WARNING") != null){
			if(Boolean.parseBoolean(DIHelper.getInstance().getProperty("SHOW_GOV_WARNING")))
				Utility.showMessage(govWarningString);
		}
		File baseFolderCheckFile = new File(DIHelper.getInstance().getProperty("BaseFolder"));
		if (!(baseFolderCheckFile.exists() && baseFolderCheckFile.isDirectory())) {
			DIHelper.getInstance().putProperty("BaseFolder",System.getProperty("user.dir"));
		}
		File logCheckFile = new File(DIHelper.getInstance().getProperty("LOG4J"));
		if (!logCheckFile.exists()) {
			DIHelper.getInstance().putProperty("LOG4J",System.getProperty("user.dir")+"\\log4j.prop");
		}
		File smssWatcherCheckFile = new File(DIHelper.getInstance().getProperty("SMSSWatcher_DIR"));
		if (!smssWatcherCheckFile.exists()) {
			DIHelper.getInstance().putProperty("SMSSWatcher_DIR",System.getProperty("user.dir")+"\\db");
		}
		

		// get the engine name
		//String engines = DIHelper.getInstance().getProperty(Constants.ENGINES);
		String engines = "";
		
		DIHelper.getInstance().setLocalProperty(Constants.ENGINES, engines);
		
		PlayPane frame = new PlayPane();
		frame.setVisible(true);
		SplashScreen ss = new SplashScreen();
		frame.start();
		
		// need to parameterize this sucker and let it roll
		// ok Load the ENGINE watcher first
		String watcherStr = DIHelper.getInstance().getProperty(Constants.ENGINE_WATCHER);
		StringTokenizer watchers = new StringTokenizer(watcherStr, ";");
		while(watchers.hasMoreElements())
		{
			String watcher = watchers.nextToken();
			String watcherClass = DIHelper.getInstance().getProperty(watcher);
			String folder = DIHelper.getInstance().getProperty(watcher + "_DIR");
			String ext = DIHelper.getInstance().getProperty(watcher + "_EXT");
			AbstractFileWatcher watcherInstance = (AbstractFileWatcher)Class.forName(watcherClass).getConstructor(null).newInstance(null);
			watcherInstance.setMonitor(starter.monitor);
			watcherInstance.setFolderToWatch(folder);
			watcherInstance.setExtension(ext);
			synchronized(starter.monitor)
			{
				watcherInstance.loadFirst();
				Thread thread = new Thread(watcherInstance);
				thread.start();
			}
		}
		
		// get this into a synchronized block
		// so this guy will wait
		// I do this so that I can get reference to the engine when I need it
		synchronized(starter.monitor)
		{
			watcherStr = DIHelper.getInstance().getProperty(Constants.WATCHERS);
			if(watcherStr != null )
			{
				watchers = new StringTokenizer(watcherStr, ";");
				while(watchers.hasMoreElements())
				{
					String watcher = watchers.nextToken();
					String watcherClass = DIHelper.getInstance().getProperty(watcher);
					String folder = DIHelper.getInstance().getProperty(watcher + "_DIR");
					String ext = DIHelper.getInstance().getProperty(watcher + "_EXT");
					String engineName = DIHelper.getInstance().getProperty(watcher+"_ENGINE");
					try
					{
						AbstractFileWatcher watcherInstance = (AbstractFileWatcher)Class.forName(watcherClass).getConstructor(null).newInstance(null);
						// engines should be loaded by now
						// hopefully :D
						if(engineName != null && DIHelper.getInstance().getLocalProp(engineName) != null)
						{
							IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(engineName);
							watcherInstance.setEngine(engine);
						}
						watcherInstance.setMonitor(starter.monitor);
						watcherInstance.setFolderToWatch(folder);
						watcherInstance.setExtension(ext);
						watcherInstance.loadFirst();
						Thread thread = new Thread(watcherInstance);
						thread.start();
					}catch(RuntimeException ex)
					{
						// ok dont do anything the file was not there
						logger.debug(ex);
					}
				}
			}
		}
		
		ss.setVisible(false);
	}
}
