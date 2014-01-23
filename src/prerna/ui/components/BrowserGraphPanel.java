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
import java.util.Hashtable;

import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JPanel;

import org.apache.log4j.Logger;

import prerna.util.Constants;
import prerna.util.DIHelper;

import com.google.gson.Gson;
import com.teamdev.jxbrowser.Browser;
import com.teamdev.jxbrowser.BrowserFactory;
import com.teamdev.jxbrowser.BrowserServices;
import com.teamdev.jxbrowser.BrowserType;
import com.teamdev.jxbrowser.UnsupportedBrowserTypeException;
import com.teamdev.jxbrowser.prompt.SilentPromptService;

/**
 * This class is used to create the appropriate window depending on the browser specified (Mozilla, IE, Safari).
 */
public class BrowserGraphPanel extends JPanel{
	Logger logger = Logger.getLogger(getClass());
	public Browser browser = null;
	/**
	 * Constructor for BrowserGraphPanel.
	 * @param fileName 	File name.
	 * @return boolean 	True if method completed without error.
	 */
	public BrowserGraphPanel(String fileName) throws UnsupportedBrowserTypeException
	{
//		try{
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
		browser.navigate("file://" + workingDir + fileName);
		browser.waitReady();
		setLayout(new BorderLayout());
		add(browser.getComponent(), BorderLayout.CENTER);
//		}
//		catch(UnsupportedBrowserTypeException e){
//			displayCheckBoxError();
//			return false;
//		}
//		return true;
	}
	/**
	 * This method is used to convert a hashtable via GSON.
	 * GSON is a Java library used to convert Java objects into their JSON representations.
	 * This value is then passed to Javascript.
	 * @param table 	Hashtable to be converted to JSON format.
	 */
	public void callIt(Hashtable table)
	{


		//Hashtable newHash = new Hashtable();
		//newHash.put("Nodes", table);
		//newHash.put("Edges", edgeHash);

		Gson gson = new Gson();
		logger.info("Converted " + gson.toJson(table));

		//webBrowser.executeJavascript("helloWorld('" + gson.toJson(newHash) + "');"); //Please tell me this is awesome !!!!!!');");
		//Please tell me this is awesome !!!!!!');");
		browser.executeScript("start('" + gson.toJson(table) + "');");
	}
//	/**
//	 * Method displayCheckBoxError.
//	 * 
//	 */
//	public void displayCheckBoxError(){
//		JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
//		JOptionPane.showMessageDialog(playPane, "Mozilla15 engine doesn't support the current environment. Please switch to 32-bit Java.", "Error", JOptionPane.ERROR_MESSAGE);
//		
//	}
}
