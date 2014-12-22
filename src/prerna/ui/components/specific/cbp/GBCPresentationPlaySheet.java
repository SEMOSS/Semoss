/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.ui.components.specific.cbp;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.Insets;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Properties;

import javax.swing.JDesktopPane;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.AbstractRDFPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class GBCPresentationPlaySheet extends AbstractRDFPlaySheet{

	private static final Logger logger = LogManager.getLogger(GBCPresentationPlaySheet.class.getName());
	Properties presProps = new Properties();
	String propsKey;
	IPlaySheet realPlaySheet;
	String playSheetClassName;


	boolean update = false;
	
	public GBCPresentationPlaySheet() 
	{
		this.setPreferredSize(new Dimension(800,600));

		Properties props = (Properties) DIHelper.getInstance().getLocalProp("GBC_PROPERTIES");
		
		if(props == null)
		{
			String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
			String fileName = workingDir + "/GBCPresentationV2.properties";
			try {
				presProps.load(new FileInputStream(fileName));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else
			presProps = props;
	}
	
	@Override
	public void createData()
	{		
		if(update){
			updateProperties(query);
			return;
		}
		// this will do everything for the real play sheet

		// now that the query is set lets create the playsheet and set those things
		if(this.playSheetClassName == null)
			playSheetClassName = presProps.getProperty(propsKey + "_LAYOUT");
		
		try {
			this.realPlaySheet = (IPlaySheet) Class.forName(playSheetClassName).getConstructor(null).newInstance(null);
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.realPlaySheet.setTitle(this.title);
		this.realPlaySheet.setRDFEngine(this.engine);
		this.realPlaySheet.setQuestionID(this.propsKey);
		this.realPlaySheet.setQuery(this.query);
		JDesktopPane pane = (JDesktopPane) DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);
		this.realPlaySheet.setJDesktopPane(pane);

		// cannot use the create runner because i don't know if it is web or not at this point
		this.realPlaySheet.createData();
		this.realPlaySheet.runAnalytics();
	}
	
	@Override
	public void setQuery(String query1) {
		this.propsKey = query1;
		
		if(!this.propsKey.startsWith("UPDATE"))
		{
			//query must be specified on the prop sheet or it can be a pointer to a generic query key
			this.query = presProps.getProperty(propsKey+"_QUERY");
			if(query == null) // this would happen when an actual query is passed in not just a props key
				this.query = query1;
			else if(presProps.getProperty(query) != null)
				query = presProps.getProperty(query);
			
			//for each param see if defined specifically otherwise get the value for the whole presentation
			Hashtable<String, String> filledParams = new Hashtable<String, String>();
			Hashtable<String, String> params = Utility.getParams(this.query);
			for (String param : params.keySet()){
				logger.info("Setting param " + param);
				String paramKey = param.substring(0, param.indexOf("-"));
				String paramValue = presProps.getProperty(propsKey+"_"+paramKey);
				if(paramValue == null)
					paramValue = presProps.getProperty(paramKey);
				filledParams.put(param, paramValue);
				logger.info("filling param " + paramKey + " with " + paramValue);
			}
			
			this.query = Utility.fillParam(this.query, filledParams);
			logger.info("filled final query : " + this.query);
		
		}
		else
		{
			this.query = query1.substring(6); // cut out UPDATE
			this.update = true;
		}
	}


	public void setGenericQuery(String queryKey, Hashtable<String, String> passedParams) {
		this.propsKey = "";
		
		//query must be specified on the prop sheet or it can be a pointer to a generic query key
		this.query = presProps.getProperty(queryKey);
		
		//for each param see if defined specifically otherwise get the value for the whole presentation
		Hashtable<String, String> filledParams = new Hashtable<String, String>();
		Hashtable<String, String> params = Utility.getParams(this.query);
		for (String param : params.keySet()){
			logger.info("Setting param " + param);
			String paramKey = param.substring(0, param.indexOf("-"));
			String paramValue = null;
			//first try to get from passed params
			if(passedParams.containsKey(paramKey))
				paramValue = passedParams.get(paramKey);
			//if it wasn't passed, get it from the properties file
			if(paramValue == null)
				paramValue = presProps.getProperty(paramKey);
			filledParams.put(param, paramValue);
			logger.info("filling param " + paramKey + " with " + paramValue);
		}
		
		this.query = Utility.fillParam(this.query, filledParams);
		logger.info("filled final query : " + this.query);
		
	}
	
	@Override
	public void createView(){
		if(!update){
			this.realPlaySheet.createView();
		}
		else{
			Utility.showMessage("Update Sucessful");
		}
	}

	@Override
	public void refineView() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void overlayView() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void runAnalytics() {
		// TODO Auto-generated method stub
		
	}
	
	public Object getData(){
		if(!update){
			return this.realPlaySheet.getData();
			
		}
		else return "Update Successful";
	}
	

	private void updateProperties(String propString)
	{
		logger.info("Time to update properties with " + propString);


		String [] tokens = propString.split(";");
		// format is prop1+++prop1Value;prop2+++prop2Value etc.
		for (int propIdx = 0; propIdx < tokens.length; propIdx++){
			String token = tokens[propIdx];
			int splitIdx = token.indexOf("+++");
			String paramName = token.substring(0,splitIdx);
			String paramValue = token.substring(splitIdx+3);
			if(!paramValue.isEmpty()){
				logger.info("Setting param " + paramName + " to " + paramValue);
				this.presProps.setProperty(paramName, paramValue);
			}
		}
		DIHelper.getInstance().setLocalProperty("GBC_PROPERTIES", presProps);
	}

	public void setPlaySheetClassName(String playSheetClassName) {
		this.playSheetClassName = playSheetClassName;
	}
	
}
