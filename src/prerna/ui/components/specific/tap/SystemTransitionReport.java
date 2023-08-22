/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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
package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.Hashtable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.bigdata.rdf.model.BigdataURIImpl;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.playsheets.TablePlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

@SuppressWarnings("serial")
public class SystemTransitionReport extends TablePlaySheet{

	static final Logger logger = LogManager.getLogger(SystemTransitionReport.class);
	private IDatabaseEngine TAP_Core_Data;

	@Override
	public void createData() {
		try{
			TAP_Core_Data = (IDatabaseEngine) DIHelper.getInstance().getLocalProp("TAP_Core_Data");
			if(TAP_Core_Data==null)
				throw new NullPointerException();
		} catch(RuntimeException e) {
			Utility.showError("Could not find necessary database: TAP_Core_Data. Cannot generate report.");
			return;
		}

		String[] systemAndReport = query.split("\\$");
		this.query = systemAndReport[0];
		String reportType = systemAndReport[1];

		ISelectWrapper sjsw = processQuery(TAP_Core_Data, systemAndReport[0]);

		String[] names = sjsw.getVariables();
		ArrayList<String> systemList = new ArrayList<>();
		String systemListString = "";
		while(sjsw.hasNext())
		{
			ISelectStatement sjss = sjsw.next();
			BigdataURIImpl sysRawURI = (BigdataURIImpl)sjss.getRawVar(names[0]);
			String sysURI = "<"+sysRawURI.stringValue()+">";
			systemList.add(sysURI);
			systemListString += sysURI + " ";
		}
		logger.info("Creating System Transition Reports for systems >>> "+systemListString);

		for(int i=0;i<systemList.size();i++)
		{
			String sysURI = systemList.get(i);
			logger.info("Creating System Transition Report for system >>> " + sysURI + "$" + Utility.cleanLogString(reportType));
			IndividualSystemTransitionReport sysTransReport = new IndividualSystemTransitionReport();
			sysTransReport.enableMessages(false);
			sysTransReport.setQuery(sysURI + "$" + reportType);
			sysTransReport.createData();
		}

		String fileLoc = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "\\export\\Reports\\";
		Utility.showMessage("System Transition Reports Finished! Files located in:\n" +fileLoc);
	}

	private ISelectWrapper processQuery(IDatabaseEngine engine, String query){
		logger.info("PROCESSING QUERY: " + query);
		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);

		/*SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		//run the query against the engine provided
		sjsw.setEngine(engine);
		sjsw.setQuery(query);
		sjsw.executeQuery();	
		return sjsw;*/
		return wrapper;
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

	@Override
	public Hashtable<String, String> getDataTableAlign() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void processQueryData() {
		// TODO Auto-generated method stub
		
	}

}
