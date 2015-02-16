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
package prerna.ui.components.specific.tap;

import java.util.List;
import java.util.StringTokenizer;

import javax.swing.JList;
import javax.swing.JProgressBar;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaUpdateWrapper;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * 
 */
public class TransitionCostInsert implements Runnable {

	static final Logger logger = LogManager.getLogger(TransitionCostInsert.class.getName());
	JProgressBar jBar;
	String Query;
	
	/**
	 * Parses queries based on transition query separator and passes each to be inserted
	 */
	public void processQueries(){
		jBar = (JProgressBar) DIHelper.getInstance().getLocalProp(Constants.TCCALC_PROGRESS_BAR);
		jBar.setVisible(true);
		jBar.setStringPainted(true);
		progressBarUpdate("0%...Preprocessing",0);
		
		StringTokenizer queryTokens = new StringTokenizer(Query, Constants.TRANSITION_QUERY_SEPARATOR);
		int tokenCount = queryTokens.countTokens();
		for(int i = 1; i<=tokenCount; i++) {
			String query = (String) queryTokens.nextElement();
			logger.info("Inserting query... " + query);
			Double d = (double) ((i-1)*90/tokenCount +10);
			progressBarUpdate(d+"%...Processing Transition Cost Query "+i +" out of "+tokenCount, d.intValue());
			runInsert(query);
		}
		progressBarUpdate("100%...Calculated Transition Costs Sucessfully Inserted Into RDF Store", 100);
	}

	/**
	 * Executes appropriate insert queries for Transition Cost function
	 * 
	 * @param query String	Query to be executed/inserted
	 */
	public void runInsert(String query) {
		// now just do class.forName for this layout Value and set it inside playsheet
		// need to template this out and there has to be a directive to identify 
		// specifically what sheet we need to refer to
		
		JList<Object> list = (JList<Object>) DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		// get the selected repository
		List<Object> repos = list.getSelectedValuesList();
		logger.info("Repository is " + repos);
		
		for(Object obj : repos) {
			IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(obj.toString());

			SesameJenaUpdateWrapper insertWrapper = new SesameJenaUpdateWrapper();
			insertWrapper.setEngine(engine);
			insertWrapper.setQuery(query);
			insertWrapper.execute();
		}
	}
	
	/**
	 * Inherited method - calls processQueries()
	 */
	public void run() {
		processQueries();
	}
	
	/**
	 * Updates progress bar text and value as queries are executed
	 * 
	 * @param status String		Text value to be set
	 * @param x int				Percentage value to be set
	 */
	private void progressBarUpdate(String status, int x)
	{
		jBar.setString(status);
		jBar.setValue(x);
	}

	/**
	 * Sets class var Query
	 * 
	 * @param Q String	Value of the query to be set
	 */
	public void setQuery(String Q) {
		Query = Q;
	}	
	

}
