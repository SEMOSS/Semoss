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
package prerna.ui.components.playsheets;

import java.lang.reflect.InvocationTargetException;
import java.util.Hashtable;
import java.util.StringTokenizer;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.BTreeDataFrame;
import prerna.engine.api.IEngine;
import prerna.util.DIHelper;
import prerna.util.PlaySheetEnum;

/**
 * This class is a temporary fix for queries to run across multiple databases
 * The query passed through this class must have the format engine1&engine2&engine1query&engine2query
 * The two queries must have exactly one variable name in common--which is how this class will line up the table
 */
public class DualEngineGenericPlaySheet extends DualEngineGridPlaySheet {

	private static final Logger logger = LogManager.getLogger(DualEngineGenericPlaySheet.class.getName());
	String playsheetName;
	BasicProcessingPlaySheet playSheet = null;

	@Override
	public Hashtable<String, String> getDataTableAlign() {
		return playSheet.getDataTableAlign();
	}
	
	@Override
	public Hashtable getData(){
		String playSheetClassName = PlaySheetEnum.getClassFromName(playsheetName);
		try {
			playSheet = (BasicProcessingPlaySheet) Class.forName(playSheetClassName).getConstructor(null).newInstance(null);
		} catch (ClassNotFoundException ex) {
			ex.printStackTrace();
			logger.fatal("No such PlaySheet: "+ playSheetClassName);
		} catch (InstantiationException e) {
			e.printStackTrace();
			logger.fatal("No such PlaySheet: "+ playSheetClassName);
		} catch (IllegalAccessException e) {
			logger.fatal("No such PlaySheet: "+ playSheetClassName);
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			logger.fatal("No such PlaySheet: "+ playSheetClassName);
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			logger.fatal("No such PlaySheet: "+ playSheetClassName);
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			logger.fatal("No such PlaySheet: "+ playSheetClassName);
			e.printStackTrace();
		} catch (SecurityException e) {
			logger.fatal("No such PlaySheet: "+ playSheetClassName);
			e.printStackTrace();
		}
		Hashtable hash = new Hashtable();
		String[] names = this.getNames();
		for(String name : names){
			hash.put(name, name);
		}
		ITableDataFrame f = new BTreeDataFrame(names);
		f.addRow(hash, hash);
		playSheet.setDataFrame(f);
		playSheet.setQuestionID(this.questionNum);
		Hashtable retHash = playSheet.getData();
		retHash.put("data", this.getList());
		return retHash;
	}
	
	/**
	 * Sets the String version of the SPARQL query on the play sheet. <p> The query must be set before creating the model for
	 * visualization.  Thus, this function is called before createView(), extendView(), overlayView()--everything that 
	 * requires the play sheet to pull data through a SPARQL query.
	 * @param query the full SPARQL query to be set on the play sheet
	 * @see	#createView()
	 * @see #extendView()
	 * @see #overlayView()
	 */
	@Override
	public void setQuery(String query) {

		StringTokenizer queryTokens = new StringTokenizer(query, "&");
		for (int queryIdx = 0; queryTokens.hasMoreTokens(); queryIdx++){
			String token = queryTokens.nextToken();
			if (queryIdx == 0){
				this.engineName1 = token;
				this.engine1 = (IEngine) DIHelper.getInstance().getLocalProp(engineName1);
			}
			else if (queryIdx == 1){
				this.engineName2 = token;
				this.engine2 = (IEngine) DIHelper.getInstance().getLocalProp(engineName2);
			}
			else if (queryIdx == 2)
				this.query1 = token;
			else if (queryIdx == 3)
				this.query2 = token;
			else if (queryIdx == 4)
				this.playsheetName = token;
			else if (queryIdx == 5)
				this.match1 = Boolean.parseBoolean(token);
			else if (queryIdx == 6)
				this.match2 = Boolean.parseBoolean(token);
		}
	}
}
