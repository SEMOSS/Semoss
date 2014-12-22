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
package prerna.ui.components.playsheets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.impl.NLPSearchMasterDB;

/**
 * The SearchMasterDBPlaySheet class is used to test the Search feature for the MasterDB.
 */
@SuppressWarnings("serial")
public class NLPSearchPlaySheet extends GridPlaySheet{

	static final Logger LOGGER = LogManager.getLogger(NLPSearchPlaySheet.class.getName());
	/**
	 * Method createData.  Creates the data needed to be printout in the grid.
	 */
	@Override
	public void createData() {
		
		NLPSearchMasterDB searchAlgo = new NLPSearchMasterDB();
		searchAlgo.setMasterDBName(this.engine.getEngineName());	
		ArrayList<Hashtable<String, Object>> hashArray = searchAlgo.findRelatedQuestions(query);
		flattenHash(hashArray);
	}
	
	private void flattenHash(ArrayList<Hashtable<String, Object>> hashArray){
		//TODO write this method that stores headers and list
		//assuming every hash has the same keys
		//get the first hash to know what keys we are working with (these are going to be our headers)
		if(hashArray.size()>0)
		{
			list = new ArrayList<Object []>();
			Hashtable<String, Object> exampleHash = hashArray.get(0);
			Collection<String> keySet = exampleHash.keySet();
			this.names = new String[keySet.size()];
			Iterator<String> keyIt = keySet.iterator();
			for(int namesIdx = 0; keyIt.hasNext(); namesIdx++){
				this.names[namesIdx] = keyIt.next();
			}
			
			// now that names has been created, just need to fill out the list to match the headers
			for(Hashtable<String,Object> hash : hashArray){
				Object[] newRow = new Object[this.names.length];
				for(int namesIdx = 0; namesIdx<this.names.length; namesIdx++)
				{
					newRow[namesIdx] = hash.get(this.names[namesIdx]);
				}
				list.add(newRow);
			}
				
		}
	}
}
