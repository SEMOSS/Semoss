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
