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
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.impl.QuestionNLP;
import prerna.algorithm.impl.SearchMasterDB;
import prerna.util.Utility;

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
		
		SearchMasterDB searchAlgo = new SearchMasterDB();
		searchAlgo.setMasterDBName(this.engine.getEngineName());

		QuestionNLP qp = new QuestionNLP();
		ArrayList<String[]> relationshipList = qp.Question_Analyzer(query);
		ArrayList<String> vertList = new ArrayList<String>();
		ArrayList<String> edgeInList = new ArrayList<String>();
		ArrayList<String> edgeOutList = new ArrayList<String>();
		if(relationshipList.isEmpty()) {
			LOGGER.info("NLP of input string returned no relationships.");
			Utility.showError("NLP of input string returned no relationships. Please try a different question.");
			return;
		}
		for(String [] relationship : relationshipList) {
			String subj = relationship[0];
			String obj = relationship[1];
			subj = Utility.cleanString(subj, true);
			obj = Utility.cleanString(obj, true);
			if(!vertList.contains(subj))
				vertList.add(subj);
			if(!vertList.contains(obj))
				vertList.add(obj);
			edgeOutList.add(subj);
			edgeInList.add(obj);
			LOGGER.info("NLP found relationship between " + subj + " AND "+obj);
		}
		searchAlgo.setKeywordAndEdgeList(vertList, edgeOutList, edgeInList);
		
		ArrayList<Hashtable<String, Object>> hashArray = searchAlgo.searchDB();
		flattenHash(hashArray);
	}
	
	private void flattenHash(ArrayList<Hashtable<String, Object>> hashArray){
		//TODO write this method that stores headers and list
		
	}
}
