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
package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.nlp.IntakePortal;
import prerna.algorithm.nlp.QuestionNLP;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.BigDataEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

import com.bigdata.rdf.model.BigdataURIImpl;

public class NLPSearchMasterDB {
	private static final Logger logger = LogManager.getLogger(NLPSearchMasterDB.class.getName());
	
	//engine variables
	String masterDBName = "MasterDatabase";
	IEngine masterEngine = (BigDataEngine)DIHelper.getInstance().getLocalProp(masterDBName);

	protected final static String semossURI = "http://semoss.org/ontologies";
	protected final static String conceptBaseURI = semossURI + "/" + Constants.DEFAULT_NODE_CLASS;

	protected final static String allMasterConceptsQuery = "SELECT DISTINCT ?MasterConcept WHERE {{?MasterConcept <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/MasterConcept>}}";

	double similarityThresh = .7022;
	
	public void setMasterDBName(String masterDBName) {
		this.masterDBName = masterDBName;
		this.masterEngine = (BigDataEngine)DIHelper.getInstance().getLocalProp(masterDBName);
	}
	
	public List<Hashtable<String,Object>> findRelatedQuestions(String nlpQuestion) {
		QuestionNLP qp = new QuestionNLP();
		List<String[]> relationshipList = qp.Question_Analyzer(nlpQuestion);
		List<String> vertList = new ArrayList<String>();
		List<String> edgeInList = new ArrayList<String>();
		List<String> edgeOutList = new ArrayList<String>();

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
			logger.info("NLP found relationship between " + subj + " AND "+obj);
		}
		
		List<String> nodeList = qp.Question_Analyzer2(nlpQuestion);
		for(String node : nodeList) {
			Utility.cleanString(node,true);
			if(!vertList.contains(node))
				vertList.add(node);
			logger.info("NLP found node "+node);
		}
		
		if(relationshipList.isEmpty()) {
			logger.info("NLP of input string returned no relationships.");
		}
		if(vertList.isEmpty()) {
			logger.info("NLP of input string returned no nodes");
			return new ArrayList<Hashtable<String,Object>>();
		}
		
		List<String> mcsForKeywords = getMCsForKeywords(vertList);
		
		SearchMasterDB searchAlgo = new SearchMasterDB();
		searchAlgo.setKeywordAndEdgeList(vertList, edgeOutList, edgeInList);
		searchAlgo.setMCsForKeywords(mcsForKeywords);
		return searchAlgo.findRelatedQuestions();
	}
	
	/**
	 * 
	 * @param keywordList
	 * @return
	 */
	private List<String> getMCsForKeywords(List<String> keywordList) {
		List<String> masterConceptsList = new ArrayList<String>(keywordList.size());
		for(int i=0;i<keywordList.size();i++)
			masterConceptsList.add("");
		//if all the keywords have a master concept, then link them	
		//if there are keywords without masterconcepts, use word net to see if there are related.
		//this primarily will happen when input comes from NLP processing.
		ArrayList<String> allMCs = processListQuery(masterEngine,allMasterConceptsQuery,false);
		double[][] simScores;
		simScores = IntakePortal.DBRecSimScore(deepCopy(keywordList),deepCopy(allMCs));
		for(int keyInd=0;keyInd<keywordList.size();keyInd++) {
			String keyword = keywordList.get(keyInd);
			keywordList.set(keyInd, Utility.cleanString(keyword,true));
		}
		for(int keyInd=0;keyInd<keywordList.size();keyInd++) {
			int mcInd = 0;
			while(mcInd<allMCs.size()) {
				if(simScores[keyInd][mcInd]>similarityThresh) {
					masterConceptsList.set(keyInd,allMCs.get(mcInd));
					logger.info("Master Concept for the NLP search term "+keywordList.get(keyInd) + " is " + allMCs.get(mcInd));
					mcInd=allMCs.size();
				}
				mcInd++;
			}
			if(masterConceptsList.get(keyInd).equals("")) {
				logger.error("No Master Concept was found for the NLP search term "+keywordList.get(keyInd));
			}
		}
		return masterConceptsList;
	}
	
	private List<String> deepCopy(List<String> list) {
		List<String> copy = new ArrayList<String>();
		for(String entry : list) {
			copy.add(entry);
		}
		return copy;
	}
	
	/**
	 * Executes a query and stores the results.
	 * @param query String to run.
	 * @return ArrayList<String> that contains the results of the query.
	 */
	private ArrayList<String> processListQuery(IEngine engine,String query,Boolean getURI) {
		SesameJenaSelectWrapper wrapper = Utility.processQuery(engine,query);
		ArrayList<String> list = new ArrayList<String>();
		// get the bindings from it
		String[] names = wrapper.getVariables();
		// now get the bindings and generate the data
		while(wrapper.hasNext())
		{
			SesameJenaSelectStatement sjss = wrapper.next();
			if(getURI) {
				String value = ((BigdataURIImpl)sjss.getRawVar(names[0])).stringValue();
				list.add(value);
			}else {
				Object value = sjss.getVar(names[0]);
				list.add((String)value);
			}
		}
		return list;
	}

}
