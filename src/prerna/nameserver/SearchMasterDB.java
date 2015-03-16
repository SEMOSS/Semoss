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
package prerna.nameserver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.impl.RemoteSemossSesameEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class SearchMasterDB extends ModifyMasterDB {
	
	private WordnetComparison wnComp;
	
	public SearchMasterDB(String localMasterDbName, String wordnetDir, String lpDir) {
		super(localMasterDbName);
		wnComp = new WordnetComparison(wordnetDir, lpDir);
	}
	
	public SearchMasterDB(String wordnetDir, String lpDir) {
		super();
		wnComp = new WordnetComparison(wordnetDir, lpDir);
	}

	public List<Hashtable<String,Object>> getRelatedInsights(List<String> instanceURIList) {
		String keywordURI = MasterDatabaseURIs.KEYWORD_BASE_URI + "/" + Utility.getClassName(instanceURIList.get(0));
		// track all keywords
		Set<String> keywordSet = new HashSet<String>();
		// track all the engines that contain the keywords to speed up instance search
		Map<String, Set<String>> engineKeywordMap = new HashMap<String, Set<String>>();
		MasterDBHelper.findRelatedKeywordsToSpecificURI(masterEngine, keywordURI, keywordSet, engineKeywordMap);
		
		Map<String, Double> similarKeywordScores = new HashMap<String, Double>();
		String query = formInsightsForKeywordsQuery(keywordURI, keywordSet, similarKeywordScores);

		// check if instance exists in the engines
		Map<String, Map<String, String>> engineInstances = new HashMap<String, Map<String, String>>();
		for(String engineName : engineKeywordMap.keySet()) {
			Set<String> keywordList = engineKeywordMap.get(engineName);
			String getUsableInstancesQuery = formUsableInsightsQuery(keywordList,instanceURIList);
			
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
			ISelectWrapper sjsw = Utility.processQuery(engine, getUsableInstancesQuery);
			
			Map<String, String> typeAndInstance = getTypeAndInstance(sjsw);
			engineInstances.put(engineName, typeAndInstance);
		}
		
		List<Hashtable<String,Object>> insightList = new ArrayList<Hashtable<String,Object>>();
		ISelectWrapper sjsw = Utility.processQuery(masterEngine, query);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String engine = sjss.getVar(names[0]).toString();
			String insightLabel = sjss.getVar(names[1]).toString();
			String keyword = sjss.getRawVar(names[2]).toString();
			String perspectiveLabel = sjss.getVar(names[3]).toString();
			String viz = sjss.getVar(names[4]).toString();
			
			if(engineInstances.containsKey(engine)) {
				Map<String, String> typeAndInstance = engineInstances.get(engine);
				String typeURI = MasterDatabaseURIs.SEMOSS_CONCEPT_URI.concat("/").concat(Utility.getInstanceName(keyword));
				if(typeAndInstance != null && typeAndInstance.containsKey(typeURI)) {
					Hashtable<String, Object> insightHash = new Hashtable<String, Object>();
					insightHash.put(MasterDatabaseConstants.DB_KEY, engine);
					insightHash.put(MasterDatabaseConstants.QUESITON_KEY, insightLabel);
					insightHash.put(MasterDatabaseConstants.TYPE_KEY, typeURI);
					insightHash.put(MasterDatabaseConstants.PERSPECTIVE_KEY, perspectiveLabel);
					insightHash.put(MasterDatabaseConstants.VIZ_TYPE_KEY, viz);
					insightHash.put(MasterDatabaseConstants.SCORE_KEY, 1.0 - similarKeywordScores.get(keyword));
					insightHash.put(MasterDatabaseConstants.INSTANCE_KEY, typeAndInstance.get(typeURI));
					insightList.add(insightHash);
				}
			}
		}
		
		return insightList;
	}
	
	public List<Hashtable<String,Object>> getRelatedInsightsWeb(List<String> instanceURIList) {
		Hashtable<String, String> engineURLHash = new Hashtable<String, String>();
		MasterDBHelper.fillAPIHash(masterEngine, engineURLHash);

		String keywordURI = MasterDatabaseURIs.KEYWORD_BASE_URI + "/" + Utility.getClassName(instanceURIList.get(0));
		
		// track all keywords
		Set<String> keywordSet = new HashSet<String>();
		// track all the engines that contain the keywords to speed up instance search
		Map<String, Set<String>> engineKeywordMap = new HashMap<String, Set<String>>();
		MasterDBHelper.findRelatedKeywordsToSpecificURI(masterEngine, keywordURI, keywordSet, engineKeywordMap);
		
		Map<String, Double> similarKeywordScores = new HashMap<String, Double>();
		String query = formInsightsForKeywordsQuery(keywordURI, keywordSet, similarKeywordScores);
		
		// check if instance exists in the engines
		Map<String, Map<String, String>> engineInstances = new HashMap<String, Map<String, String>>();
		for(String engineName : engineKeywordMap.keySet()) {
			Set<String> keywordList = engineKeywordMap.get(engineName);
			String getUsableInstancesQuery = formUsableInsightsQuery(keywordList,instanceURIList);
			
			// this will call the engine and gets then flushes it into sesame jena construct wrapper
			String engineAPI = engineURLHash.get(engineName);
			RemoteSemossSesameEngine engine = new RemoteSemossSesameEngine();
			engine.setAPI(engineAPI);
			engine.setDatabase(engineName);
			
			ISelectWrapper sjsw = WrapperManager.getInstance().getSWrapper(engine,  getUsableInstancesQuery);
			
			Map<String, String> typeAndInstance = getTypeAndInstance(sjsw);
			engineInstances.put(engineName, typeAndInstance);
		}
		
		List<Hashtable<String,Object>> insightList = new ArrayList<Hashtable<String,Object>>();
		ISelectWrapper sjsw = Utility.processQuery(masterEngine, query);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String engine = sjss.getVar(names[0]).toString();
			String insightLabel = sjss.getVar(names[1]).toString();
			String keyword = sjss.getRawVar(names[2]).toString();
			String perspectiveLabel = sjss.getVar(names[3]).toString();
			String viz = sjss.getVar(names[4]).toString();
			
			if(engineInstances.containsKey(engine)) {
				Map<String, String> typeAndInstance = engineInstances.get(engine);
				String typeURI = MasterDatabaseURIs.SEMOSS_CONCEPT_URI.concat("/").concat(Utility.getInstanceName(keyword));
				if(typeAndInstance != null && typeAndInstance.containsKey(typeURI)) {
					Hashtable<String, Object> insightHash = new Hashtable<String, Object>();
					insightHash.put(MasterDatabaseConstants.DB_KEY, engine);
					insightHash.put(MasterDatabaseConstants.QUESITON_KEY, insightLabel);
					insightHash.put(MasterDatabaseConstants.TYPE_KEY, typeURI);
					insightHash.put(MasterDatabaseConstants.PERSPECTIVE_KEY, perspectiveLabel);
					insightHash.put(MasterDatabaseConstants.VIZ_TYPE_KEY, viz);
					insightHash.put(MasterDatabaseConstants.SCORE_KEY, 1.0 - similarKeywordScores.get(keyword));
					insightHash.put(MasterDatabaseConstants.INSTANCE_KEY, typeAndInstance.get(typeURI));
					String engineURI = engineURLHash.get(engine);
					insightHash.put(MasterDatabaseConstants.ENGINE_URI_KEY, engineURI);
					insightList.add(insightHash);
				}
			}
		}
		return insightList;
	}
	
	private String formUsableInsightsQuery(Set<String> keywordList, List<String> instanceURIList) {
		int numInstances = instanceURIList.size();
		String bindingsStr = "";
		Iterator<String> keywordIt = keywordList.iterator();
		while(keywordIt.hasNext()) {
			String keywordName = Utility.getInstanceName(keywordIt.next());
			int j = 0;
			for(; j < numInstances; j++) {
				String instanceBaseURI = Utility.getBaseURI(instanceURIList.get(j)).concat("/Concept");
				bindingsStr = bindingsStr.concat("(<").concat(instanceBaseURI).concat("/").concat(keywordName).concat("/").concat(Utility.getInstanceName(instanceURIList.get(j))).concat(">)");
			}
		}
		return MasterDatabaseQueries.INSTANCE_EXISTS_QUERY.replace("@BINDINGS@", bindingsStr);
	}
	
	private String formInsightsForKeywordsQuery(String keywordURI, Set<String> keywordSet, Map<String, Double> similarKeywordScores) {
		// get list of insights for keywords if the score is above threshold
		List<String> similarKeywordList = new ArrayList<String>();
		
		for(String otherKeywordURI : keywordSet) {
			double simScore = wnComp.compareKeywords(keywordURI, otherKeywordURI);
			if(wnComp.isSimilar(simScore)) {
				similarKeywordList.add(otherKeywordURI);
				similarKeywordScores.put(otherKeywordURI, simScore);
			}
		}

		String keywords = "";
		int i = 0;
		int size = similarKeywordList.size();
		for(; i < size; i++) {
			keywords = keywords.concat("(<").concat(similarKeywordList.get(i)).concat(">)");
		}
		
		return MasterDatabaseQueries.GET_INSIGHTS_FOR_KEYWORDS.replace("@KEYWORDS@", keywords);
	}
	
	private Map<String, String> getTypeAndInstance(ISelectWrapper sjsw) {
		String[] names = sjsw.getVariables();

		Map<String, String> typeAndInstance = new HashMap<String, String>();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			typeAndInstance.put(sjss.getRawVar(names[0]).toString(), sjss.getRawVar(names[1]).toString());
		}
		return typeAndInstance;
	}
	
}
