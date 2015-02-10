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
import prerna.rdf.engine.impl.RemoteSemossSesameEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class SearchMasterDB extends ModifyMasterDB {
	
	private WordnetComparison wnComp;
	private Hashtable<String,String> engineURLHash;
	
	public SearchMasterDB(String localMasterDbName, String wordnetDir, String lpDir) {
		super(localMasterDbName);
		wnComp = new WordnetComparison(wordnetDir, lpDir);
	}
	
	public SearchMasterDB(String wordnetDir, String lpDir) {
		super();
		wnComp = new WordnetComparison(wordnetDir, lpDir);
	}

	public List<Hashtable<String,Object>> getRelatedInsights(List<String> instanceURIList) {
		String keywordURI = KEYWORD_BASE_URI + "/" + Utility.getClassName(instanceURIList.get(0));
		
		// track all keywords and the nouns that make them up
		Map<String, Set<String>> keywordNounMap = new HashMap<String, Set<String>>();
		// track all the engines that contain the keywords to speed up instance search
		Map<String, Set<String>> engineKeywordMap = new HashMap<String, Set<String>>();
		
		findRelatedKeywords(keywordURI,keywordNounMap,engineKeywordMap);
		
		// check if instance exists in the engines
		Map<String, Map<String, String>> engineInstances = new HashMap<String, Map<String, String>>();
		
		for(String engineName : engineKeywordMap.keySet()) {
			Set<String> keywordList = engineKeywordMap.get(engineName);
			String getUsableInstancesQuery = formUsableInsightsQuery(keywordList,instanceURIList);
			
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
			SesameJenaSelectWrapper sjsw = Utility.processQuery(engine, getUsableInstancesQuery);
			
			Map<String, String> typeAndInstance = getTypeAndInstance(sjsw);
			engineInstances.put(engineName, typeAndInstance);
		}
		
		Map<String, Double> similarKeywordScores = new HashMap<String, Double>();
		String query = formInsightsForKeywordsQuery(keywordURI,keywordNounMap,similarKeywordScores);

		List<Hashtable<String,Object>> insightList = new ArrayList<Hashtable<String,Object>>();
		SesameJenaSelectWrapper sjsw = Utility.processQuery(masterEngine, query);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext()) {
			SesameJenaSelectStatement sjss = sjsw.next();
			String engine = sjss.getVar(names[0]).toString();
			String insightLabel = sjss.getVar(names[1]).toString();
			String keyword = sjss.getRawVar(names[2]).toString();
			String perspectiveLabel = sjss.getVar(names[3]).toString();
			String viz = sjss.getVar(names[4]).toString();
			
			if(engineInstances.containsKey(engine)) {
				Map<String, String> typeAndInstance = engineInstances.get(engine);
				String typeURI = SEMOSS_CONCEPT_URI.concat("/").concat(Utility.getInstanceName(keyword));
				if(typeAndInstance != null && typeAndInstance.containsKey(typeURI)) {
					Hashtable<String, Object> insightHash = new Hashtable<String, Object>();
					insightHash.put(DB_KEY, engine);
					insightHash.put(QUESITON_KEY, insightLabel);
					insightHash.put(TYPE_KEY, typeURI);
					insightHash.put(PERSPECTIVE_KEY, perspectiveLabel);
					insightHash.put(VIZ_TYPE_KEY, viz);
					insightHash.put(SCORE_KEY, similarKeywordScores.get(keyword));
					insightHash.put(INSTANCE_KEY, typeAndInstance.get(typeURI));
					insightList.add(insightHash);
				}
			}
		}
		
		return insightList;
	}
	
	public List<Hashtable<String,Object>> getRelatedInsightsWeb(List<String> instanceURIList) {
		fillAPIHash();
		String keywordURI = KEYWORD_BASE_URI + "/" + Utility.getClassName(instanceURIList.get(0));
		
		// track all keywords and the nouns that make them up
		Map<String, Set<String>> keywordNounMap = new HashMap<String, Set<String>>();
		// track all the engines that contain the keywords to speed up instance search
		Map<String, Set<String>> engineKeywordMap = new HashMap<String, Set<String>>();
		
		findRelatedKeywords(keywordURI,keywordNounMap,engineKeywordMap);
		
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
			
			SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
			sjsw.setEngine(engine);
			sjsw.setQuery(getUsableInstancesQuery);
			sjsw.executeQuery();
			
			Map<String, String> typeAndInstance = getTypeAndInstance(sjsw);
			engineInstances.put(engineName, typeAndInstance);
		}
		
		Map<String, Double> similarKeywordScores = new HashMap<String, Double>();
		String query = formInsightsForKeywordsQuery(keywordURI,keywordNounMap,similarKeywordScores);

		List<Hashtable<String,Object>> insightList = new ArrayList<Hashtable<String,Object>>();
		SesameJenaSelectWrapper sjsw = Utility.processQuery(masterEngine, query);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext()) {
			SesameJenaSelectStatement sjss = sjsw.next();
			String engine = sjss.getVar(names[0]).toString();
			String insightLabel = sjss.getVar(names[1]).toString();
			String keyword = sjss.getRawVar(names[2]).toString();
			String perspectiveLabel = sjss.getVar(names[3]).toString();
			String viz = sjss.getVar(names[4]).toString();
			
			if(engineInstances.containsKey(engine)) {
				Map<String, String> typeAndInstance = engineInstances.get(engine);
				String typeURI = SEMOSS_CONCEPT_URI.concat("/").concat(Utility.getInstanceName(keyword));
				if(typeAndInstance != null && typeAndInstance.containsKey(typeURI)) {
					Hashtable<String, Object> insightHash = new Hashtable<String, Object>();
					insightHash.put(DB_KEY, engine);
					insightHash.put(QUESITON_KEY, insightLabel);
					insightHash.put(TYPE_KEY, typeURI);
					insightHash.put(PERSPECTIVE_KEY, perspectiveLabel);
					insightHash.put(VIZ_TYPE_KEY, viz);
					insightHash.put(SCORE_KEY, similarKeywordScores.get(keyword));
					insightHash.put(INSTANCE_KEY, typeAndInstance.get(typeURI));
					String engineURI = engineURLHash.get(engine);
					insightHash.put(ENGINE_URI_KEY, engineURI);
					insightList.add(insightHash);
				}
			}
		}
		return insightList;
	}
	
	private void fillAPIHash(){
		engineURLHash = new Hashtable<String,String>();
		SesameJenaSelectWrapper wrapper = Utility.processQuery(masterEngine,ENGINE_API_QUERY);
		// get the bindings from it
		String[] names = wrapper.getVariables();
		// now get the bindings and generate the data
		while(wrapper.hasNext())
		{
			SesameJenaSelectStatement sjss = wrapper.next();
			String engine = (String)sjss.getVar(names[0]);
			String baseURI = (sjss.getRawVar(names[1])).toString();
			engineURLHash.put(engine,baseURI);
		}
	}
	
	private void findRelatedKeywords(String keywordURI,Map<String, Set<String>> keywordNounMap,Map<String, Set<String>> engineKeywordMap){
		// find all related keywords to the inputed data type
		String query = GET_RELATED_KEYWORDS_AND_THEIR_NOUNS.replace("@KEYWORD@", keywordURI);
		SesameJenaSelectWrapper sjsw = Utility.processQuery(masterEngine, query);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext()) {
			SesameJenaSelectStatement sjss = sjsw.next();
			String engine = sjss.getVar(names[0]).toString();
			String keyword = sjss.getRawVar(names[1]).toString();
			String noun = sjss.getRawVar(names[2]).toString();
			
			Set<String> nounList;
			if(keywordNounMap.containsKey(keyword)) {
				nounList = keywordNounMap.get(keyword);
				nounList.add(noun);
			} else {
				nounList = new HashSet<String>();
				nounList.add(noun);
				keywordNounMap.put(keyword, nounList);
			}
			
			Set<String> keywordForEngineList;
			if(engineKeywordMap.containsKey(engine)) {
				keywordForEngineList = engineKeywordMap.get(engine);
				keywordForEngineList.add(keyword);
			} else {
				keywordForEngineList = new HashSet<String>();
				keywordForEngineList.add(keyword);
				engineKeywordMap.put(engine, keywordForEngineList);
			}
		}
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
				bindingsStr += bindingsStr.concat("(<").concat(instanceBaseURI).concat("/").concat(keywordName).concat("/").concat(Utility.getInstanceName(instanceURIList.get(j))).concat(">)");
			}
		}
		return INSTANCE_EXISTS_QUERY.replace("@BINDINGS@", bindingsStr);
	}
	

	private String formInsightsForKeywordsQuery(String keywordURI, Map<String, Set<String>> keywordNounMap,Map<String, Double> similarKeywordScores) {
		// get list of insights for keywords if the score is above threshold
		List<String> similarKeywordList = new ArrayList<String>();
		similarKeywordList.add(keywordURI);
		similarKeywordScores.put(keywordURI, 1.0);
		
		Set<String> nounList = keywordNounMap.get(keywordURI);
		for(String otherKeywords : keywordNounMap.keySet()) {
			if(!otherKeywords.equals(keywordURI)) {
				double simScore = wnComp.compareKeywords(nounList, keywordNounMap.get(otherKeywords));
				if(wnComp.isSimilar(simScore)) {
					similarKeywordList.add(otherKeywords);
					similarKeywordScores.put(otherKeywords, 1.0 - simScore);
				}
			}
		}

		String keywords = "";
		int i = 0;
		int size = similarKeywordList.size();
		for(; i < size; i++) {
			keywords = keywords.concat("(<").concat(similarKeywordList.get(i)).concat(">)");
		}
		return GET_INSIGHTS_FOR_KEYWORDS.replace("@KEYWORDS@", keywords);
		
	}
	
	private Map<String, String> getTypeAndInstance(SesameJenaSelectWrapper sjsw) {
		String[] names = sjsw.getVariables();

		Map<String, String> typeAndInstance = new HashMap<String, String>();
		while(sjsw.hasNext()) {
			SesameJenaSelectStatement sjss = sjsw.next();
			typeAndInstance.put(sjss.getRawVar(names[0]).toString(), sjss.getRawVar(names[1]).toString());
		}
		return typeAndInstance;
	}
	
}
