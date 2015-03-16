package prerna.nameserver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.algorithm.nlp.NaturalLanguageProcessingHelper;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.impl.RemoteSemossSesameEngine;
import prerna.util.DIHelper;
import prerna.util.Utility;
import rita.RiWordNet;
import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import edu.stanford.nlp.trees.EnglishGrammaticalRelations;
import edu.stanford.nlp.trees.GrammaticalRelation;
import edu.stanford.nlp.trees.TypedDependency;

public class SearchEngineMasterDB extends ModifyMasterDB {

	private LexicalizedParser lp;
	private WordnetComparison wnComp;
	private RiWordNet wordnet;
	
	private Set<String> keywordSet;
	private Set<String> mcSet;
	
	/**
	 * Constructor for the class
	 * Defines the wordnet library
	 * Defines the nlp lib
	 */
	public SearchEngineMasterDB(String wordNetDir, String lpDir) {
		super();
		lp = LexicalizedParser.loadModel(lpDir);
		lp.setOptionFlags(new String[]{"-maxLength", "80", "-retainTmpSubcategories"});
		wordnet = new RiWordNet(wordNetDir, false, true); // params: wordnetInstallDir, ignoreCompoundWords, ignoreUppercaseWords
		wnComp = new WordnetComparison();
		wnComp.setLp(lp);
		wnComp.setWordnet(wordnet);
	}
	
	/**
	 * Constructor for the class
	 * Defines the localMasterDBName
	 * Defines the wordnet library
	 * Defines the nlp lib
	 */
	public SearchEngineMasterDB(String localMasterDbName, String wordNetDir, String lpDir) {
		super(localMasterDbName);
		lp = LexicalizedParser.loadModel(lpDir);
		lp.setOptionFlags(new String[]{"-maxLength", "80", "-retainTmpSubcategories"});
		wordnet = new RiWordNet(wordNetDir, false, true); // params: wordnetInstallDir, ignoreCompoundWords, ignoreUppercaseWords
		wnComp = new WordnetComparison();
		wnComp.setLp(lp);
		wnComp.setWordnet(wordnet);
	}

	public List<Hashtable<String, Object>> getWebInsightsFromSearchString(String searchString) {
		Set<String> mainNouns = new HashSet<String>();
		Set<String> nounModifiers = new HashSet<String>();
		
		// update the mainNouns and nounModifiers set
		processString(searchString, mainNouns, nounModifiers);
		getMasterConceptAndKeywordList();
		
		Set<String> searchKeywords = new HashSet<String>();
		Set<String> searchMC = new HashSet<String>();
		Set<String> searchInstances = new HashSet<String>();
		determineWordAllocation(searchKeywords, searchMC, searchInstances, mainNouns, nounModifiers);
		Map<String, Map<String, Set<String>>> engineInstancesMap = new Hashtable<String, Map<String, Set<String>>>();
		webWordAllocationHelper(searchKeywords, searchMC, searchInstances, engineInstancesMap);
		
		List<Hashtable<String,Object>> insightList = new ArrayList<Hashtable<String,Object>>();
		
		// path for looking through each db and checking if any keyword contains the instance
		if(searchKeywords.isEmpty() && searchMC.isEmpty()) {
			ISelectWrapper sjsw = Utility.processQuery(masterEngine, MasterDatabaseQueries.GET_ALL_INSIGHTS);
			String[] names = sjsw.getVariables();
			while(sjsw.hasNext()) {
				ISelectStatement sjss = sjsw.next();
				String engine = sjss.getVar(names[0]).toString();
				String insightLabel = sjss.getVar(names[1]).toString();
				String keyword = sjss.getRawVar(names[2]).toString();
				String perspectiveLabel = sjss.getVar(names[3]).toString();
				String viz = sjss.getVar(names[4]).toString();
				
				Map<String, Set<String>> typeAndInstance = engineInstancesMap.get(engine);
				String typeURI = MasterDatabaseURIs.SEMOSS_CONCEPT_URI.concat("/").concat(Utility.getInstanceName(keyword));
				if(typeAndInstance != null && typeAndInstance.containsKey(typeURI)) {
					Hashtable<String, Object> insightHash = new Hashtable<String, Object>();
					insightHash.put(MasterDatabaseConstants.DB_KEY, engine);
					insightHash.put(MasterDatabaseConstants.QUESITON_KEY, insightLabel);
					insightHash.put(MasterDatabaseConstants.TYPE_KEY, typeURI);
					insightHash.put(MasterDatabaseConstants.PERSPECTIVE_KEY, perspectiveLabel);
					insightHash.put(MasterDatabaseConstants.VIZ_TYPE_KEY, viz);
					insightHash.put(MasterDatabaseConstants.SCORE_KEY, 1.0);
					insightHash.put(MasterDatabaseConstants.INSTANCE_KEY, typeAndInstance.get(typeURI));
					insightList.add(insightHash);
				}
			}
		} 
		// path for looking at specific keywords 
		// initially, returning all nouns found
		else {
			Set<String> combinedNounSet = new HashSet<String>();
			combinedNounSet.addAll(searchKeywords);
			combinedNounSet.addAll(getKeywordsFromMCSet(searchMC));

			// track all keywords
			Set<String> connectedKeywordSet = new HashSet<String>();
			// track all the engines that contain the keywords to speed up instance search
			Map<String, Set<String>> engineKeywordMap = new HashMap<String, Set<String>>();
			MasterDBHelper.findRelatedKeywordsToSetStrings(masterEngine, combinedNounSet, connectedKeywordSet, engineKeywordMap);
			
			Map<String, Double> similarKeywordScores = new HashMap<String, Double>();
			String query = formInsightsForKeywordsQuery(combinedNounSet, connectedKeywordSet, similarKeywordScores);
			
			ISelectWrapper sjsw = Utility.processQuery(masterEngine, query);
			String[] names = sjsw.getVariables();
			while(sjsw.hasNext()) {
				ISelectStatement sjss = sjsw.next();
				String engine = sjss.getVar(names[0]).toString();
				String insightLabel = sjss.getVar(names[1]).toString();
				String keyword = sjss.getRawVar(names[2]).toString();
				String perspectiveLabel = sjss.getVar(names[3]).toString();
				String viz = sjss.getVar(names[4]).toString();
				
				String typeURI = MasterDatabaseURIs.SEMOSS_CONCEPT_URI.concat("/").concat(Utility.getInstanceName(keyword));
				if(engineInstancesMap == null) {
					Hashtable<String, Object> insightHash = new Hashtable<String, Object>();
					insightHash.put(MasterDatabaseConstants.DB_KEY, engine);
					insightHash.put(MasterDatabaseConstants.QUESITON_KEY, insightLabel);
					insightHash.put(MasterDatabaseConstants.TYPE_KEY, typeURI);
					insightHash.put(MasterDatabaseConstants.PERSPECTIVE_KEY, perspectiveLabel);
					insightHash.put(MasterDatabaseConstants.VIZ_TYPE_KEY, viz);
					insightHash.put(MasterDatabaseConstants.SCORE_KEY, 1.0 - similarKeywordScores.get(keyword));
					insightList.add(insightHash);
				} else {
					Map<String, Set<String>> typeAndInstance = engineInstancesMap.get(engine);
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
		}
		
		return insightList;
	}
	
	public List<Hashtable<String,Object>> getLocalInsightsFromSearchString(String searchString) {
		Set<String> mainNouns = new HashSet<String>();
		Set<String> nounModifiers = new HashSet<String>();
		
		// update the mainNouns and nounModifiers set
		processString(searchString, mainNouns, nounModifiers);
		getMasterConceptAndKeywordList();
		Set<String> searchKeywords = new HashSet<String>();
		Set<String> searchMC = new HashSet<String>();
		Set<String> searchInstances = new HashSet<String>();
		determineWordAllocation(searchKeywords, searchMC, searchInstances, mainNouns, nounModifiers);

		Map<String, Map<String, Set<String>>> engineInstancesMap = new Hashtable<String, Map<String, Set<String>>>();
		localWordAllocationHelper(searchKeywords, searchMC, searchInstances, engineInstancesMap);
		
		List<Hashtable<String,Object>> insightList = new ArrayList<Hashtable<String,Object>>();
		
		// path for looking through each db and checking if any keyword contains the instance
		if(searchKeywords.isEmpty() && searchMC.isEmpty()) {
			ISelectWrapper sjsw = Utility.processQuery(masterEngine, MasterDatabaseQueries.GET_ALL_INSIGHTS);
			String[] names = sjsw.getVariables();
			while(sjsw.hasNext()) {
				ISelectStatement sjss = sjsw.next();
				String engine = sjss.getVar(names[0]).toString();
				String insightLabel = sjss.getVar(names[1]).toString();
				String keyword = sjss.getRawVar(names[2]).toString();
				String perspectiveLabel = sjss.getVar(names[3]).toString();
				String viz = sjss.getVar(names[4]).toString();
				
				Map<String, Set<String>> typeAndInstance = engineInstancesMap.get(engine);
				String typeURI = MasterDatabaseURIs.SEMOSS_CONCEPT_URI.concat("/").concat(Utility.getInstanceName(keyword));
				if(typeAndInstance != null && typeAndInstance.containsKey(typeURI)) {
					Hashtable<String, Object> insightHash = new Hashtable<String, Object>();
					insightHash.put(MasterDatabaseConstants.DB_KEY, engine);
					insightHash.put(MasterDatabaseConstants.QUESITON_KEY, insightLabel);
					insightHash.put(MasterDatabaseConstants.TYPE_KEY, typeURI);
					insightHash.put(MasterDatabaseConstants.PERSPECTIVE_KEY, perspectiveLabel);
					insightHash.put(MasterDatabaseConstants.VIZ_TYPE_KEY, viz);
					insightHash.put(MasterDatabaseConstants.SCORE_KEY, 1.0);
					insightHash.put(MasterDatabaseConstants.INSTANCE_KEY, typeAndInstance.get(typeURI));
					insightList.add(insightHash);
				}
			}
		} 
		// path for looking at specific keywords 
		// initially, returning all nouns found
		else {
			Set<String> combinedNounSet = new HashSet<String>();
			combinedNounSet.addAll(searchKeywords);
			combinedNounSet.addAll(getKeywordsFromMCSet(searchMC));

			// track all keywords
			Set<String> connectedKeywordSet = new HashSet<String>();
			// track all the engines that contain the keywords to speed up instance search
			Map<String, Set<String>> engineKeywordMap = new HashMap<String, Set<String>>();
			MasterDBHelper.findRelatedKeywordsToSetStrings(masterEngine, combinedNounSet, connectedKeywordSet, engineKeywordMap);
			
			Map<String, Double> similarKeywordScores = new HashMap<String, Double>();
			String query = formInsightsForKeywordsQuery(combinedNounSet, connectedKeywordSet, similarKeywordScores);

			ISelectWrapper sjsw = Utility.processQuery(masterEngine, query);
			String[] names = sjsw.getVariables();
			while(sjsw.hasNext()) {
				ISelectStatement sjss = sjsw.next();
				String engine = sjss.getVar(names[0]).toString();
				String insightLabel = sjss.getVar(names[1]).toString();
				String keyword = sjss.getRawVar(names[2]).toString();
				String perspectiveLabel = sjss.getVar(names[3]).toString();
				String viz = sjss.getVar(names[4]).toString();
				
				String typeURI = MasterDatabaseURIs.SEMOSS_CONCEPT_URI.concat("/").concat(Utility.getInstanceName(keyword));
				if(engineInstancesMap.isEmpty()) {
					Hashtable<String, Object> insightHash = new Hashtable<String, Object>();
					insightHash.put(MasterDatabaseConstants.DB_KEY, engine);
					insightHash.put(MasterDatabaseConstants.QUESITON_KEY, insightLabel);
					insightHash.put(MasterDatabaseConstants.TYPE_KEY, typeURI);
					insightHash.put(MasterDatabaseConstants.PERSPECTIVE_KEY, perspectiveLabel);
					insightHash.put(MasterDatabaseConstants.VIZ_TYPE_KEY, viz);
					insightHash.put(MasterDatabaseConstants.SCORE_KEY, 1.0 - similarKeywordScores.get(keyword));
					insightList.add(insightHash);
				} else {
					Map<String, Set<String>> typeAndInstance = engineInstancesMap.get(engine);
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
		}
		
		return insightList;
	}
	
	private String formInsightsForKeywordsQuery(Set<String> keywordSet, Set<String> connectedKeywordSet, Map<String, Double> similarKeywordScores) {
		// get list of insights for keywords if the score is above threshold
		List<String> similarKeywordList = new ArrayList<String>();
		
		Iterator<String> keywordIt = keywordSet.iterator();
		while(keywordIt.hasNext()) {
			String keywordURI = MasterDatabaseURIs.KEYWORD_BASE_URI +"/" + keywordIt.next();
			
			for(String otherKeywordURI : connectedKeywordSet) {
				double simScore = wnComp.compareKeywords(keywordURI, otherKeywordURI);
				if(wnComp.isSimilar(simScore)) {
					similarKeywordList.add(otherKeywordURI);
					similarKeywordScores.put(otherKeywordURI, simScore);
				}
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
	
	//TODO: KYLENE
	/**
	 * Add local engines and acceptable instances 
	 * @param instanceNameSet
	 * @param engineSet
	 * @param engineInstances
	 */
	private void determineWebEngineAndAcceptableInstances(Set<String> instanceNameSet,Hashtable<String, String> engineURLHash, Map<String, Map<String, Set<String>>> engineInstancesMap) {
		// query the db to get every concept and an example instance to get the baseURI
		Iterator<String> engineIt = engineURLHash.keySet().iterator();
		while(engineIt.hasNext()) {			
			String engineName = engineIt.next();
			String engineAPI = engineURLHash.get(engineName);
			
			Map<String, String> keywordBaseURIMap = new Hashtable<String, String>();
			
			// this will call the engine and gets then flushes it into sesame jena construct wrapper
			RemoteSemossSesameEngine engine = new RemoteSemossSesameEngine();
			engine.setAPI(engineAPI);
			engine.setDatabase(engineName);
			
			ISelectWrapper sjsw = Utility.processQuery(engine, MasterDatabaseQueries.GET_ENGINE_CONCEPTS_AND_SAMPLE_INSTANCE);
			String[] names = sjsw.getVariables();
			while(sjsw.hasNext()) {
				ISelectStatement sjss = sjsw.next();
				String concept = sjss.getVar(names[0]).toString();
				String instanceSampleURI = sjss.getRawVar(names[1]).toString();
				String instanceBaseURI = Utility.getBaseURI(instanceSampleURI) + "/Concept/" + concept;
				
				keywordBaseURIMap.put(concept, instanceBaseURI);
			}
			
			String bindingsStr = "";
			for(String concept : keywordBaseURIMap.keySet()) {
				String instanceBaseURI = keywordBaseURIMap.get(concept);
				Iterator<String> instanceNameIt = instanceNameSet.iterator();
				while(instanceNameIt.hasNext()) {
					bindingsStr = bindingsStr.concat("(<").concat(instanceBaseURI).concat("/").concat(instanceNameIt.next()).concat(">)");
				}
			}
			
			Map<String, Set<String>> usableInstances = new Hashtable<String, Set<String>>();
			String useableInstanceQuery = MasterDatabaseQueries.INSTANCE_EXISTS_QUERY.replace("@BINDINGS@", bindingsStr);
			sjsw = Utility.processQuery(engine, useableInstanceQuery);
			names = sjsw.getVariables();
			while(sjsw.hasNext()) {
				ISelectStatement sjss = sjsw.next();
				String type = sjss.getRawVar(names[0]).toString();
				String instance = sjss.getRawVar(names[1]).toString();
				
				Set<String> instanceList;
				if(usableInstances.containsKey(type)) {
					instanceList = usableInstances.get(type);
					instanceList.add(instance);
				} else {
					instanceList = new HashSet<String>();
					instanceList.add(instance);
					usableInstances.put(type, instanceList);
				}
			}
			engineInstancesMap.put(engineName, usableInstances);
		}
	}
	
	
	//TODO: how to deal with multiple baseURI's in a db?
	/**
	 * Add local engines and acceptable instances 
	 * @param instanceNameSet
	 * @param engineSet
	 * @param engineInstances
	 */
	private void determineLocalEngineAndAcceptableInstances(Set<String> instanceNameSet, Set<String> engineSet, Map<String, Map<String, Set<String>>> engineInstancesMap) {
		// query the db to get every concept and an example instance to get the baseURI
		Iterator<String> engineIt = engineSet.iterator();
		while(engineIt.hasNext()) {			
			String engineName = engineIt.next();
			
			Map<String, String> keywordBaseURIMap = new Hashtable<String, String>();
			
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
			ISelectWrapper sjsw = Utility.processQuery(engine, MasterDatabaseQueries.GET_ENGINE_CONCEPTS_AND_SAMPLE_INSTANCE);
			String[] names = sjsw.getVariables();
			while(sjsw.hasNext()) {
				ISelectStatement sjss = sjsw.next();
				String concept = sjss.getVar(names[0]).toString();
				String instanceSampleURI = sjss.getRawVar(names[1]).toString();
				String instanceBaseURI = Utility.getBaseURI(instanceSampleURI) + "/Concept/" + concept;
				
				keywordBaseURIMap.put(concept, instanceBaseURI);
			}
			
			String bindingsStr = "";
			for(String concept : keywordBaseURIMap.keySet()) {
				String instanceBaseURI = keywordBaseURIMap.get(concept);
				Iterator<String> instanceNameIt = instanceNameSet.iterator();
				while(instanceNameIt.hasNext()) {
					bindingsStr = bindingsStr.concat("(<").concat(instanceBaseURI).concat("/").concat(instanceNameIt.next()).concat(">)");
				}
			}
			
			Map<String, Set<String>> usableInstances = new Hashtable<String, Set<String>>();
			String useableInstanceQuery = MasterDatabaseQueries.INSTANCE_EXISTS_QUERY.replace("@BINDINGS@", bindingsStr);
			sjsw = Utility.processQuery(engine, useableInstanceQuery);
			names = sjsw.getVariables();
			while(sjsw.hasNext()) {
				ISelectStatement sjss = sjsw.next();
				String type = sjss.getRawVar(names[0]).toString();
				String instance = sjss.getRawVar(names[1]).toString();
				
				Set<String> instanceList;
				if(usableInstances.containsKey(type)) {
					instanceList = usableInstances.get(type);
					instanceList.add(instance);
				} else {
					instanceList = new HashSet<String>();
					instanceList.add(instance);
					usableInstances.put(type, instanceList);
				}
			}
			engineInstancesMap.put(engineName, usableInstances);
		}
	}
	
	/**
	 * 
	 * @param mcSet
	 * @return
	 */
	private Set<String> getKeywordsFromMCSet(Set<String> mcSet) {
		Set<String> retSet = new HashSet<String>();
		if(!mcSet.isEmpty()) {
			String bindingStr = "";
			for(String mc : mcSet) {
				bindingStr = bindingStr.concat("(<").concat(MasterDatabaseURIs.MC_BASE_URI).concat("/").concat(mc).concat(">)");
			}
			String query = MasterDatabaseQueries.GET_ALL_KEYWORDS_FROM_MC_List.replace("@BINDINGS@", bindingStr);
			ISelectWrapper sjsw = Utility.processQuery(masterEngine, query);
			String[] names = sjsw.getVariables();
			while(sjsw.hasNext()) {
				ISelectStatement sjss = sjsw.next();
				retSet.add(sjss.getVar(names[0]).toString());
			}
		}
		return retSet;
	}
	
	
	/**
	 * 
	 * @param searchKeywords
	 * @param searchMC
	 * @param searchInstances
	 * @param mainNouns
	 * @param nounModifiers
	 * @param engineInstancesMap 
	 */
	private void determineWordAllocation(
			Set<String> searchKeywords,
			Set<String> searchMC,
			Set<String> searchInstances,
			Set<String> mainNouns,
			Set<String> nounModifiers
			) 
	{
		// search if main nouns are keywords, master concepts, or instances
		for(String noun : mainNouns) {
			if(containsIgnoreCase(keywordSet, noun)) {
				searchKeywords.add(getFromSetIgnoringCase(keywordSet, noun));
			} else if(containsIgnoreCase(mcSet, noun)) {
				searchMC.add(getFromSetIgnoringCase(mcSet, noun));
			} else {
				searchInstances.add(noun);
			}
		}
		
		// if only found instances in main nouns, look through noun modifiers
		if(searchKeywords.isEmpty() && searchMC.isEmpty()) {
			for(String noun : nounModifiers) {
				if(containsIgnoreCase(keywordSet, noun)) {
					searchKeywords.add(getFromSetIgnoringCase(keywordSet, noun));
				} else if(containsIgnoreCase(mcSet, noun)) {
					searchMC.add(getFromSetIgnoringCase(mcSet, noun));
				} else {
					searchInstances.add(noun);
				}
			}
		} else {
			searchInstances.addAll(nounModifiers);
		}
	}
	
	/**
	 * 
	 * @param searchKeywords
	 * @param searchMC
	 * @param searchInstances
	 * @param engineInstancesMap
	 */
	private void localWordAllocationHelper(
			Set<String> searchKeywords,
			Set<String> searchMC,
			Set<String> searchInstances,
			Map<String, Map<String, Set<String>>> engineInstancesMap			
			) 
	{
		if(!searchInstances.isEmpty()) {
			Set<String> engineList = new HashSet<String>();
			MasterDBHelper.fillEnglishList(masterEngine, engineList);
			// fill in the engineInstances map
			determineLocalEngineAndAcceptableInstances(searchInstances, engineList, engineInstancesMap);
			// make sure engine InstanceMap is not empty for instance
			boolean isEmpty = true;
			for(String engine : engineInstancesMap.keySet()) {
				if(!engineInstancesMap.get(engine).isEmpty()) {
					isEmpty = false;
					break;
				}
			}
			if(isEmpty) {
				engineInstancesMap.clear(); // make it empty, before it contained engine keys pointing to empty values
			}
			
			// make sure engine InstanceMap contains every instance
			if(!searchInstances.isEmpty()) {
				OUTER: for(String engine : engineInstancesMap.keySet()) {
					Map<String, Set<String>> instanceMap = engineInstancesMap.get(engine);
					for(String key : instanceMap.keySet()) {
						Set<String> instancesFound = instanceMap.get(key);
						for(String instance : instancesFound) {
							searchInstances.remove(Utility.getBaseURI(instance));
							if(searchInstances.isEmpty()) {
								break OUTER;
							}
						}
						
					}
				}
			}
				
			if(!searchInstances.isEmpty()) {
				// find related nouns in keyword set and mc set if searchInstance was not found as an instance in any engine
				for(String instance : searchInstances) {
					double bestSim = 1.1; // want min sim value, val cannot be larger than 1
					String bestMatch = "";
					for(String mc : mcSet) {
						double simVal = wordnet.getDistance(instance.toLowerCase(), mc.toLowerCase(), "n");
						if(simVal < bestSim) {
							bestSim = simVal;
							bestMatch = mc;
						}
					}
					if(bestSim < MasterDatabaseConstants.SIMILARITY_CUTOFF) {
						searchMC.add(bestMatch);
					}
				}
			}
		}
	}
	
	/**
	 * 
	 * @param searchKeywords
	 * @param searchMC
	 * @param searchInstances
	 * @param engineInstancesMap
	 */
	private void webWordAllocationHelper(
			Set<String> searchKeywords,
			Set<String> searchMC,
			Set<String> searchInstances,
			Map<String, Map<String, Set<String>>> engineInstancesMap			
			) 
	{
		if(!searchInstances.isEmpty()) {
			Hashtable<String, String> engineURLHash = new Hashtable<String, String>();
			MasterDBHelper.fillAPIHash(masterEngine, engineURLHash);
			// fill in the engineInstances map
			determineWebEngineAndAcceptableInstances(searchInstances, engineURLHash, engineInstancesMap);
			// make sure engine InstanceMap is not empty for instance
			boolean isEmpty = true;
			for(String engine : engineInstancesMap.keySet()) {
				if(!engineInstancesMap.get(engine).isEmpty()) {
					isEmpty = false;
					break;
				}
			}
			if(isEmpty) {
				engineInstancesMap.clear(); // make it empty, before it contained engine keys pointing to empty values
			}
			
			// make sure engine InstanceMap contains every instance
			if(!searchInstances.isEmpty()) {
				OUTER: for(String engine : engineInstancesMap.keySet()) {
					Map<String, Set<String>> instanceMap = engineInstancesMap.get(engine);
					for(String key : instanceMap.keySet()) {
						Set<String> instancesFound = instanceMap.get(key);
						for(String instance : instancesFound) {
							searchInstances.remove(Utility.getBaseURI(instance));
							if(searchInstances.isEmpty()) {
								break OUTER;
							}
						}
						
					}
				}
			}
				
			if(!searchInstances.isEmpty()) {
				// find related nouns in keyword set and mc set if searchInstance was not found as an instance in any engine
				for(String instance : searchInstances) {
					double bestSim = 1.1; // want min sim value, val cannot be larger than 1
					String bestMatch = "";
					for(String mc : mcSet) {
						double simVal = wordnet.getDistance(instance.toLowerCase(), mc.toLowerCase(), "n");
						if(simVal < bestSim) {
							bestSim = simVal;
							bestMatch = mc;
						}
					}
					if(bestSim < MasterDatabaseConstants.SIMILARITY_CUTOFF) {
						searchMC.add(bestMatch);
					}
				}
			}
		}
	}
	
	/**
	 * 
	 * @param searchString
	 * @param mainNouns
	 * @param nounModifiers
	 */
	public void processString(String searchString, Set<String> mainNouns, Set<String> nounModifiers) {
		List<TypedDependency> tdl = new ArrayList<TypedDependency>();
		List<TaggedWord> wordPOS = new ArrayList<TaggedWord>();
		Hashtable<GrammaticalRelation, Vector<TypedDependency>> nodeHash = new Hashtable<GrammaticalRelation, Vector<TypedDependency>>();
		
		NaturalLanguageProcessingHelper.createDepList(lp, searchString, tdl, wordPOS); //create dependencies
		NaturalLanguageProcessingHelper.setTypeDependencyHash(tdl, nodeHash);
		
//		Set<String> nouns = new HashSet<String>();
//		Set<String> verbs = new HashSet<String>();
//		Set<String> other = new HashSet<String>();
//		
//		for(TaggedWord tw : wordPOS) {
//			String tag = tw.tag();
//			if(tag.startsWith("NN")) {
//				nouns.add(tw.word());
//			} else if(tag.startsWith("VB"))  {
//				verbs.add(tw.word());
//			} else {
//				other.add(tw.word());
//			}
//		}
		
		determineMainNoun(nodeHash, mainNouns);
		System.out.println("Main nouns: " + mainNouns);
		determineModifiersOfMainNoun(nodeHash, nounModifiers, mainNouns);
		System.out.println("Modifiers of main nouns: " + nounModifiers);
	}
	
	/**
	 * 
	 * @param nodeHash
	 * @param mainNouns
	 */
	public void determineMainNoun(Hashtable<GrammaticalRelation, Vector<TypedDependency>> nodeHash, Set<String> mainNouns) {
		if(nodeHash.containsKey(EnglishGrammaticalRelations.NOMINAL_SUBJECT)) {
			mainNouns.addAll(getTopDep(nodeHash.get(EnglishGrammaticalRelations.NOMINAL_SUBJECT)));
		}
		if(nodeHash.containsKey(EnglishGrammaticalRelations.NOUN_COMPOUND_MODIFIER)) {
			mainNouns.addAll(getTopGov(nodeHash.get(EnglishGrammaticalRelations.NOUN_COMPOUND_MODIFIER)));
		}
	}
	
	/**
	 * 
	 * @param nodeHash
	 * @param nounModifiers
	 * @param mainNouns
	 */
	private void determineModifiersOfMainNoun(Hashtable<GrammaticalRelation, Vector<TypedDependency>> nodeHash, Set<String> nounModifiers, Set<String> mainNouns) {
		if(nodeHash.containsKey(EnglishGrammaticalRelations.NOUN_COMPOUND_MODIFIER)) {
			nounModifiers.addAll(getTopDep(nodeHash.get(EnglishGrammaticalRelations.NOUN_COMPOUND_MODIFIER), mainNouns));
		}
		if(nodeHash.containsKey(GrammaticalRelation.DEPENDENT)) {
			nounModifiers.addAll(getTopDep(nodeHash.get(GrammaticalRelation.DEPENDENT), mainNouns));
		}
		if(nodeHash.containsKey(GrammaticalRelation.ROOT)) {
			nounModifiers.addAll(getTopDep(nodeHash.get(GrammaticalRelation.ROOT), mainNouns));
		}
	}
	
	/**
	 * 
	 * @param tdlArr
	 * @param filterValues
	 * @return
	 */
	private Set<String> getTopDep(Vector<TypedDependency> tdlArr, Set<String> filterValues) {
		Set<String> depList = new HashSet<String>();
		
		int i = 0;
		int size = tdlArr.size();
		
		for(; i < size; i++) {
			TypedDependency td = tdlArr.get(i);
			if(filterValues.contains(td.gov().value())) {
				depList.add(td.dep().value());
			}
			// in case no main nouns found
			if(filterValues.isEmpty()) {
				depList.add(td.dep().value());
			}
		}
		
		return depList;
	}

	/**
	 * Return the Set of deps in the sentence based on the grammatical relationship formed by combining the nouns in the word
	 * @param tdlArr		The Vector of typed dependencies of the specified grammatical relationship
	 * @return				Set of deps in the sentence based on the grammatical relationship
	 */
	private Set<String> getTopDep(Vector<TypedDependency> tdlArr) {
		Set<String> govList = new HashSet<String>();
		
		int i = 0;
		int size = tdlArr.size();
		
		for(; i < size; i++) {
			TypedDependency td = tdlArr.get(i);
			govList.add(td.dep().value());
		}
		
		return govList;
	}
	
	/**
	 * Return the Set of govs in the sentence based on the grammatical relationship formed by combining the nouns in the word
	 * @param tdlArr		The Vector of typed dependencies of the specified grammatical relationship
	 * @return				Set of govs in the sentence based on the grammatical relationship
	 */
	private Set<String> getTopGov(Vector<TypedDependency> tdlArr) {
		Set<String> govList = new HashSet<String>();
		
		int i = 0;
		int size = tdlArr.size();
		
		for(; i < size; i++) {
			TypedDependency td = tdlArr.get(i);
			govList.add(td.gov().value());
		}
		
		return govList;
	}
	
	/**
	 * Gets the Set of all keywords and master concepts in the master database
	 */
	private void getMasterConceptAndKeywordList() {
		keywordSet = runQueryAsSet(MasterDatabaseQueries.GET_ALL_KEYWORDS);
		mcSet = runQueryAsSet(MasterDatabaseQueries.GET_ALL_MASTER_CONCEPTS);
	}
	
	/**
	 * Runs a query and saves the output as a Set
	 * @param query		The query to run
	 * @return			The set containing the query return
	 */
	private Set<String> runQueryAsSet(String query) {
		Set<String> set = new HashSet<String>();
		
		ISelectWrapper wrapper = Utility.processQuery(masterEngine, query);
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext()) {
			ISelectStatement sjss = wrapper.next();
			set.add(sjss.getVar(names[0]).toString());
		}
		
		return set;
	}
	
	//TODO: put in utility class somewhere
	private boolean containsIgnoreCase(Set<String> set1, String compareVal) {
		Iterator<String> it1 = set1.iterator();
		while(it1.hasNext()) {
			String val1 = it1.next();
			if(val1.equalsIgnoreCase(compareVal)) {
				return true;
			}
		}
		
		return false;
	}
	
	//TODO: put in utility class somewhere
	private String getFromSetIgnoringCase(Set<String> set1, String compareVal) {
		Iterator<String> it1 = set1.iterator();
		while(it1.hasNext()) {
			String val1 = it1.next();
			if(val1.equalsIgnoreCase(compareVal)) {
				return val1;
			}
		}
		
		return null;
	}

}
