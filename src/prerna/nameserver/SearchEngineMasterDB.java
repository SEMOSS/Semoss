package prerna.nameserver;

import java.util.ArrayList;
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
import prerna.util.DIHelper;
import prerna.util.Utility;
import rita.RiWordNet;
import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import edu.stanford.nlp.trees.EnglishGrammaticalRelations;
import edu.stanford.nlp.trees.GrammaticalRelation;
import edu.stanford.nlp.trees.TypedDependency;

public class SearchEngineMasterDB extends ModifyMasterDB {

	private RiWordNet wordnet;
	private LexicalizedParser lp;
	
	private Set<String> keywordSet;
	private Set<String> mcSet;
	
	/**
	 * Constructor for the class
	 * Defines the wordnet library
	 * Defines the nlp lib
	 */
	public SearchEngineMasterDB(String wordNetDir, String lpDir) {
		super();
		wordnet = new RiWordNet(wordNetDir, false, true); // params: wordnetInstallDir, ignoreCompoundWords, ignoreUppercaseWords
		lp = LexicalizedParser.loadModel(lpDir);
		lp.setOptionFlags(new String[]{"-maxLength", "80", "-retainTmpSubcategories"});
	}
	
	/**
	 * Constructor for the class
	 * Defines the localMasterDBName
	 * Defines the wordnet library
	 * Defines the nlp lib
	 */
	public SearchEngineMasterDB(String localMasterDbName, String wordNetDir, String lpDir) {
		super(localMasterDbName);
		wordnet = new RiWordNet(wordNetDir, false, true); // params: wordnetInstallDir, ignoreCompoundWords, ignoreUppercaseWords
		lp = LexicalizedParser.loadModel(lpDir);
		lp.setOptionFlags(new String[]{"-maxLength", "80", "-retainTmpSubcategories"});
	}

	public List<Hashtable<String, Object>> getWebInsightsFromSearchString(String searchString) {

		
		return null;
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
		
		// path for looking through each db and checking if any keyword contains the instance
		if(searchKeywords.isEmpty() && searchMC.isEmpty()) {
			Set<String> engineList = new HashSet<String>();
			MasterDBHelper.fillEnglishList(masterEngine, engineList);
			
			Map<String, Map<String, Set<String>>> engineInstancesMap = new Hashtable<String, Map<String, Set<String>>>();
			// fill in the engineInstances map
			determineLocalEngineAndAcceptableInstances(searchInstances, engineList, engineInstancesMap);
			
			List<Hashtable<String,Object>> insightList = new ArrayList<Hashtable<String,Object>>();
			ISelectWrapper sjsw = Utility.processQuery(masterEngine, GET_ALL_INSIGHTS);
			String[] names = sjsw.getVariables();
			while(sjsw.hasNext()) {
				ISelectStatement sjss = sjsw.next();
				String engine = sjss.getVar(names[0]).toString();
				String insightLabel = sjss.getVar(names[1]).toString();
				String keyword = sjss.getRawVar(names[2]).toString();
				String perspectiveLabel = sjss.getVar(names[3]).toString();
				String viz = sjss.getVar(names[4]).toString();
				
				Map<String, Set<String>> typeAndInstance = engineInstancesMap.get(engine);
				String typeURI = SEMOSS_CONCEPT_URI.concat("/").concat(Utility.getInstanceName(keyword));
				if(typeAndInstance != null && typeAndInstance.containsKey(typeURI)) {
					Hashtable<String, Object> insightHash = new Hashtable<String, Object>();
					insightHash.put(DB_KEY, engine);
					insightHash.put(QUESITON_KEY, insightLabel);
					insightHash.put(TYPE_KEY, typeURI);
					insightHash.put(PERSPECTIVE_KEY, perspectiveLabel);
					insightHash.put(VIZ_TYPE_KEY, viz);
					insightHash.put(SCORE_KEY, 1.0);
					insightHash.put(INSTANCE_KEY, typeAndInstance.get(typeURI));
					insightList.add(insightHash);
				}
			}
			return insightList;
		} else {
			// path for looking at specific keywords 
			
			
		}
		
		List<Hashtable<String, Object>> retList = new ArrayList<Hashtable<String, Object>>();
		
		
		return retList;
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
			ISelectWrapper sjsw = Utility.processQuery(engine, GET_ENGINE_CONCEPTS_AND_SAMPLE_INSTANCE);
			String[] names = sjsw.getVariables();
			while(sjsw.hasNext()) {
				ISelectStatement sjss = sjsw.next();
				String concept = sjss.getVar(names[0]).toString();
				String instanceSampleURI = sjss.getRawVar(names[1]).toString();
				String instanceBaseURI = Utility.getBaseURI(instanceSampleURI) + "/" + concept;
				
				keywordBaseURIMap.put(concept, instanceBaseURI);
			}
			
			String bindingsStr = "";
			for(String concept : keywordBaseURIMap.keySet()) {
				String instanceBaseURI = keywordBaseURIMap.get(concept).concat("/Concept");
				Iterator<String> instanceNameIt = instanceNameSet.iterator();
				while(instanceNameIt.hasNext()) {
					bindingsStr = bindingsStr.concat("(<").concat(instanceBaseURI).concat("/").concat(concept).concat("/").concat(instanceNameIt.next()).concat(">)");
				}
			}
			
			Map<String, Set<String>> usableInstances = new Hashtable<String, Set<String>>();
			String useableInstanceQuery = INSTANCE_EXISTS_QUERY.replace("@BINDINGS@", bindingsStr);
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
	 * @param searchKeywords
	 * @param searchMC
	 * @param searchInstances
	 * @param mainNouns
	 * @param nounModifiers
	 */
	public void determineWordAllocation(
			Set<String> searchKeywords,
			Set<String> searchMC,
			Set<String> searchInstances,
			Set<String> mainNouns,
			Set<String> nounModifiers			
			) 
	{
		// search if main nouns are keywords, master concepts, or instances
		for(String noun : mainNouns) {
			if(keywordSet.contains(noun)) {
				searchKeywords.add(noun);
			} else if(mcSet.contains(noun)) {
				searchMC.add(noun);
			} else {
				searchInstances.add(noun);
			}
		}
		
		// if only found instances in main nouns, look through noun modifiers
		if(keywordSet.isEmpty() && mcSet.isEmpty()) {
			for(String noun : nounModifiers) {
				if(keywordSet.contains(noun)) {
					searchKeywords.add(noun);
				} else if(mcSet.contains(noun)) {
					searchMC.add(noun);
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
			mainNouns = getTopDep(nodeHash.get(EnglishGrammaticalRelations.NOMINAL_SUBJECT));
		}
		if(nodeHash.containsKey(EnglishGrammaticalRelations.NOUN_COMPOUND_MODIFIER)) {
			mainNouns = getTopGov(nodeHash.get(EnglishGrammaticalRelations.NOUN_COMPOUND_MODIFIER));
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
	}
	
	/**
	 * 
	 * @param tdlArr
	 * @param filterValues
	 * @return
	 */
	private Set<String> getTopDep(Vector<TypedDependency> tdlArr, Set<String> filterValues) {
		Set<String> govList = new HashSet<String>();
		
		int i = 0;
		int size = tdlArr.size();
		
		for(; i < size; i++) {
			TypedDependency td = tdlArr.get(i);
			if(filterValues.contains(td.gov().value())) {
				govList.add(td.dep().value());
			}
		}
		
		return govList;
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
		keywordSet = runQueryAsSet(GET_ALL_KEYWORDS);
		mcSet = runQueryAsSet(GET_ALL_MASTER_CONCEPTS);
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

}
