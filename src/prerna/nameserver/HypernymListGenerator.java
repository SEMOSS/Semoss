package prerna.nameserver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.algorithm.nlp.PartOfSpeechHelper;
import prerna.util.Constants;
import prerna.util.DIHelper;
import rita.RiWordNet;

public class HypernymListGenerator {

	private RiWordNet wordnet;
	
	private final double SIMILARITY_CUTOFF = 0.25;
	private final String NOUN = "n";
	
	// keep a list of all child -> parent relationships
	private Map<String, String> mappings;
	
	/**
	 * Constructor for the class
	 * Defines the wordnet library
	 */
	public HypernymListGenerator() {
		String baseDirectory = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String wordNet = "RDFGraphLib" + System.getProperty("file.separator") + "WordNet-3.1";
		String wordNetDir  = baseDirectory + System.getProperty("file.separator") + wordNet;
		wordnet = new RiWordNet(wordNetDir, false, true); // params: wordnetInstallDir, ignoreCompoundWords, ignoreUppercaseWords
		mappings = new HashMap<String, String>();
	}
	
	/**
	 * Constructor for the class
	 * Defines the wordnet library
	 */
	public HypernymListGenerator(String wordNetDir) {
		wordnet = new RiWordNet(wordNetDir, false, true); // params: wordnetInstallDir, ignoreCompoundWords, ignoreUppercaseWords
		mappings = new HashMap<String, String>();
	}
	
	/**
	 * Returns the root node in a hypernym tree
	 * @param noun	The noun (also the leaf) to be used to generate up the hypernym tree
	 * @return		The root node (which can be traversed to all the children) of the hypernym tree
	 */
	public TreeNode<String> getHypernymTree(String noun) {
		List<String> hypernymList = getHypernymList(noun);
		return generateTree(hypernymList);
	}
	
	/**
	 * Returns the root node in a hypernym tree
	 * @param noun	The list of hypernyms of a noun
	 * @return		The root node (which can be traversed to all the children) of the hypernym tree
	 */
	public TreeNode<String> getHypernymTree(List<String> hypernymList) {
		return generateTree(hypernymList);
	}
	
	/**
	 * Gets a list of the hypernyms within the acceptable range
	 * @param noun	The noun to find the hypernyms
	 * @return		The list of hypernyms (list is hyponym -> hypernym) for the noun
	 */
	public List<String> getHypernymList(String noun) {
		List<String> hypernymList = new ArrayList<String>();
		hypernymList.add(noun);
		
		String previousHypernym = noun;
		// continue until break occurs due to no hypernym within acceptable similarity threshold
		while(true) {
			String[] hypernymArr = wordnet.getHypernyms(previousHypernym, NOUN);
			String bestHypernym = getBestHypernym(noun, previousHypernym, hypernymArr);
			if(bestHypernym == null || hypernymList.contains(bestHypernym)) { // prevent loops
				break;
			}
			hypernymList.add(bestHypernym);
			mappings.put(previousHypernym, bestHypernym);
			previousHypernym = bestHypernym;
		}
		
		return hypernymList;
	}
	
	/**
	 * Gets the best hypernym from the set
	 * @param noun				The original noun being used to get the hypernym tree
	 * @param hyponymOfList		The direct hyponym of the hypernym list, used to update the mapping object
	 * @param hypernymList		The hypernyms to test against the noun to see if they are in the acceptable similarity range
	 * @return					The best hypernym to the noun
	 */
	private String getBestHypernym(String noun, String hyponymOfList, String[] hypernymList) {
		String bestHypernym = null;
		double distance = SIMILARITY_CUTOFF;
		
		// see if word already exists in mapping and test if in appropriate range
		if(mappings.containsKey(hyponymOfList)) {
			String hypernym = mappings.get(hyponymOfList);
			double newDistance = wordnet.getDistance(noun, hypernym, NOUN);
			if(newDistance < distance) {
				bestHypernym = hypernym;
			}
			return bestHypernym;
		}
		
		
		int i = 0;
		int size = hypernymList.length;
		for(; i < size; i++) {
			String hypernym = hypernymList[i];
			double newDistance = wordnet.getDistance(noun, hypernym, NOUN);
			if(newDistance < distance) {
				bestHypernym = hypernym;
				distance = newDistance;
			} else if(newDistance == distance) {
				// if distance is the same, take the hypernym with less letters
				if(bestHypernym == null) {
					bestHypernym = hypernym;
				} else if(hypernym.length() < bestHypernym.length()) {
					bestHypernym = hypernym;
				}
			}
		}
		
		return bestHypernym;
	}
	
	/**
	 * Generate the Tree from the hypernym List
	 * @param hypernymList		The list of hypernyms (from child -> parent)
	 * @return					The root node of the tree
	 */
	public TreeNode<String> generateTree(List<String> hypernymList) {
		if(hypernymList == null) {
			throw new NullPointerException("The hypernymList inputed is null");
		}
		if(hypernymList.size() == 0) {
			throw new IllegalArgumentException("The hypernymList does not contain any content.");
		}
		
		int index = hypernymList.size() - 1;
		TreeNode<String> root = new TreeNode<String>(hypernymList.get(index));
		index--;
		TreeNode<String> previousTree = root;
		for(; index >= 0; index--) {
			previousTree.addChild(hypernymList.get(index));
			previousTree = previousTree.getChildren().get(0);
		}
		
		return root;
	}
	
	/**
	 * Add previous mapping values to the mappings
	 */
	public void addMappings(Map<String, String> previousMappings) {
		mappings.putAll(previousMappings);
	}
	
	/**
	 * Get the best part of speech guess for a word
	 * @param word		String containing the word
	 * @return			boolean is word is a noun 
	 */
	public boolean isNoun(String word) {
		String pos = PartOfSpeechHelper.bestPOS(wordnet, word);
		if(pos != null && pos.equals("n")) {
			return true;
		}
		return false;
	}
}
