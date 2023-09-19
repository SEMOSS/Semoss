//package prerna.algorithm.nlp;
//
//import java.io.StringReader;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//
//import edu.stanford.nlp.ling.CoreLabel;
//import edu.stanford.nlp.ling.IndexedWord;
//import edu.stanford.nlp.ling.LabeledWord;
//import edu.stanford.nlp.ling.TaggedWord;
//import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
//import edu.stanford.nlp.process.CoreLabelTokenFactory;
//import edu.stanford.nlp.process.PTBTokenizer;
//import edu.stanford.nlp.process.TokenizerFactory;
//import edu.stanford.nlp.semgraph.SemanticGraph;
//import edu.stanford.nlp.semgraph.SemanticGraphFactory;
//import edu.stanford.nlp.trees.EnglishGrammaticalRelations;
//import edu.stanford.nlp.trees.GrammaticalRelation;
//import edu.stanford.nlp.trees.GrammaticalStructure;
//import edu.stanford.nlp.trees.GrammaticalStructureFactory;
//import edu.stanford.nlp.trees.PennTreebankLanguagePack;
//import edu.stanford.nlp.trees.Tree;
//import edu.stanford.nlp.trees.TreeGraphNode;
//import edu.stanford.nlp.trees.TreebankLanguagePack;
//import edu.stanford.nlp.trees.TypedDependency;
//import rita.RiTa;
//import rita.RiWordNet;
//
//public final class NaturalLanguageProcessingHelper {
//
//	private static final Logger logger = LogManager.getLogger(NaturalLanguageProcessingHelper.class);
//	
//	private NaturalLanguageProcessingHelper() {
//
//	}
//	
//	public static String bestPOS(RiWordNet wordnet, String word) {
//		return wordnet.getBestPos(word);
//	}
//	
//	/**
//	 * Create a list of the type dependencies for each word in an inputed sentence
//	 * @param lp			The parser that generates the treebank of the sentence depdency 
//	 * @param sentence		The sentence to generate the typed dependencies list
//	 * @param tdl			The object to add the typed dependency list too
//	 * @param taggedWords	The list to add all the Parts of Speech (POS) for each word in the sentence
//	 * @return				Returns a boolean if the sentence was parsable
//	 */
//	public static SemanticGraph createDepList(LexicalizedParser lp, String sentence, List<TypedDependency> tdl, List<TaggedWord> taggedWords)
//	{
//		// Performs a Penn-Treebank Style Tokenization 
//		/* Basics of how this tokenization works (from http://www.cis.upenn.edu/~treebank/tokenization.html)
//		 * Most punctuation is split from adjoining words
//		 * Double quotes (") are changed to doubled single forward- and backward- quotes (`` and '')
//		 * Verb contractions and the Anglo-Saxon genitive of nouns are split into their component morphemes, and each morpheme is tagged separately
//		 * 		Examples:
//		 * 		children's --> children 's
//		 * 		parents' --> parents '
//		 * 		won't --> wo n't
//		 * 		gonna --> gon na
//		 * 		I'm --> I 'm
//		 * This tokenization allows us to analyze each component separately, so (for example) "I" can be in the subject Noun Phrase while "'m" is the head of the main verb phrase
//		 * There are some subtleties for hyphens vs. dashes, elipsis dots (...) and so on, but these often depend on the particular corpus or application of the tagged data.
//		 * In parsed corpora, bracket-like characters are converted to special 3-letter sequences, to avoid confusion with parse brackets. Some POS taggers, such as Adwait Ratnaparkhi's MXPOST, require this form for their input
//		 * 		In other words, these tokens in POS files: ( ) [ ] { } become, in parsed files: -LRB- -RRB- -RSB- -RSB- -LCB- -RCB- (The acronyms stand for (Left|Right) (Round|Square|Curly) Bracket.)
//		 */
//		TokenizerFactory<CoreLabel> tokenizerFactory = PTBTokenizer.factory(new CoreLabelTokenFactory(), "");
//		// Generate a list of all the words in the sentence
//		List<CoreLabel> rawWords = tokenizerFactory.getTokenizer(new StringReader(sentence)).tokenize();
//		// Generate a treebank that annotates the dependency structure of the sentence
//		Tree treeBank = lp.parseTree(rawWords);
//
//		// Give each word its Parts of Speech (POS)
//		try {
//			taggedWords.addAll(treeBank.taggedYield());
//		} catch(NullPointerException e) {
//			logger.info("The following sentence failed to be loadede:  " + sentence);
//			return null;
//		}
//		// Store the dependency relations between nodes in a tree
//		TreebankLanguagePack tlp = new PennTreebankLanguagePack();
//		GrammaticalStructureFactory gsf = tlp.grammaticalStructureFactory();
//		GrammaticalStructure gs = gsf.newGrammaticalStructure(treeBank);
//
//		// Get a list of typed dependencies, including control dependencies, by collapsing them and distributing relations across coordination
//		tdl.addAll(gs.typedDependenciesCCprocessed());
//		return SemanticGraphFactory.makeFromTree(gs);
//	}
//	
//	/**
//	 * Generate a key-value mapping between the grammatical relation to the word from the sentence
//	 * @param tdl			The type dependency list for every word in the sentence
//	 * @param nodeHash		The Hashtable to put the key-value mapping from the type dependency list
//	 */
//	public static void setTypeDependencyHash(List<TypedDependency> tdl, Map<GrammaticalRelation, List<TypedDependency>> nodeHash)
//	{
//		int i = 0;
//		int size = tdl.size();
//		for(; i < size; i++)
//		{
//			TypedDependency one = tdl.get(i);
//			List<TypedDependency> baseVector = new ArrayList<>();
//			GrammaticalRelation rel = one.reln();
//
//			//if this type of relation already exists
//			if(nodeHash.containsKey(rel)) {
//				baseVector = nodeHash.get(rel);
//			}
//			baseVector.add(one);
//			nodeHash.put(rel, baseVector);
//		}
//	}
//	
//	/**
//	 * Generate a key-value mapping between the grammatical relation to the word from the sentence
//	 * @param lp				The parser that generates the treebank of the sentence depdency 
//	 * @param searchString		The sentence to generate the typed dependencies list
//	 * @return					The Hashtable to put the key-value mapping from the type dependency list
//	 */
//	public static Map<GrammaticalRelation, List<TypedDependency>> getTypeDependencyHash(LexicalizedParser lp, String searchString)
//	{
//		List<TypedDependency> tdl = new ArrayList<>();
//		createDepList(lp, searchString, tdl, new ArrayList<TaggedWord>()); //create dependencies
//		Map<GrammaticalRelation, List<TypedDependency>> nodeHash = new HashMap<>();
//		setTypeDependencyHash(tdl, nodeHash);
//		return nodeHash;
//	}
//
//	/**
//	 * Store negation modifiers in Hashtable to use when creating triples
//	 * @param negHash		The Hashtable to add the negation modifiers to
//	 * @param nodeHash		The Hashtable containing the type dependencies
//	 */
//	public static void createNegations(Map<String, String> negHash, Map<GrammaticalRelation, List<TypedDependency>> nodeHash)
//	{
//		List<TypedDependency> negVector = nodeHash.get(EnglishGrammaticalRelations.NEGATION_MODIFIER);
//		if(negVector != null)
//		{
//			// run through each of these to see if I find any negation
//			int i = 0;
//			int size = negVector.size();
//			for(;i < size; i++)
//			{
//				TypedDependency neg = negVector.get(i);
//				String gov = neg.gov().toString();
//				negHash.put(gov, gov);
//			}
//		}
//	}
//	
//	/**
//	 * 
//	 * @param dobjV
//	 * @param subjV
//	 * @param nodeHash
//	 * @param subjR
//	 * @param objR
//	 * @return
//	 */
//	public static IndexedWord findPrepObject(List<TypedDependency> dobjV, List<TypedDependency> subjV, Map<GrammaticalRelation, List<TypedDependency>> nodeHash, 
//			GrammaticalRelation subjR, GrammaticalRelation objR) {
//		// based on the subjects and objects now find the predicates
//		dobjV = nodeHash.get(objR);
//		subjV = nodeHash.get(subjR);
//
//		if (dobjV != null && subjV != null) {
//			for (int dobjIndex = 0; dobjIndex < dobjV.size(); dobjIndex++) {
//				IndexedWord pobj = dobjV.get(dobjIndex).dep();
//				IndexedWord prep = dobjV.get(dobjIndex).gov();
//
//				// now find the subject
//				for (int subjIndex = 0; subjIndex < subjV.size(); subjIndex++) {
//					IndexedWord prep2 = subjV.get(subjIndex).dep();
//					if ((prep2 + "").equalsIgnoreCase(prep + "")) // this is the comparison to determine if there is a chain
//						return pobj;
//				}
//			}
//		}
//		return null;
//	}
//
//	/**
//	 * 
//	 * @param subj
//	 * @param nodeHash
//	 * @return
//	 */
//	// finds the expanded object
//	public static IndexedWord findCompObject(IndexedWord governor, Map<GrammaticalRelation, List<TypedDependency>> nodeHash)
//	{
//		IndexedWord retNode = governor;
//		List<TypedDependency> compVector = nodeHash.get(EnglishGrammaticalRelations.XCLAUSAL_COMPLEMENT);
//		if(compVector != null) {
//			int i = 0;
//			int size = compVector.size();
//			for(; i < size; i++) {
//				TypedDependency td = compVector.get(i);
//				if(td.dep() == retNode) {
//					retNode = td.gov();
//					break;
//				}
//			}
//		}
//
//		compVector = nodeHash.get(EnglishGrammaticalRelations.CLAUSAL_COMPLEMENT);
//		if(compVector != null) {
//			int i = 0;
//			int size = compVector.size();
//			for(; i < size; i++) {
//				TypedDependency td = compVector.get(i);
//				if(td.dep() == retNode) {
//					retNode = td.gov();
//					break;
//				}
//			}
//		}
//		return retNode;
//	}
//
//	/**
//	 * 
//	 * @param subj
//	 * @param nodeHash
//	 * @return
//	 */
//	// find expanded subject
//	public static IndexedWord findCompSubject(IndexedWord subj, Map<GrammaticalRelation, List<TypedDependency>> nodeHash)
//	{
//		IndexedWord retNode = subj;
//		List<TypedDependency> compVector = nodeHash.get(EnglishGrammaticalRelations.XCLAUSAL_COMPLEMENT);
//		boolean subjFound = false;
//		if(compVector != null)
//		{
//			for(int cInd = 0;cInd < compVector.size()&& !subjFound;cInd++)
//			{
//				TypedDependency td = compVector.get(cInd);
//				if(td.dep() == retNode)
//				{
//					retNode = td.gov();
//					subjFound = true;
//				}
//			}
//			compVector = nodeHash.get(EnglishGrammaticalRelations.CLAUSAL_COMPLEMENT);
//			subjFound = false;
//			if(compVector !=null)
//			{
//				for(int cInd = 0;cInd < compVector.size()&& !subjFound;cInd++)
//				{
//					TypedDependency td = compVector.get(cInd);
//					if(td.dep() == retNode)
//					{
//						retNode = td.gov();
//						subjFound = true;
//					}
//				}
//				compVector = nodeHash.get(EnglishGrammaticalRelations.NOMINAL_SUBJECT);
//				subjFound = false;
//				if(compVector != null){
//					for(int cInd = 0;cInd < compVector.size()&& !subjFound;cInd++)
//					{
//						TypedDependency td = compVector.get(cInd);
//						if(td.gov() == retNode)
//						{
//							retNode = td.dep();
//							subjFound = true;
//						}
//					}
//				}
//
//				return retNode;
//			}
//		}
//		return retNode;
//	}
//
//	/**
//	 * 
//	 * @param subj
//	 * @param dep2
//	 * @param nodeHash
//	 * @param relation
//	 * @return
//	 */
//	//sometimes the DAMN complement is recursive
//	public static IndexedWord findComplementNoun(IndexedWord subj, IndexedWord dep2, Map<GrammaticalRelation, List<TypedDependency>> nodeHash, GrammaticalRelation relation) {
//
//		IndexedWord retNode = subj;
//		// find all the complements
//		// find the one where the dep is the same as dep passed through
//		// now find a nsubj based on that new gov
//		// start with CComplement
//		List<TypedDependency> compVector = nodeHash.get(relation);
//		if(compVector != null)
//		{
//			for(int cInd = 0;cInd < compVector.size();cInd++)
//			{
//				TypedDependency td = compVector.get(cInd);
//				IndexedWord dep = td.dep();
//				IndexedWord gov = td.gov();
//				if(dep == dep2)
//				{
//					// now find the nsubj
//					List<TypedDependency> subjVector = nodeHash.get(EnglishGrammaticalRelations.NOMINAL_SUBJECT);
//					for(int subIndex = 0;subIndex < subjVector.size();subIndex++)
//					{
//						TypedDependency subTd = subjVector.get(subIndex);
//						if(subTd.gov() == gov)
//							retNode = subTd.dep();
//					}
//				}
//			}
//			return retNode;
//		}
//		return retNode;
//	}
//
//	/**
//	 * 
//	 * @param node
//	 * @return
//	 */
//	public static String getFullNoun(TreeGraphNode node)
//	{
//		String finalObject = "";
//		boolean npFound = false;
//		TreeGraphNode parentSearcher = node;
//		while(!npFound) {
//			if(parentSearcher != null && !parentSearcher.label().toString().startsWith("NP")) {
//				if(parentSearcher.parent() instanceof TreeGraphNode)
//					parentSearcher = (TreeGraphNode) parentSearcher.parent();
//				else {
//					npFound = true;
//					parentSearcher = null;
//				}
//			}
//			else {
//				npFound = true;
//				if (parentSearcher != null) {
//					List<LabeledWord> lw = parentSearcher.labeledYield();
//					// if this is not a noun then I need find the actual proper noun
//					// and it may be because there is a CCOMP or XCOMP with this label
//					// or there is an amod with this label
//					for(int labIndex = 0; labIndex < lw.size();labIndex++)
//					{
//						finalObject = finalObject + lw.get(labIndex).word();
//						if(labIndex != lw.size() - 1) {
//							finalObject += " ";
//						}
//					}
//				}
//			}
//		}
//		return finalObject;
//	}
//	
//	/**
//	 * 
//	 * @param graph
//	 * @param node
//	 * @return
//	 */
//	public static String getFullNoun(SemanticGraph graph, IndexedWord node) 
//	{
//		String finalObject = "";
//		boolean npFound = false;
//		IndexedWord parentSearcher = node;
//		while(!npFound) {
//			if(parentSearcher != null && !parentSearcher.value().startsWith("NP")) {
//				IndexedWord possibleParent = graph.getParent(parentSearcher);
//				if(possibleParent != null)
//					parentSearcher = possibleParent;
//				else {
//					npFound = true;
//					parentSearcher = null;
//				}
//			}
//			else {
//				npFound = true;
//				if (parentSearcher != null) {
//					List<IndexedWord> lw = graph.getChildList(parentSearcher);
//					// if this is not a noun then I need find the actual proper noun
//					// and it may be because there is a CCOMP or XCOMP with this label
//					// or there is an amod with this label
//					for(int labIndex = 0; labIndex < lw.size();labIndex++)
//					{
//						finalObject = finalObject + lw.get(labIndex).word();
//						if(labIndex != lw.size() - 1) {
//							finalObject += " ";
//						}
//					}
//				}
//			}
//		}
//		return finalObject;
//	}
//
//
//	/**
//	 * 
//	 * @param graph
//	 * @param noun
//	 * @param nodeHash
//	 * @return
//	 */
//	public static String findPrepNoun(SemanticGraph graph, IndexedWord noun, Map<GrammaticalRelation, List<TypedDependency>> nodeHash)
//	{
//		// given the preperator
//		// complete the string
//		String retString = noun.value();
//
//		if(!nodeHash.containsKey(EnglishGrammaticalRelations.PREPOSITIONAL_MODIFIER))
//			return retString;
//		List<TypedDependency> prepVector = nodeHash.get(EnglishGrammaticalRelations.PREPOSITIONAL_MODIFIER);
//		//prepVector.addAll(nodeHash.get(EnglishGrammaticalRelations.NOUN_COMPOUND_MODIFIER));
//		for(int prepIndex = 0;prepIndex < prepVector.size();prepIndex++)
//		{
//			TypedDependency tdl = prepVector.get(prepIndex);
//			IndexedWord gov = tdl.gov();
//			IndexedWord dep = tdl.dep();
//			if(noun == gov )
//			{
//				String fullNoun = getFullNoun(graph, dep);
//				if(dep != null && dep.value() != null && fullNoun.equalsIgnoreCase(dep.value())) {
//					retString = retString + " " + tdl.reln().getSpecific() + " " + fullNoun + findPrepNoun(graph, dep, nodeHash);
//				} else {
//					retString = retString + " " + tdl.reln().getSpecific() + " " + fullNoun + findPrepNoun(graph, dep, nodeHash).replace(dep.value(), "");
//				}
//			}
//		}
//		return retString;
//	}
//
//	/**
//	 * 
//	 * @param graph
//	 * @param noun
//	 * @param nodeHash
//	 * @return
//	 */
//	public static String findPrepNounForPredicate(SemanticGraph graph, IndexedWord noun, Map<GrammaticalRelation, List<TypedDependency>> nodeHash)
//	{
//		// given the preperator
//		// complete the string
//		String retString = "";
//
//		if(!nodeHash.containsKey(EnglishGrammaticalRelations.PREPOSITIONAL_MODIFIER))
//			return retString;
//		List<TypedDependency> prepVector = nodeHash.get(EnglishGrammaticalRelations.PREPOSITIONAL_MODIFIER);
//		for(int prepIndex = 0;prepIndex < prepVector.size();prepIndex++)
//		{
//			TypedDependency tdl = prepVector.get(prepIndex);
//			IndexedWord gov = tdl.gov();
//			IndexedWord dep = tdl.dep();
//			if(noun == gov )
//			{				 
//				String fullNoun = getFullNoun(graph, dep);
//				if(dep != null && dep.value() != null && fullNoun.equalsIgnoreCase(dep.value())) {
//					retString = retString + " " + tdl.reln().getSpecific() + " " + fullNoun + findPrepNoun(graph, dep, nodeHash);
//				} else {
//					retString = retString + " " + tdl.reln().getSpecific() + " " + fullNoun;
//				}
//			}
//		}
//		return retString;
//	}
//	
//	
////	public static String[] determineMostSimilarValues(RiWordNet wordnet, Object[] col) {
////		
////		String[] mostSimilarSet = null;
////		double simVal = 2.0; // set to arbitrary value larger than 1
////		
////		int i = 0;
////		int size = col.length;
////		for(; i < size; i++) {
////			String s1 = col[i].toString();
////			String s_s1 = RiTa.singularize(s1.toLowerCase());
////			int j = i+1;
////			// compare against all other strings in col
////			for(; j < size; j++) {
////				String s2 = col[j].toString();
////				String s_s2 = RiTa.singularize(s2.toLowerCase());
////				double newSim = wordnet.getDistance(s_s1, s_s2, "n");
////				if(newSim < simVal) {
////					mostSimilarSet = new String[]{s1, s2};
////					simVal = newSim;
////				}
////			}
////		}
////		
////		return mostSimilarSet;
////	}
//	
//	public static double determineAverageMinimumSimilarity(RiWordNet wordnet, Object[] col) {
//		
//		int numOfVals = 0;
//		double sumMinSimilarity = 0.0; // set to arbitrary value larger than 1
//		
//		int size = col.length;
//		for(int i=0; i < size; i++) {
//			String s1 = col[i].toString();
//			String ss1 = RiTa.singularize(s1.toLowerCase());
//			// compare against all other strings in col
//			double minSimVal = 2.0;
//			for(int j=0; j < size; j++) {
//				if(i!=j) {
//					String s2 = col[j].toString();
//					String ss2 = RiTa.singularize(s2.toLowerCase());
//					double newSim = wordnet.getDistance(ss1, ss2, "n");
//					if(newSim < minSimVal) {
//						minSimVal = newSim;
//					}
//				}
//			}
//			if(minSimVal != 1) {
//				numOfVals++;
//				sumMinSimilarity += minSimVal;
//			}
//		}
//	    if(numOfVals == 0) {
//	        throw new IllegalArgumentException("numOfVals can not be 0");
//	      }
//		double avgMinSimilarity = sumMinSimilarity / numOfVals;
//
//		return avgMinSimilarity;
//	}
//}
