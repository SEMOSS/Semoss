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
package prerna.poi.main;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Scanner;
import java.util.Vector;
import java.util.regex.Pattern;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tika.exception.TikaException;
import org.xml.sax.SAXException;

import prerna.error.NLPException;
import prerna.util.DIHelper;
import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.LabeledWord;
import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.process.CoreLabelTokenFactory;
import edu.stanford.nlp.process.PTBTokenizer;
import edu.stanford.nlp.process.TokenizerFactory;
import edu.stanford.nlp.trees.EnglishGrammaticalRelations;
import edu.stanford.nlp.trees.GrammaticalRelation;
import edu.stanford.nlp.trees.GrammaticalStructure;
import edu.stanford.nlp.trees.GrammaticalStructureFactory;
import edu.stanford.nlp.trees.PennTreebankLanguagePack;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreeGraphNode;
import edu.stanford.nlp.trees.TreebankLanguagePack;
import edu.stanford.nlp.trees.TypedDependency;
import edu.stanford.nlp.util.CoreMap;

public class ProcessNLP {

	private List<TripleWrapper> triples; // list of all triples stored after running findVerbTriples()
	private LexicalizedParser lp;

	private static final Logger LOGGER = LogManager.getLogger(ProcessNLP.class.getName());

	public ProcessNLP(){
		lp = LexicalizedParser.loadModel(DIHelper.getInstance().getProperty("BaseFolder")+"\\NLPartifacts\\englishPCFG.ser");
		lp.setOptionFlags(new String[]{"-maxLength", "80", "-retainTmpSubcategories"});
	}

	public List<TripleWrapper> generateTriples(String[] files) throws NLPException {
		triples = new ArrayList<TripleWrapper>();
		int i = 0;
		int size = files.length;
		for(; i < size; i++){	
			processFile(files[i]);
		}

		trimTriples();
		createOccuranceCount();
		lemmatize();
		normalizeCase();
		return triples;
	}

	/**
	 * Generates all the triples for the file or web-page
	 * @param file				String representing the file path
	 * @throws NLPException		
	 */
	private void processFile(String file) throws NLPException {
		// Returns a list of all the sentences in a file/web-page
		List<String> fileSentences = readDoc(file);

		int i = 0;
		int size = fileSentences.size();
		for(; i < size; i++) {
			String sentence = fileSentences.get(i);
			
			List<TypedDependency> tdl = new ArrayList<TypedDependency>();
			List<TaggedWord> taggedWords = new ArrayList<TaggedWord>();

			boolean sentenceParsable = createDepList(sentence, tdl, taggedWords); //create dependencies
			if(sentenceParsable)
			{
				Hashtable<GrammaticalRelation, Vector<TypedDependency>> nodeHash = new Hashtable<GrammaticalRelation, Vector<TypedDependency>>();
				Hashtable<String, String> negHash = new Hashtable<String, String>();
				// fill the hashtable between the grammatical part of speech to the words in the sentence
				setHash(tdl, nodeHash);
				generateTriples(sentence, file.substring(file.lastIndexOf(File.separator)+1), taggedWords, negHash, nodeHash);
			}
		}
	}

	/**
	 * Returns a list of all the sentences in a file or web-page
	 * @param file				String representing the file path
	 * @return					List<String> containing the sentences in the file
	 * @throws NLPException
	 */
	public List<String> readDoc(String file) throws NLPException {
		// Use a text extractor to grab all the sentences in a file or web-page

		List<String> fileSentences = new ArrayList<String>();
		if(file.contains("http")) {
			LOGGER.info("Extracting text from a web-page...");
			try {
				readFile(TextExtractor.websiteTextExtractor(file), fileSentences);
			} catch (IOException e) {
				e.printStackTrace();
				throw new NLPException("Error processing website");
			}
		}
		if(file.endsWith(".doc") || file.endsWith(".docx")){
			LOGGER.info("Extracting text from a word document...");
			try {
				readFile(TextExtractor.fileTextExtractor(file), fileSentences);
			} catch (IOException e) {
				e.printStackTrace();
				throw new NLPException("Error extrating text from word doc");
			} catch (SAXException e) {
				e.printStackTrace();
				throw new NLPException("Error extrating text from word doc");
			} catch (TikaException e) {
				e.printStackTrace();
				throw new NLPException("Error extrating text from word doc");
			}
		}
		if(file.endsWith(".txt"))
		{
			LOGGER.info("Extracting text from a text file...");
			try {
				readFile(TextExtractor.fileTextExtractor(file), fileSentences);
			} catch (IOException e) {
				e.printStackTrace();
				throw new NLPException("Error extrating text from document");
			} catch (SAXException e) {
				e.printStackTrace();
				throw new NLPException("Error extrating text from document");
			} catch (TikaException e) {
				e.printStackTrace();
				throw new NLPException("Error extrating text from document");
			}
		}
		return fileSentences;
	}

	/**
	 * Fills in a List<String> with all the sentences in a string
	 * @param text				The String containing sentences
	 * @param fileSentences		The List<String> to fill with all the sentences in the String passed in
	 */
	private void readFile(String text, List<String> fileSentences) {
		Pattern p = Pattern.compile("(?<!Mr)(?<!Mrs)(?<!Dr)(?<!Ms)(?<!\\.[A-Z])\\. *\\s|\\? *\\s|\\! *\\s");
		Scanner scan = new Scanner(text);
		scan.useDelimiter(p);
		while (scan.hasNext()) {
			fileSentences.add(scan.next().replaceAll("\\r\\n|\\r|\\n", " ").replace("\n", "").replace("\r", ""));
		}
		scan.close();
	}
	
	/**
	 * Create a list of the type dependencies for each word in an inputed sentence
	 * @param sentence		The sentence to generate the typed dependencies list
	 * @param tdl			The object to add the typed dependency list too
	 * @param taggedWords	The list to add all the Parts of Speech (POS) for each word in the sentence
	 * @return				Returns a boolean if the sentence was parsable
	 */
	public boolean createDepList(String sentence, List<TypedDependency> tdl, List<TaggedWord> taggedWords)
	{
		// Performs a Penn-Treebank Style Tokenization 
		/* Basics of how this tokenization works (from http://www.cis.upenn.edu/~treebank/tokenization.html)
		 * Most punctuation is split from adjoining words
		 * Double quotes (") are changed to doubled single forward- and backward- quotes (`` and '')
		 * Verb contractions and the Anglo-Saxon genitive of nouns are split into their component morphemes, and each morpheme is tagged separately
		 * 		Examples:
		 * 		children's --> children 's
		 * 		parents' --> parents '
		 * 		won't --> wo n't
		 * 		gonna --> gon na
		 * 		I'm --> I 'm
		 * This tokenization allows us to analyze each component separately, so (for example) "I" can be in the subject Noun Phrase while "'m" is the head of the main verb phrase
		 * There are some subtleties for hyphens vs. dashes, elipsis dots (...) and so on, but these often depend on the particular corpus or application of the tagged data.
		 * In parsed corpora, bracket-like characters are converted to special 3-letter sequences, to avoid confusion with parse brackets. Some POS taggers, such as Adwait Ratnaparkhi's MXPOST, require this form for their input
		 * 		In other words, these tokens in POS files: ( ) [ ] { } become, in parsed files: -LRB- -RRB- -RSB- -RSB- -LCB- -RCB- (The acronyms stand for (Left|Right) (Round|Square|Curly) Bracket.)
		 */
		TokenizerFactory<CoreLabel> tokenizerFactory = PTBTokenizer.factory(new CoreLabelTokenFactory(), "");
		// Generate a list of all the words in the sentence
		List<CoreLabel> rawWords = tokenizerFactory.getTokenizer(new StringReader(sentence)).tokenize();
		// Generate a treebank that annotates the dependency structure of the sentence
		Tree treeBank = lp.parseTree(rawWords);

		// Give each word its Parts of Speech (POS)
		try {
			taggedWords.addAll(treeBank.taggedYield());
		} catch(NullPointerException e) {
			LOGGER.info("The following sentence failed to be loadede:  " + sentence);
			return false;
		}
		// Store the dependency relations between nodes in a tree
		TreebankLanguagePack tlp = new PennTreebankLanguagePack();
		GrammaticalStructureFactory gsf = tlp.grammaticalStructureFactory();
		GrammaticalStructure gs = gsf.newGrammaticalStructure(treeBank);

		// Get a list of typed dependencies, including control dependencies, by collapsing them and distributing relations across coordination
		tdl.addAll(gs.typedDependenciesCCprocessed());
		return true;
	}

	/**
	 * Generate a key-value mapping between the grammatical relation to the word from the sentence
	 * @param tdl			The type dependency list for every word in the sentence
	 * @param nodeHash		The Hashtable to put the key-value mapping from the type dependency list
	 */
	public void setHash(List<TypedDependency> tdl, Hashtable <GrammaticalRelation, Vector<TypedDependency>> nodeHash)
	{
		int i = 0;
		int size = tdl.size();
		for(; i < size; i++)
		{
			TypedDependency one = tdl.get(i);
			Vector<TypedDependency> baseVector = new Vector<TypedDependency>();
			GrammaticalRelation rel = one.reln();

			//if this type of relation already exists
			if(nodeHash.containsKey(rel)) {
				baseVector = nodeHash.get(rel);
			}
			baseVector.addElement(one);
			nodeHash.put(rel, baseVector);
		}
	}

	public void generateTriples(String sentence, String documentName, List<TaggedWord> taggedWords, Hashtable<String, String> negHash, Hashtable<GrammaticalRelation, Vector<TypedDependency>> nodeHash)
	{
		createNegations(negHash, nodeHash);
		// I ate the sandwich. -> I, ate, sandwich (“the” is included in expanded object.)
		findTriples(sentence, documentName, taggedWords, negHash, nodeHash, EnglishGrammaticalRelations.NOMINAL_SUBJECT, EnglishGrammaticalRelations.DIRECT_OBJECT);
		// The man has been killed by the police. -> man, killed, police (Requires Collapsed Dependencies)
		findTriples(sentence, documentName, taggedWords, negHash, nodeHash, EnglishGrammaticalRelations.AGENT, EnglishGrammaticalRelations.NOMINAL_PASSIVE_SUBJECT);
		// TODO: this no longer exists in jar version 3.5, need to replace controlling_subject
		findTriples(sentence, documentName, taggedWords, negHash, nodeHash, EnglishGrammaticalRelations.CONTROLLING_SUBJECT, EnglishGrammaticalRelations.DIRECT_OBJECT);
		// I sat on the chair. -> I, sat, on (without our code)
		findTriples(sentence, documentName, taggedWords, negHash, nodeHash, EnglishGrammaticalRelations.NOMINAL_PASSIVE_SUBJECT, EnglishGrammaticalRelations.PREPOSITIONAL_MODIFIER);
		// He is tall. -> He, tall, is (without our code)
		findTriples(sentence, documentName, taggedWords, negHash, nodeHash, EnglishGrammaticalRelations.NOMINAL_SUBJECT, EnglishGrammaticalRelations.COPULA);
		// She looks beautiful. -> She, looks, beautiful
		findTriples(sentence, documentName, taggedWords, negHash, nodeHash, EnglishGrammaticalRelations.NOMINAL_SUBJECT, EnglishGrammaticalRelations.ADJECTIVAL_COMPLEMENT);
		// I will sit on the chair. -> I, sit, on (without our code)
		findTriples(sentence, documentName, taggedWords, negHash, nodeHash, EnglishGrammaticalRelations.NOMINAL_SUBJECT, EnglishGrammaticalRelations.PREPOSITIONAL_MODIFIER);
	}

	/**
	 * Store negation modifiers in Hashtable to use when creating triples
	 * @param negHash		The Hashtable to add the negation modifiers to
	 * @param nodeHash		The Hashtable containing the type dependencies
	 */
	public void createNegations(Hashtable<String, String> negHash, Hashtable<GrammaticalRelation, Vector<TypedDependency>> nodeHash)
	{
		Vector <TypedDependency> negVector = nodeHash.get(EnglishGrammaticalRelations.NEGATION_MODIFIER);
		if(negVector != null)
		{
			// run through each of these to see if I find any negation
			int i = 0;
			int size = negVector.size();
			for(;i < size; i++)
			{
				TypedDependency neg = negVector.elementAt(i);
				String gov = neg.gov().toString();
				negHash.put(gov, gov);
			}
		}
	}

	public void findTriples(
			String sentence,
			String documentName,
			List<TaggedWord> taggedWords, 
			Hashtable<String, String> negHash, 
			Hashtable<GrammaticalRelation, Vector<TypedDependency>> nodeHash, 
			GrammaticalRelation subjR, 
			GrammaticalRelation objR)
	{
		// based on the subjects and objects now find the predicates
		Vector<TypedDependency> dobjV = nodeHash.get(objR);
		Vector<TypedDependency> subjV = nodeHash.get(subjR);

		if(dobjV != null && subjV != null)
		{
			for(int dobjIndex = 0;dobjIndex < dobjV.size();dobjIndex++)
			{
				TreeGraphNode obj = dobjV.get(dobjIndex).dep();
				TreeGraphNode pred = dobjV.get(dobjIndex).gov();
				String predicate = pred.value(); // Note: value doesn't return the number, while toString does
				 
				String preposition = null;
				if (dobjV.get(dobjIndex).toString().contains("prep")) {
					obj = findPrepObject(dobjV, subjV, nodeHash, EnglishGrammaticalRelations.PREPOSITIONAL_MODIFIER, EnglishGrammaticalRelations.PREPOSITIONAL_OBJECT);
					preposition = dobjV.get(dobjIndex).dep().toString();
				}
				
				// now find the subject
				for(int subjIndex = 0;subjIndex < subjV.size();subjIndex++)
				{
					TreeGraphNode subj = subjV.get(subjIndex).dep();
					TreeGraphNode dep2 = subjV.get(subjIndex).gov();
					// Test to make sure both words have the same governor -> i.e. they are connected in the sentence
					if((dep2.toString()).equalsIgnoreCase(pred.toString()))
					{
						// JJ = adjective
						// If predicate gets stored as adjective (usually occurs for adjectivial), gets stored as predicate while verb gets stored as obj -> switch the two
						if (subj.label().tag().contains("JJ")) {
							TreeGraphNode tempNode = pred;
							pred = obj;
							obj = tempNode;
						}
						
						//CORE TRIPLES FOUND
						TripleWrapper tripleContainer = new TripleWrapper();
						tripleContainer.setObj1(subj.value());
						tripleContainer.setPred(pred.value());
						tripleContainer.setObj2(obj.value());

						//FINDING EXTENSION OF SUBJECT****
						// find if complemented
						// need to do this only if the subj is not a noun
						// final subject
						TreeGraphNode altPredicate = findCompObject(dep2, nodeHash);
						if(!subj.label().tag().contains("NN") && ( nodeHash.containsKey(EnglishGrammaticalRelations.CLAUSAL_COMPLEMENT) || nodeHash.containsKey(EnglishGrammaticalRelations.XCLAUSAL_COMPLEMENT)))
						{
							subj = findComplementNoun(subj, dep2, nodeHash, EnglishGrammaticalRelations.CLAUSAL_COMPLEMENT);
							if(!subj.label().tag().contains("NN")){
								subj = findCompSubject(dep2, nodeHash);
							}
						}		

						String finalSubject = getFullNoun(subj);
						String finalObject = getFullNoun(obj);
						finalObject = finalObject + findPrepNounForPredicate(pred, nodeHash);

						//FINDING EXTENSION OF PREDICATE****
						// find the negators for the predicates next
						if(negHash.containsKey(pred + "")|| negHash.containsKey(altPredicate + "")) {
							predicate = "NOT " + predicate;
						}
						
						// I sat on a chair -> I -> sat on -> chair 
						if (preposition != null) {
							predicate += preposition;
						}

						//EXTENSION OF OBJECT FOUND****
						// fulcrum on the nsubj to see if there is an NNP in the vicinity
						if(finalObject.indexOf(predicate) < 0 && predicate.indexOf(finalObject) < 0) {
							LOGGER.info("VERB Triple: 	" + finalSubject + "<<>>" + predicate + "<<>>" + finalObject);
						}
						tripleContainer.setObj1Expanded(finalSubject.toString());// part of future SetTriple
						tripleContainer.setPredExpanded(predicate.toString());
						tripleContainer.setObj2Expanded(finalObject.toString());
						tripleContainer.setDocName(documentName);
						tripleContainer.setSentence(sentence);
						
						triples.add(tripleContainer);
					}
				}
			}
		}
	}

	private TreeGraphNode findPrepObject(Vector<TypedDependency> dobjV, Vector<TypedDependency> subjV, Hashtable<GrammaticalRelation, Vector<TypedDependency>> nodeHash, GrammaticalRelation subjR, GrammaticalRelation objR) {
			// based on the subjects and objects now find the predicates
			dobjV = nodeHash.get(objR);
			subjV = nodeHash.get(subjR);
			
			if (dobjV != null && subjV != null) {
				for (int dobjIndex = 0; dobjIndex < dobjV.size(); dobjIndex++) {
					TreeGraphNode pobj = dobjV.get(dobjIndex).dep();
					TreeGraphNode prep = dobjV.get(dobjIndex).gov();
					
					// now find the subject
					for (int subjIndex = 0; subjIndex < subjV.size(); subjIndex++) {
						TreeGraphNode prep2 = subjV.get(subjIndex).dep();
						if ((prep2 + "").equalsIgnoreCase(prep + "")) // this is the comparison to determine if there is a chain
							return pobj;
					}
				}
			}
			return null;
		}

	
	// finds the expanded object
	public TreeGraphNode findCompObject(TreeGraphNode subj, Hashtable<GrammaticalRelation, Vector<TypedDependency>> nodeHash)
	{
		TreeGraphNode retNode = subj;
		Vector <TypedDependency> compVector = nodeHash.get(EnglishGrammaticalRelations.XCLAUSAL_COMPLEMENT);
		boolean subjFound = false;
		if(compVector != null)
		{
			for(int cInd = 0;cInd < compVector.size()&& !subjFound;cInd++)
			{
				TypedDependency td = compVector.elementAt(cInd);
				if(td.dep() == retNode)
				{
					retNode = td.gov();
					subjFound = true;
				}
			}
		}

		compVector = nodeHash.get(EnglishGrammaticalRelations.CLAUSAL_COMPLEMENT);
		if(compVector != null)
		{
			subjFound = false;

			for(int cInd = 0;cInd < compVector.size()&& !subjFound;cInd++)
			{
				TypedDependency td = compVector.elementAt(cInd);
				if(td.dep() == retNode)
				{
					retNode = td.gov();
					subjFound = true;
				}
			}
		}
		return retNode;
	}

	// find expanded subject
	public TreeGraphNode findCompSubject(TreeGraphNode subj, Hashtable<GrammaticalRelation, Vector<TypedDependency>> nodeHash)
	{
		TreeGraphNode retNode = subj;
		Vector <TypedDependency> compVector = nodeHash.get(EnglishGrammaticalRelations.XCLAUSAL_COMPLEMENT);
		boolean subjFound = false;
		if(compVector != null)
		{
			for(int cInd = 0;cInd < compVector.size()&& !subjFound;cInd++)
			{
				TypedDependency td = compVector.elementAt(cInd);
				if(td.dep() == retNode)
				{
					retNode = td.gov();
					subjFound = true;
				}
			}
			compVector = nodeHash.get(EnglishGrammaticalRelations.CLAUSAL_COMPLEMENT);
			subjFound = false;
			if(compVector !=null)
			{
				for(int cInd = 0;cInd < compVector.size()&& !subjFound;cInd++)
				{
					TypedDependency td = compVector.elementAt(cInd);
					if(td.dep() == retNode)
					{
						retNode = td.gov();
						subjFound = true;
					}
				}
				compVector = nodeHash.get(EnglishGrammaticalRelations.NOMINAL_SUBJECT);
				subjFound = false;
				if(compVector != null){
					for(int cInd = 0;cInd < compVector.size()&& !subjFound;cInd++)
					{
						TypedDependency td = compVector.elementAt(cInd);
						if(td.gov() == retNode)
						{
							retNode = td.dep();
							subjFound = true;
						}
					}
				}

				return retNode;
			}
		}
		return retNode;
	}

	//sometimes the DAMN complement is recursive
	public TreeGraphNode findComplementNoun(TreeGraphNode subj, TreeGraphNode dep2, Hashtable<GrammaticalRelation, Vector<TypedDependency>> nodeHash, GrammaticalRelation relation) {

		TreeGraphNode retNode = subj;
		// find all the complements
		// find the one where the dep is the same as dep passed through
		// now find a nsubj based on that new gov
		// start with CComplement
		Vector <TypedDependency> compVector = nodeHash.get(relation);
		if(compVector != null)
		{
			for(int cInd = 0;cInd < compVector.size();cInd++)
			{
				TypedDependency td = compVector.elementAt(cInd);
				TreeGraphNode dep = td.dep();
				TreeGraphNode gov = td.gov();
				if(dep == dep2)
				{
					// now find the nsubj
					Vector <TypedDependency> subjVector = nodeHash.get(EnglishGrammaticalRelations.NOMINAL_SUBJECT);
					for(int subIndex = 0;subIndex < subjVector.size();subIndex++)
					{
						TypedDependency subTd = subjVector.elementAt(subIndex);
						if(subTd.gov() == gov)
							retNode = subTd.dep();
					}
				}
			}
			return retNode;
		}
		return retNode;
	}

	public String getFullNoun(TreeGraphNode node)
	{
		String finalObject = "";
		boolean npFound = false;
		TreeGraphNode parentSearcher = node;
		while(!npFound)
		{
			if(!parentSearcher.label().toString().startsWith("NP"))
			{
				if(parentSearcher.parent() instanceof TreeGraphNode)
					parentSearcher = (TreeGraphNode)parentSearcher.parent();
				else
				{
					npFound = true;
					parentSearcher = null;
				}
			}
			else 
			{
				npFound = true;
				List<LabeledWord> lw = parentSearcher.labeledYield();
				// if this is not a noun then I need find the actual proper noun
				// and it may be because there is a CCOMP or XCOMP with this label
				// or there is an amod with this label
				for(int labIndex = 0; labIndex < lw.size();labIndex++)
				{
					finalObject = finalObject + lw.get(labIndex).word();
					if(labIndex != lw.size() - 1) {
						finalObject += " ";
					}
				}
			}
		}
		return finalObject;
	}

	public String findPrepNoun(TreeGraphNode noun, Hashtable<GrammaticalRelation, Vector<TypedDependency>> nodeHash)
	{
		// given the preperator
		// complete the string
		String retString = noun.value();

		if(!nodeHash.containsKey(EnglishGrammaticalRelations.PREPOSITIONAL_MODIFIER))
			return retString;
		Vector <TypedDependency> prepVector = nodeHash.get(EnglishGrammaticalRelations.PREPOSITIONAL_MODIFIER);
		//prepVector.addAll(nodeHash.get(EnglishGrammaticalRelations.NOUN_COMPOUND_MODIFIER));
		for(int prepIndex = 0;prepIndex < prepVector.size();prepIndex++)
		{
			TypedDependency tdl = prepVector.elementAt(prepIndex);
			TreeGraphNode gov = tdl.gov();
			TreeGraphNode dep = tdl.dep();
			if(noun == gov )
			{
				String fullNoun = getFullNoun(dep);
				if(fullNoun.equalsIgnoreCase(dep.value()))
					retString = retString + " " + tdl.reln().getSpecific() + " " + fullNoun + findPrepNoun(dep, nodeHash);
				else
					retString = retString + " " + tdl.reln().getSpecific() + " " + fullNoun + findPrepNoun(dep, nodeHash).replace(dep.value(), "");
			}
		}
		return retString;
	}

	public String findPrepNounForPredicate(TreeGraphNode noun, Hashtable<GrammaticalRelation, Vector<TypedDependency>> nodeHash)
	{
		// given the preperator
		// complete the string
		String retString = "";

		if(!nodeHash.containsKey(EnglishGrammaticalRelations.PREPOSITIONAL_MODIFIER))
			return retString;
		Vector <TypedDependency> prepVector = nodeHash.get(EnglishGrammaticalRelations.PREPOSITIONAL_MODIFIER);
		for(int prepIndex = 0;prepIndex < prepVector.size();prepIndex++)
		{
			TypedDependency tdl = prepVector.elementAt(prepIndex);
			TreeGraphNode gov = tdl.gov();
			TreeGraphNode dep = tdl.dep();
			if(noun == gov )
			{				 
				String fullNoun = getFullNoun(dep);
				if(fullNoun.equalsIgnoreCase(dep.value()))
					retString = retString + " " + tdl.reln().getSpecific() + " " + fullNoun + findPrepNoun(dep, nodeHash);
				else
					retString = retString + " " + tdl.reln().getSpecific() + " " + fullNoun;
			}
		}
		return retString;
	}

	//TODO: Should create new method cleanUpTriples
	private void trimTriples() {
		int i = 0;
		int numTriples = triples.size();
		for(; i < numTriples; i++) {
			//set the expanded to NA if given no value
			if(triples.get(i).getObj1Expanded().equals("")){
				triples.get(i).setObj1Expanded("NA");
			}
			if(triples.get(i).getPredExpanded().equals("")){
				triples.get(i).setPredExpanded("NA");
			}
			if(triples.get(i).getObj2Expanded().equals("")){
				triples.get(i).setObj2Expanded("NA");
			}
			triples.get(i).setObj1Expanded(triples.get(i).getObj1Expanded().toString().replace("'", ",").replace("`", ","));
			triples.get(i).setPredExpanded(triples.get(i).getPredExpanded().toString().replace("'", ",").replace("`", ","));
			triples.get(i).setObj2Expanded(triples.get(i).getObj2Expanded().toString().replace("'", ",").replace("`", ","));
		}
	}

	private void createOccuranceCount() {
		Hashtable<String, Integer> termCounts = new Hashtable<String, Integer>();
		int i = 0;
		int numTriples = triples.size();
		for(; i < numTriples; i++){
			String[] keys = new String[]{triples.get(i).getObj1(), triples.get(i).getPred(), triples.get(i).getObj2()};
			for(String key : keys) {
				if(termCounts.containsKey(key)) {
					int val = termCounts.get(key);
					termCounts.put(key, val+1);
				} else {
					termCounts.put(key, 1);
				}
			}
		}
		
		i = 0;
		for(; i < numTriples; i++) {
			triples.get(i).setObj1Count(termCounts.get(triples.get(i).getObj1()));
			triples.get(i).setPredCount(termCounts.get(triples.get(i).getPred()));
			triples.get(i).setObj2Count(termCounts.get(triples.get(i).getObj2()));
		}
	}

	public void lemmatize() {
		StanfordCoreNLP pipeline = new StanfordCoreNLP();
		
		int i = 0;
		int size = triples.size();
		for(; i < size; i++){
			Annotation document = new Annotation(triples.get(i).getPred());
			pipeline.annotate(document);
			List<CoreMap> sentences = document.get(SentencesAnnotation.class);
			for(CoreMap sentence: sentences) {
				for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
					LOGGER.info("lemmatized " + token.get(LemmaAnnotation.class));
					LOGGER.info("original   " + triples.get(i).getPred()); 
					triples.get(i).setPred(token.get(LemmaAnnotation.class));
				}
			}
		}
	}

	//TODO: Should be added in trim triples to create new method cleanUpTriples
	private void normalizeCase() {
		int i = 0;
		int size = triples.size();
		for(; i < size; i++){
			triples.get(i).setObj1(triples.get(i).getObj1().toLowerCase());
			triples.get(i).setPred(triples.get(i).getPred().toLowerCase());
			triples.get(i).setObj2(triples.get(i).getObj2().toLowerCase());
		}
	}

}
