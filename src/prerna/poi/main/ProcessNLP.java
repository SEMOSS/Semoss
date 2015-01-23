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

import prerna.algorithm.nlp.PartOfSpeechHelper;
import prerna.error.NLPException;
import prerna.util.DIHelper;
import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.trees.EnglishGrammaticalRelations;
import edu.stanford.nlp.trees.GrammaticalRelation;
import edu.stanford.nlp.trees.TreeGraphNode;
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

		createOccuranceCount();
		lemmatize();
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

			boolean sentenceParsable = PartOfSpeechHelper.createDepList(lp, sentence, tdl, taggedWords); //create dependencies
			if(sentenceParsable)
			{
				Hashtable<GrammaticalRelation, Vector<TypedDependency>> nodeHash = new Hashtable<GrammaticalRelation, Vector<TypedDependency>>();
				Hashtable<String, String> negHash = new Hashtable<String, String>();
				// fill the hashtable between the grammatical part of speech to the words in the sentence
				PartOfSpeechHelper.setTypeDependencyHash(tdl, nodeHash);
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
	
	public void generateTriples(String sentence, String documentName, List<TaggedWord> taggedWords, Hashtable<String, String> negHash, Hashtable<GrammaticalRelation, Vector<TypedDependency>> nodeHash)
	{
		PartOfSpeechHelper.createNegations(negHash, nodeHash);
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
	 * 
	 * @param sentence
	 * @param documentName
	 * @param taggedWords
	 * @param negHash
	 * @param nodeHash
	 * @param subjR
	 * @param objR
	 */
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
			int i = 0;
			int dobjVSize = dobjV.size();
			for(; i < dobjVSize; i++)
			{
				TreeGraphNode obj = dobjV.get(i).dep();
				TreeGraphNode pred = dobjV.get(i).gov();
				String predicate = pred.value(); // Note: value doesn't return the number, while toString does
				 
				String preposition = null;
				if (dobjV.get(i).toString().contains("prep")) {
					obj = PartOfSpeechHelper.findPrepObject(dobjV, subjV, nodeHash, EnglishGrammaticalRelations.PREPOSITIONAL_MODIFIER, EnglishGrammaticalRelations.PREPOSITIONAL_OBJECT);
					preposition = dobjV.get(i).dep().toString();
				}
				
				// now find the subject
				int j = 0;
				int subjVSize = subjV.size();
				for(; j < subjVSize; j++)
				{
					TreeGraphNode subj = subjV.get(j).dep();
					TreeGraphNode dep2 = subjV.get(j).gov();
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
						tripleContainer.setObj1(formatString(subj.value(), false, true));
						tripleContainer.setPred(formatString(pred.value(), false, true));
						tripleContainer.setObj2(formatString(obj.value(), false, true));

						//FINDING EXTENSION OF SUBJECT****
						// find if complemented
						// need to do this only if the subj is not a noun
						// final subject
						TreeGraphNode altPredicate = PartOfSpeechHelper.findCompObject(dep2, nodeHash);
						if(!subj.label().tag().contains("NN") && ( nodeHash.containsKey(EnglishGrammaticalRelations.CLAUSAL_COMPLEMENT) || nodeHash.containsKey(EnglishGrammaticalRelations.XCLAUSAL_COMPLEMENT)))
						{
							subj = PartOfSpeechHelper.findComplementNoun(subj, dep2, nodeHash, EnglishGrammaticalRelations.CLAUSAL_COMPLEMENT);
							if(!subj.label().tag().contains("NN")){
								subj = PartOfSpeechHelper.findCompSubject(dep2, nodeHash);
							}
						}		

						String finalSubject = PartOfSpeechHelper.getFullNoun(subj);
						String finalObject = PartOfSpeechHelper.getFullNoun(obj);
						finalObject = finalObject + PartOfSpeechHelper.findPrepNounForPredicate(pred, nodeHash);

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
						tripleContainer.setObj1Expanded(formatString(finalSubject.toString(), true, false));// part of future SetTriple
						tripleContainer.setPredExpanded(formatString(predicate.toString(), true, false));
						tripleContainer.setObj2Expanded(formatString(finalObject.toString(), true, false));
						tripleContainer.setDocName(documentName);
						tripleContainer.setSentence(sentence);
						
						triples.add(tripleContainer);
					}
				}
			}
		}
	}

	/**
	 * Format the string before putting it in the TripleWrapper.  If the input is empty or null, the return is "NA".
	 * @param s			The String to format
	 * @param clean		Boolean if you want to clean the String by replacing unwanted characters
	 * @param toLower	Boolean if you want to make the String lowercase
	 * @return			The cleaned up version of the String
	 */
	private String formatString(String s, boolean clean, boolean toLower) {
		if(s == null || s.isEmpty()) {
			return "NA";
		}
		String retString = s;
		if(clean) {
			retString = s.replace("'", ",").replace("`", ",");
		}
		if(toLower) {
			retString = retString.toLowerCase();
		}
		return retString;
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

	//TODO: figure out how to put this in find triples
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
}
