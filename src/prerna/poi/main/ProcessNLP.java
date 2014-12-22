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

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Scanner;
import java.util.Vector;

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

	private List<TypedDependency> tdl = new ArrayList<TypedDependency>();
	private ArrayList <TaggedWord> TaggedWords = new ArrayList<TaggedWord>();
	private ArrayList <CoreLabel> CoreLabels = new ArrayList<CoreLabel>();
	private ArrayList <TripleWrapper> Triples = new ArrayList<TripleWrapper>(); // list of all triples stored after running findVerbTriples()
	private Hashtable <GrammaticalRelation, Vector<TypedDependency>> nodeHash = new Hashtable<GrammaticalRelation, Vector<TypedDependency>>();
	private Vector <TypedDependency> dobjV = new Vector<TypedDependency>();
	private Vector <TypedDependency> subjV = new Vector<TypedDependency>();
	private Hashtable <String, String> negHash = new Hashtable<String, String>();
	private boolean SentenceParsable = true;
	private LexicalizedParser lp;
	private StanfordCoreNLP pipeline;
	private int ArticleNUM = 0;
	private String CurrentSentence = "";

	private static final Logger logger = LogManager.getLogger(ProcessNLP.class.getName());
	
	public ProcessNLP(){
		lp = LexicalizedParser.loadModel(DIHelper.getInstance().getProperty("BaseFolder")+"\\NLPartifacts\\englishPCFG.ser");
		pipeline = new StanfordCoreNLP();
	}

	public ArrayList<TripleWrapper> masterRead(String[] files) throws NLPException {
		Triples = new ArrayList<TripleWrapper>();
		for(ArticleNUM = 0; ArticleNUM<files.length; ArticleNUM++){	
			String docin = files[ArticleNUM];
			NLP(docin);
		}

		TrimTriples();
		createOccuranceCount();
		lemmatize(Triples.size());
		normalizecase(Triples.size());
		return Triples;
	}

	private void NLP(String docin) throws NLPException {
		List<String> DocSentences = new ArrayList<String>();
		ReadDoc(DocSentences, docin);

		lp.setOptionFlags(new String[]{"-maxLength", "80", "-retainTmpSubcategories"});
		for(int i = 0; i < DocSentences.size(); i++) //DocSentences.size()
		{
			tdl = new ArrayList<TypedDependency>();
			TaggedWords = new ArrayList<TaggedWord>();
			CoreLabels = new ArrayList<CoreLabel>();
			nodeHash = new Hashtable<GrammaticalRelation, Vector<TypedDependency>>();
			dobjV = new Vector<TypedDependency>();
			subjV = new Vector<TypedDependency>();
			negHash = new Hashtable<String, String>();

			tdl = CreateDepList(DocSentences.get(i), tdl, TaggedWords); //create dependencies
			if(SentenceParsable == true)
			{
				CurrentSentence = DocSentences.get(i);
				setHash(tdl, nodeHash);
				GetTriples();
			}
		}
	}
	
	public void ReadDoc(List<String> DocSentences2, String docin) throws NLPException {
		//need to deal with return carriage!!!
		//logic for website, .docx .doc branch resume
		Scanner scan;
		TextExtractor textExtractor = new TextExtractor();
		String extractedText = "";
		
		if(docin.contains("http")){
			//source is website
			try {
				extractedText = textExtractor.WebsiteTextExtractor(docin);
			} catch (IOException e) {
				e.printStackTrace();
				throw new NLPException("Error processing website");
			}
			scan = new Scanner(extractedText);
			logger.info("Processing Website");
			int j = 0;
			scan.useDelimiter("\\. *\\s|\\? *\\s|\\! *\\s");
			while (scan.hasNext()){
				DocSentences2.add(scan.next()+".");
				DocSentences2.get(j).replaceAll("\\r\\n|\\r|\\n", " ").replace("\n","").replace("\r", "");
				logger.info(DocSentences2.get(j));
				j++;
			}
			scan.close();
		}
		if(docin.contains(".doc")){
			//source is a wordocument
			try {
				extractedText = textExtractor.WorddocTextExtractor(docin);
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
			scan = new Scanner(extractedText);
			int j = 0;
			scan.useDelimiter("\\. *\\s|\\? *\\s|\\! *\\s");
			while (scan.hasNext()){
				DocSentences2.add(scan.next()+".");
				DocSentences2.get(j).replaceAll("\\r\\n|\\r|\\n", " ").replace("\n","").replace("\r", "");
				logger.info(DocSentences2.get(j));
				j++;
			}
			scan.close();
		}
		if(docin.contains(".txt"))
		{
			try {
				extractedText = textExtractor.TextDocExtractor(docin);
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
			scan = new Scanner(extractedText);
			logger.info("Processing TextDocument");
			int j = 0;
			scan.useDelimiter("\\. *\\s|\\? *\\s|\\! *\\s|\\\n *\\s");
			String temp = "";
			while (scan.hasNext()){
				temp = scan.next();	
				DocSentences2.add(temp+".");
				DocSentences2.get(j).replaceAll("\\r\\n|\\r|\\n", " ").replace("\n","").replace("\r", "");
				logger.info("this is sentence "+j+" "+DocSentences2.get(j));
				j++;
			}
			scan.close();
		}
	}
	
	public List<TypedDependency> CreateDepList(String TheSentence, List<TypedDependency> tdl, List<TaggedWord> TaggedWords)
	{
		//picking the grammer sheet to use for parsing

		TokenizerFactory<CoreLabel> tokenizerFactory = PTBTokenizer.factory(new CoreLabelTokenFactory(), "");
		////This structures the sentence - needs sentence as an input and would return a list of typedependencies
		List<CoreLabel> rawWords = tokenizerFactory.getTokenizer(new StringReader(TheSentence)).tokenize();
		Tree bestParse = lp.parseTree(rawWords);

		Tree parse = bestParse; 
		try{
			TaggedWords.addAll(bestParse.taggedYield()); //gives each word with its POS
			SentenceParsable = true;
		}
		catch(NullPointerException e ){
			logger.info("This Sentence failed: "+ TheSentence);
			SentenceParsable = false;
			return tdl;
		}
		CoreLabels.addAll( bestParse.taggedLabeledYield());
		logger.info("From createDep: "+ TheSentence);
		logger.info("From createDep: "+ TaggedWords);

		GrammaticalStructure gs = null;
		TreebankLanguagePack tlp = new PennTreebankLanguagePack();
		GrammaticalStructureFactory gsf = tlp.grammaticalStructureFactory();
		gs = gsf.newGrammaticalStructure(parse);

		tdl = gs.typedDependenciesCCprocessed(); //@@choose which set of dependencies you want
		return tdl;
	}
	
	public Hashtable<GrammaticalRelation, Vector<TypedDependency>> setHash(List<TypedDependency> tdl,Hashtable <GrammaticalRelation, Vector<TypedDependency>> nodeHashA)
	{
		for(int tdlIndex = 0;tdlIndex < tdl.size();tdlIndex++)
		{
			TypedDependency one = tdl.get(tdlIndex);
			Vector <TypedDependency> baseVector = new Vector<TypedDependency>();
			GrammaticalRelation rel = one.reln();

			if(nodeHashA.containsKey(rel)) //if this type of relation already exists
				baseVector = nodeHashA.get(rel);
			baseVector.addElement(one);
			nodeHashA.put(rel, baseVector);
		}
		return nodeHashA;
	}

	public void GetTriples()
	{
		createNegations();
		findVerbTriples(EnglishGrammaticalRelations.NOMINAL_SUBJECT, EnglishGrammaticalRelations.DIRECT_OBJECT);
		findVerbTriples(EnglishGrammaticalRelations.AGENT, EnglishGrammaticalRelations.NOMINAL_PASSIVE_SUBJECT);
		findVerbTriples(EnglishGrammaticalRelations.CONTROLLING_SUBJECT, EnglishGrammaticalRelations.DIRECT_OBJECT);
		findVerbTriples(EnglishGrammaticalRelations.NOMINAL_PASSIVE_SUBJECT, EnglishGrammaticalRelations.PREPOSITIONAL_MODIFIER);
		// find expanded subjects and objects
		// runs but need to get it to return triples
		// findBaseSubjectTriples(EnglishGrammaticalRelations.NOMINAL_SUBJECT);
	}
	
	public void createNegations()
	{
		Vector <TypedDependency> negVector = nodeHash.get(EnglishGrammaticalRelations.NEGATION_MODIFIER);
		if(negVector != null)
		{
			// run through each of these to see if I find any negation
			for(int negIndex = 0;negIndex < negVector.size();negIndex++)
			{
				TypedDependency neg = negVector.elementAt(negIndex);
				String gov = neg.gov() + "" ;
				negHash.put(gov, gov);
			}
		}
	}

	public void findVerbTriples(GrammaticalRelation subjR, GrammaticalRelation objR)
	{
		// based on the subjects and objects now find the predicates
		dobjV = nodeHash.get(objR);
		subjV = nodeHash.get(subjR);

		if(dobjV != null && subjV != null)
		{
			for(int dobjIndex = 0;dobjIndex < dobjV.size();dobjIndex++)
			{
				TreeGraphNode obj = dobjV.get(dobjIndex).dep();
				TreeGraphNode pred = dobjV.get(dobjIndex).gov();
				String predicate = pred.value();
				//possibly add a clausal search for this as well

				// now find the subject
				for(int subjIndex = 0;subjIndex < subjV.size();subjIndex++)
				{
					TreeGraphNode subj = subjV.get(subjIndex).dep();
					TreeGraphNode dep2 = subjV.get(subjIndex).gov();
					if((dep2+"").equalsIgnoreCase(pred + "")) //Sam - !!!this is the comparison to determine if their is a chain
					{
						//CORE TRIPLES FOUND
						TripleWrapper temp = new TripleWrapper();
						String finalSubject  = subj.value();
						String finalObject = "";
						logger.info("VERB Triple simple: "+subj+" "+pred+" "+obj);
						//Setting TripleWrapper
						temp.setObj1(subj.toString()); //part of future SetTriple
						temp.setPred(pred.toString());
						temp.setObj2(obj.toString());
						temp.setObj1POS(temp.getObj1(),TaggedWords);
						temp.setPredPOS(temp.getPred(),TaggedWords);
						temp.setObj2POS(temp.getObj2(),TaggedWords);

						//FINDING EXTENSION OF SUBJECT****
						// find if complemented
						// need to do this only if the subj is not a noun
						// final subject
						TreeGraphNode altPredicate = findCompObject(dep2);
						if(!subj.label().tag().contains("NN") && ( nodeHash.containsKey(EnglishGrammaticalRelations.CLAUSAL_COMPLEMENT) || nodeHash.containsKey(EnglishGrammaticalRelations.XCLAUSAL_COMPLEMENT)))
						{
							subj = findComplementNoun(subj, dep2, EnglishGrammaticalRelations.CLAUSAL_COMPLEMENT);
							if(!subj.label().tag().contains("NN")){
								subj = findCompSubject(dep2);
							}
						}		
						finalObject = getFullNoun(obj);
						finalObject = finalObject + findPrepNounForPredicate(pred);
						finalSubject = getFullNoun(subj);

						//FINDING EXTENSION OF PREDICATE****
						// find the negators for the predicates next
						if(negHash.containsKey(pred + "")|| negHash.containsKey(altPredicate + "")) {
							predicate = "NOT " + predicate;
						}

						//EXTENSION OF OBJECT FOUND****
						// fulcrum on the nsubj to see if there is an NNP in the vicinity
						if(finalObject.indexOf(predicate) < 0 && predicate.indexOf(finalObject) < 0) {
							logger.info("VERB Triple: 	" + finalSubject + "<<>>" + predicate + "<<>>" + finalObject);
						}
						temp.setObj1exp(finalSubject.toString());// part of future SetTriple
						temp.setPredexp(predicate.toString());
						temp.setObj2exp(finalObject.toString());
						temp.setArticleNum(Integer.toString(ArticleNUM));
						temp.setSentence(CurrentSentence);
						Triples.add(temp);
						temp = new TripleWrapper();//be sure this line happens before you add another triple or they will point to the same thing
					}
				}
			}
		}
	}

	// finds the expanded object
	public TreeGraphNode findCompObject(TreeGraphNode subj)
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
	public TreeGraphNode findCompSubject(TreeGraphNode subj)
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
	public TreeGraphNode findComplementNoun(TreeGraphNode subj, TreeGraphNode dep2, GrammaticalRelation relation) {

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
				for(int labIndex = 0;labIndex < lw.size();labIndex++)
				{
					finalObject = finalObject + lw.get(labIndex).word() + " ";
				}
			}
		}
		return finalObject;
	}

	public String findPrepNoun(TreeGraphNode noun)
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
					retString = retString + " " + tdl.reln().getSpecific() + " " + fullNoun + findPrepNoun(dep);
				else
					retString = retString + " " + tdl.reln().getSpecific() + " " + fullNoun + findPrepNoun(dep).replace(dep.value(), "");
			}
		}
		return retString;
	}

	public String findPrepNounForPredicate(TreeGraphNode noun)
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
					retString = retString + " " + tdl.reln().getSpecific() + " " + fullNoun + findPrepNoun(dep);
				else
					retString = retString + " " + tdl.reln().getSpecific() + " " + fullNoun;
			}
		}
		return retString;
	}
	
	private void TrimTriples() {
		for(int i = 0; i<Triples.size(); i++)
		{
			logger.info(Triples.get(i).getObj1());
			Triples.get(i).setObj1(Triples.get(i).getObj1().toString().substring(0, Triples.get(i).getObj1().toString().indexOf('-')));
			Triples.get(i).setPred(Triples.get(i).getPred().toString().substring(0, Triples.get(i).getPred().toString().indexOf('-')));
			Triples.get(i).setObj2(Triples.get(i).getObj2().toString().substring(0, Triples.get(i).getObj2().toString().indexOf('-')));

			//set the expanded to NA if given no value
			if(Triples.get(i).getObj1exp().equals("")){
				Triples.get(i).setObj1exp("NA");
			}
			if(Triples.get(i).getPredexp().equals("")){
				Triples.get(i).setPredexp("NA");
			}
			if(Triples.get(i).getObj2exp().equals("")){
				Triples.get(i).setObj2exp("NA");
			}
			Triples.get(i).setObj1exp(Triples.get(i).getObj1exp().toString().replace("'", ","));
			Triples.get(i).setObj1exp(Triples.get(i).getObj1exp().toString().replace("`", ","));
			Triples.get(i).setPredexp(Triples.get(i).getPredexp().toString().replace("'", ","));
			Triples.get(i).setPredexp(Triples.get(i).getPredexp().toString().replace("`", ","));
			Triples.get(i).setObj2exp(Triples.get(i).getObj2exp().toString().replace("'", ","));
			Triples.get(i).setObj2exp(Triples.get(i).getObj2exp().toString().replace("`", ","));
		}
	}
	
	private void createOccuranceCount() {
		logger.info("COUNT TABLE PRINTED ");

		ArrayList <String> term = new ArrayList<String>();
		ArrayList <Integer> termcount = new ArrayList<Integer>();
		int indexofcount = 0;
		for(int i = 0; i<Triples.size(); i++){

			if(term.contains(Triples.get(i).getObj1())){
				indexofcount = term.indexOf(Triples.get(i).getObj1());
				termcount.set(indexofcount, termcount.get(indexofcount)+1);}
			else{
				term.add(Triples.get(i).getObj1());
				termcount.add(1);}
			if(term.contains(Triples.get(i).getPred())){
				indexofcount = term.indexOf(Triples.get(i).getPred());
				termcount.set(indexofcount, termcount.get(indexofcount)+1);}
			else{
				term.add(Triples.get(i).getPred());
				termcount.add(1);}
			if(term.contains(Triples.get(i).getObj2())){
				indexofcount = term.indexOf(Triples.get(i).getObj2());
				termcount.set(indexofcount, termcount.get(indexofcount)+1);}
			else{
				term.add(Triples.get(i).getObj2());
				termcount.add(1);}
		}

		logger.info("COUNT TABLE PRINTED "+ term);
		logger.info("COUNT TABLE PRINTED "+ termcount);

		for(int i = 0; i<Triples.size(); i++){
			if(term.contains(Triples.get(i).getObj1())){
				indexofcount = term.indexOf(Triples.get(i).getObj1());
				Triples.get(i).setObj1num(termcount.get(indexofcount));   
			}
			if(term.contains(Triples.get(i).getPred())){
				indexofcount = term.indexOf(Triples.get(i).getPred());
				Triples.get(i).setPrednum(termcount.get(indexofcount));   
			}
			if(term.contains(Triples.get(i).getObj2())){
				indexofcount = term.indexOf(Triples.get(i).getObj2());
				Triples.get(i).setObj2num(termcount.get(indexofcount));   
			}
		}

		logger.info("TriplesNum");
		for(int i = 0; i <Triples.size();i++){
			logger.info(Triples.get(i).getObj1num());
			logger.info(Triples.get(i).getPrednum());
			logger.info(Triples.get(i).getObj2num());
		}
	}

	public void lemmatize(int TripleCount)
	{
		for(int i = 0; i<TripleCount; i++){
			Annotation document = new Annotation(Triples.get(i).getPred());
			pipeline.annotate(document);
			List<CoreMap> sentences = document.get(SentencesAnnotation.class);
			for(CoreMap sentence: sentences) {
				for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
					logger.info("lemmatized "+token.get(LemmaAnnotation.class));
					logger.info("original   "+Triples.get(i).getPred()); 
					Triples.get(i).setPred(token.get(LemmaAnnotation.class));
				}
			}
		}
	}
	
	private void normalizecase(int size) {
		for(int i = 0; i<Triples.size(); i++){
			Triples.get(i).setObj1(Triples.get(i).getObj1().toLowerCase());
			Triples.get(i).setPred(Triples.get(i).getPred().toLowerCase());
			Triples.get(i).setObj2(Triples.get(i).getObj2().toLowerCase());
		}
	}

}
