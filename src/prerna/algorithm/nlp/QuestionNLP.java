/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.algorithm.nlp;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Scanner;
import java.util.Vector;

import prerna.util.Constants;
import prerna.util.DIHelper;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.LabeledWord;
import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.ling.WordLemmaTag;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
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


public class QuestionNLP {
	static LexicalizedParser lp;
	static StanfordCoreNLP pipeline;
	
	static List<TypedDependency> tdl = new ArrayList<TypedDependency>();
	static List<TypedDependency> tdl2 = new ArrayList<TypedDependency>();
	static ArrayList <TaggedWord> TaggedWords = new ArrayList<TaggedWord>();
	static ArrayList <CoreLabel> CoreLabels = new ArrayList<CoreLabel>();
	static ArrayList <WordLemmaTag> WordLemmas = new ArrayList<WordLemmaTag>();
	//static ArrayList <TripleWrapper> Triples = new ArrayList<TripleWrapper>(); // list of all triples stored after running findVerbTriples()
	static Hashtable <GrammaticalRelation, Vector<TypedDependency>> nodeHash = new Hashtable<GrammaticalRelation, Vector<TypedDependency>>();
	static Vector <TypedDependency> dobjV = new Vector<TypedDependency>();
	static Vector <TypedDependency> subjV = new Vector<TypedDependency>();
	static Hashtable <String, String> negHash = new Hashtable<String, String>();
	static Hashtable<String, Vector<String>> VNhash = new Hashtable<String,Vector<String>>();
	static ArrayList<String> Noun_POS = new ArrayList<String>();
	Hashtable<String, Vector<String>> VNFNhash = new Hashtable<String,Vector<String>>();
//	static List<RelationSheet> excelfiller = new ArrayList<RelationSheet>();
	static boolean SentenceParsable = true;

	public QuestionNLP(){
		final String baseDirectory = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		final String fileSeparator = System.getProperty("file.separator");
		String file =  baseDirectory + fileSeparator + "NLPartifacts" + fileSeparator + "englishPCFG.ser";
		lp = LexicalizedParser.loadModel(file);
		pipeline = new StanfordCoreNLP();
		Noun_POS.add("NN");
		Noun_POS.add("NNS");
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String Question = "Show all Systems that have data objects.";
		//class used to load NLP model and ask question
		QuestionNLP DBRec = new QuestionNLP();
		Scanner s = new Scanner(System.in);
		while(true){
		//two different parsings of the question provided Question_Analyzer and Question_Analyzer2
		System.out.println("QUESTION ANALYZER1: Returns extended subject and objects of sentence: ");
		ArrayList<String[]> relationshipList = DBRec.Question_Analyzer(Question);
		for(String[] relation : relationshipList) {
			System.out.println("Subject: " + relation[0] + " Object: " + relation[1]);
		}
		System.out.println("QUESTION ANALYZER2: Returns all nouns in the sentence: " +(DBRec.Question_Analyzer2(Question)).toString());
		System.out.println("Enter another question...");
		//type a new question or statement in the console
		Question = s.nextLine();
		}
		
	}
	
	private static List<TypedDependency> CreateDepList(String TheSentence, List<TypedDependency> tdl, List<TaggedWord> TaggedWords)
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
			System.out.println("This Sentence failed: "+ TheSentence);
			SentenceParsable = false;
			return tdl;
		}
		CoreLabels.addAll( bestParse.taggedLabeledYield());
		System.out.println("From createDep: "+ TheSentence);
		System.out.println("From createDep: "+ TaggedWords);

		GrammaticalStructure gs = null;
		TreebankLanguagePack tlp = new PennTreebankLanguagePack();
		GrammaticalStructureFactory gsf = tlp.grammaticalStructureFactory();
		gs = gsf.newGrammaticalStructure(parse);

		tdl = gs.typedDependenciesCCprocessed(); //@@choose which set of dependencies you want
		return tdl;
	}
	
	
	public ArrayList<String> Question_Analyzer2(String Question_Sentence){
		ArrayList<String> DBterms = new ArrayList<String>();
		tdl = new ArrayList<TypedDependency>();
		tdl2 = new ArrayList<TypedDependency>();;
		TaggedWords = new ArrayList<TaggedWord>();
		CoreLabels = new ArrayList<CoreLabel>();
		WordLemmas = new ArrayList<WordLemmaTag>();
		nodeHash = new Hashtable<GrammaticalRelation, Vector<TypedDependency>>();
		dobjV = new Vector<TypedDependency>();
		subjV = new Vector<TypedDependency>();
		negHash = new Hashtable<String, String>();


		tdl = CreateDepList(Question_Sentence, tdl, TaggedWords); //create dependencies
		System.out.println(TaggedWords);
		for(int i = 0; i<TaggedWords.size(); i++){
			for(int j = 0; j<Noun_POS.size();j++){
				if(TaggedWords.get(i).toString().substring(TaggedWords.get(i).toString().indexOf('/')+1).equals(Noun_POS.get(j) )){
					DBterms.add(TaggedWords.get(i).toString().substring(0,   TaggedWords.get(i).toString().indexOf('/') ));
				}
			}
		}
		return DBterms;
		
	}
	
	public ArrayList<String[]> Question_Analyzer(String Question_Sentence){
		ArrayList<String[]> phraselist = new ArrayList<String[]>();
		tdl = new ArrayList<TypedDependency>();
		tdl2 = new ArrayList<TypedDependency>();;
		TaggedWords = new ArrayList<TaggedWord>();
		CoreLabels = new ArrayList<CoreLabel>();
		WordLemmas = new ArrayList<WordLemmaTag>();
		nodeHash = new Hashtable<GrammaticalRelation, Vector<TypedDependency>>();
		dobjV = new Vector<TypedDependency>();
		subjV = new Vector<TypedDependency>();
		negHash = new Hashtable<String, String>();


		tdl = CreateDepList(Question_Sentence, tdl, TaggedWords); //create dependencies
		if(SentenceParsable == true)
		{
			setHash(tdl, nodeHash);
		}
		//extract key nouns
		phraselist.addAll(findVerbTriples(EnglishGrammaticalRelations.NOMINAL_SUBJECT, EnglishGrammaticalRelations.DIRECT_OBJECT));
		phraselist.addAll(findVerbTriples(EnglishGrammaticalRelations.AGENT, EnglishGrammaticalRelations.NOMINAL_PASSIVE_SUBJECT));
		phraselist.addAll(findVerbTriples(EnglishGrammaticalRelations.CONTROLLING_SUBJECT, EnglishGrammaticalRelations.DIRECT_OBJECT));
		phraselist.addAll(findVerbTriples(EnglishGrammaticalRelations.NOMINAL_PASSIVE_SUBJECT, EnglishGrammaticalRelations.PREPOSITIONAL_MODIFIER));
		return phraselist;
}
	
	private static Hashtable<GrammaticalRelation, Vector<TypedDependency>> setHash(List<TypedDependency> tdl,Hashtable <GrammaticalRelation, Vector<TypedDependency>> nodeHashA)
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
	
	private static ArrayList<String[]> findVerbTriples(GrammaticalRelation subjR, GrammaticalRelation objR)
	{
		ArrayList<String[]> NounPhrases = new ArrayList<String[]>();
		//ArrayList<String> NounPhrases = new ArrayList<String>();
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
				String object = obj + "";
				//possibly add a clausal search for this as well

				// now find the subject
				//System.out.println("Node in relation " + GrammaticalStructure.getNodeInRelation(obj, EnglishGrammaticalRelations.PREDICATE));
				for(int subjIndex = 0;subjIndex < subjV.size();subjIndex++)
				{
					TreeGraphNode subj = subjV.get(subjIndex).dep();
					TreeGraphNode dep2 = subjV.get(subjIndex).gov();
					if((dep2+"").equalsIgnoreCase(pred + "")) //this is the comparison to determine if their is a chain
					{
						//CORE TRIPLES FOUND
						
						String finalSubject  = subj.value();
						String finalObject = "";
						System.out.println("VERB Triple simple: "+subj+" "+pred+" "+obj);
						//Setting TripleWrapper
				/*		temp.setObj1(subj.toString()); //part of future SetTriple
						temp.setPred(pred.toString());
						temp.setObj2(obj.toString());
						temp.setObj1POS(temp.getObj1(),TaggedWords);
						temp.setPredPOS(temp.getPred(),TaggedWords);
						temp.setObj2POS(temp.getObj2(),TaggedWords);
				*/
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
						if(negHash.containsKey(pred + "")|| negHash.containsKey(altPredicate + "")){
							predicate = "NOT " + predicate;
						}

						//EXTENSION OF OBJECT FOUND****
						// fulcrum on the nsubj to see if there is an NNP in the vicinity
						if(finalObject.indexOf(predicate) < 0 && predicate.indexOf(finalObject) < 0){
							System.out.println("VERB Triple: 	" + finalSubject + "<<>>" + predicate + "<<>>" + finalObject);
							
							String [] phrase = new String[2];
							phrase[0] = finalSubject;
							phrase[1] = finalObject;
							NounPhrases.add(phrase);
						}
				/*		temp.setObj1exp(finalSubject.toString());// part of future SetTriple
						temp.setPredexp(predicate.toString());
						temp.setObj2exp(finalObject.toString());
						temp.setArticleNum(Integer.toString(ArticleNUM));
						temp.setSentence(CurrentSentence);
						Triples.add(temp);
						temp = new TripleWrapper();//be sure this line happens before you add another triple or they will point to the same thing
					*/
					}
				}
			}
		}
		return NounPhrases;
	}
	
	private static TreeGraphNode findComplementNoun(TreeGraphNode subj, TreeGraphNode dep2, GrammaticalRelation relation) {

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
	
	
	// finds the expanded object
	private static TreeGraphNode findCompObject(TreeGraphNode subj)
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
	
	private static String getFullNoun(TreeGraphNode node)
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
	
	private static TreeGraphNode findCompSubject(TreeGraphNode subj)
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
	
	
	private static String findPrepNounForPredicate(TreeGraphNode noun)
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
	
	private static String findPrepNoun(TreeGraphNode noun)
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
}
