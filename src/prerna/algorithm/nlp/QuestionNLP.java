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
package prerna.algorithm.nlp;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

import prerna.util.Constants;
import prerna.util.DIHelper;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
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
	static List<String> Noun_POS = new ArrayList<String>();
	static boolean SentenceParsable = true;

	public QuestionNLP(){
		final String baseDirectory = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		final String fileSeparator = System.getProperty("file.separator");
		String file =  baseDirectory + fileSeparator + "NLPartifacts" + fileSeparator + "englishPCFG.ser";
		lp = LexicalizedParser.loadModel(file);
		Noun_POS.add("NN");
		Noun_POS.add("NNS");
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
		System.out.println("From createDep: "+ TheSentence);
		System.out.println("From createDep: "+ TaggedWords);

		GrammaticalStructure gs = null;
		TreebankLanguagePack tlp = new PennTreebankLanguagePack();
		GrammaticalStructureFactory gsf = tlp.grammaticalStructureFactory();
		gs = gsf.newGrammaticalStructure(parse);

		tdl = gs.typedDependenciesCCprocessed(); //@@choose which set of dependencies you want
		return tdl;
	}


	public List<String> Question_Analyzer2(String Question_Sentence){
		List<String> DBterms = new ArrayList<String>();
		List<TypedDependency> tdl = new ArrayList<TypedDependency>();
		List<TaggedWord> TaggedWords = new ArrayList<TaggedWord>();

		tdl = CreateDepList(Question_Sentence, tdl, TaggedWords); //create dependencies
		System.out.println(TaggedWords);
		for(int i = 0; i<TaggedWords.size(); i++){
			for(int j = 0; j<Noun_POS.size();j++){
				if(TaggedWords.get(i).toString().substring(TaggedWords.get(i).toString().indexOf('/')+1).equals(Noun_POS.get(j) )){
					DBterms.add(TaggedWords.get(i).toString().substring(0, TaggedWords.get(i).toString().indexOf('/') ));
				}
			}
		}
		return DBterms;

	}

	public List<String[]> Question_Analyzer(String Question_Sentence){
		List<String[]> phraselist = new ArrayList<String[]>();
		List<TypedDependency> tdl = new ArrayList<TypedDependency>();
		List<TaggedWord> TaggedWords = new ArrayList<TaggedWord>();
		Hashtable<GrammaticalRelation, Vector<TypedDependency>> nodeHash = new Hashtable<GrammaticalRelation, Vector<TypedDependency>>();
		Hashtable<String, String> negHash = new Hashtable<String, String>();


		tdl = CreateDepList(Question_Sentence, tdl, TaggedWords); //create dependencies
		if(SentenceParsable == true)
		{
			PartOfSpeechHelper.setTypeDependencyHash(tdl, nodeHash);
		}
		//extract key nouns
		phraselist.addAll(findTriples(negHash, nodeHash, EnglishGrammaticalRelations.NOMINAL_SUBJECT, EnglishGrammaticalRelations.DIRECT_OBJECT));
		phraselist.addAll(findTriples(negHash, nodeHash, EnglishGrammaticalRelations.AGENT, EnglishGrammaticalRelations.NOMINAL_PASSIVE_SUBJECT));
		phraselist.addAll(findTriples(negHash, nodeHash, EnglishGrammaticalRelations.CONTROLLING_SUBJECT, EnglishGrammaticalRelations.DIRECT_OBJECT));
		phraselist.addAll(findTriples(negHash, nodeHash, EnglishGrammaticalRelations.NOMINAL_PASSIVE_SUBJECT, EnglishGrammaticalRelations.PREPOSITIONAL_MODIFIER));
		return phraselist;
	}


	private List<String[]> findTriples(Hashtable<String, String> negHash, Hashtable <GrammaticalRelation, Vector<TypedDependency>> nodeHash, GrammaticalRelation subjR, GrammaticalRelation objR)
	{
		ArrayList<String[]> NounPhrases = new ArrayList<String[]>();
		// based on the subjects and objects now find the predicates
		Vector<TypedDependency> dobjV = nodeHash.get(objR);
		Vector<TypedDependency> subjV = nodeHash.get(subjR);

		if(dobjV != null && subjV != null)
		{
			for(int dobjIndex = 0;dobjIndex < dobjV.size();dobjIndex++)
			{
				TreeGraphNode obj = dobjV.get(dobjIndex).dep();
				TreeGraphNode pred = dobjV.get(dobjIndex).gov();
				String predicate = pred.value();
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
						finalObject = PartOfSpeechHelper.getFullNoun(obj);
						finalObject = finalObject + PartOfSpeechHelper.findPrepNounForPredicate(pred, nodeHash);
						finalSubject = PartOfSpeechHelper.getFullNoun(subj);

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
					}
				}
			}
		}
		return NounPhrases;
	}
}
