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
package prerna.nameserver;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;

import prerna.algorithm.nlp.NaturalLanguageProcessingHelper;
import prerna.algorithm.nlp.TextHelper;
import prerna.util.Utility;
import rita.RiWordNet;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import edu.stanford.nlp.trees.EnglishGrammaticalRelations;
import edu.stanford.nlp.trees.GrammaticalRelation;
import edu.stanford.nlp.trees.TypedDependency;

public class WordnetComparison implements IMasterDatabaseConstants{

	private RiWordNet wordnet;
	private LexicalizedParser lp;

	public WordnetComparison() {
		
	}
	
	public void setWordnet(RiWordNet wordnet) {
		this.wordnet = wordnet;
	}
	
	public void setLp(LexicalizedParser lp) {
		this.lp = lp;
	}
	
	/**
	 * Constructor for the class
	 * Defines the wordnet library
	 */
	public WordnetComparison(String wordNetDir, String lpDir) {
		wordnet = new RiWordNet(wordNetDir, false, true); // params: wordnetInstallDir, ignoreCompoundWords, ignoreUppercaseWords
		lp = LexicalizedParser.loadModel(lpDir);
		lp.setOptionFlags(new String[]{"-maxLength", "80", "-retainTmpSubcategories"});
	}

	
	/**
	 * Determines if two words are similar based on their noun list
	 * @param keyword1		The Set of nouns in the first word
	 * @param keyword2	The Set of nouns in the second word
	 * @return					Boolean true if the two words are similar based on the threshold
	 */
	public boolean isSimilar(String keyword1, String keyword2) {
		double comparissonVal = compareKeywords(keyword1, keyword2);
		return isSimilar(comparissonVal);
	}
	
	/**
	 * Determines if the distance score is within the threshold to consider two words similar
	 * @param comparissonVal	The value of the distance between the words
	 * @return					Boolean true if the two words are similar based on the threshold
	 */
	public boolean isSimilar(double comparissonVal) {
		if(comparissonVal <= similarityCutOff) {
			return true;
		}
		return false;
	}
	
	/**
	 * Runs the comparison algorithm to determine the distance between two words
	 * @param firstNounList			The Set of nouns in the first word
	 * @param secondNounList		The Set of nouns in the second word
	 * @return						Double containing the distance between the two words
	 */
	public double compareKeywords(String keyword1, String keyword2) {
		System.err.println(">>>>>>>>>>>>>>>>>COMPARING: " + keyword1 + " to " + keyword2);
		// need to iterate through and break apart the URIs
		String keywordName1 = TextHelper.formatCompountText(Utility.getInstanceName(keyword1));
		String keywordName2 = TextHelper.formatCompountText(Utility.getInstanceName(keyword2));

		Set<String> firstMainNouns = getMainNouns(keywordName1);
		Set<String> secondMainNouns = getMainNouns(keywordName2);
		System.err.println(">>>>>>>>>>>>>>>>>FOUND MAIN NOUNS IN SET 1: " + firstMainNouns);
		System.err.println(">>>>>>>>>>>>>>>>>FOUND MAIN NOUNS IN SET 2: " + secondMainNouns);

		Set<String> firstOtherNouns = new HashSet<String>();
		Set<String> secondOtherNouns = new HashSet<String>();
		
		String[] nouns = TextHelper.splitRemovingEmptyValuesAndNulls(keywordName1);
		for(String noun : nouns) {
			if(!firstMainNouns.contains(noun)) {
				firstOtherNouns.add(noun);
			}
		}
		
		nouns = TextHelper.splitRemovingEmptyValuesAndNulls(keywordName2);
		for(String noun : nouns) {
			if(!secondMainNouns.contains(noun)) {
				secondOtherNouns.add(noun);
			}
		}
		
		double[][] mainComparisonMatrix = getComparisonMatrix(firstMainNouns, secondMainNouns);
		double[][] otherComparisonMatrix = getComparisonMatrix(firstOtherNouns, secondOtherNouns);

		double mainCompVal = calculateBestComparison(mainComparisonMatrix) * mainNounWeight;
		double otherCompVal = calculateBestComparison(otherComparisonMatrix) * otherNounWeight;
		
		System.err.println(">>>>>>>>>>>>>>>>>COMPARING: " + firstMainNouns + " to " + secondMainNouns + " gives value = " + mainCompVal);
		System.err.println(">>>>>>>>>>>>>>>>>COMPARING: " + firstOtherNouns + " to " + secondOtherNouns + " gives value = " + otherCompVal);

		return mainCompVal + otherCompVal;
	}
	
	/**
	 * Determines the main noun by concatenating the nouns and using NLP to find the gov/dep depending on the grammatical relationship found
	 * @param sentence		The String to get the main nouns
	 * @return				The Set containing the list of main nouns
	 */
	private Set<String> getMainNouns(String sentence) {
		// get the grammatical relationships in the sentence
		Hashtable<GrammaticalRelation, Vector<TypedDependency>> nodeHash = NaturalLanguageProcessingHelper.getTypeDependencyHash(lp, sentence);

		Vector<TypedDependency> tdlArr = nodeHash.get(EnglishGrammaticalRelations.NOUN_COMPOUND_MODIFIER);
		if(tdlArr != null) {
			return getTopGov(tdlArr);
		}
		
		// if NLP determines something isn't a noun, look for adj/adv
		tdlArr = nodeHash.get(EnglishGrammaticalRelations.ADJECTIVAL_MODIFIER);
		if(tdlArr != null) {
			return getTopGov(tdlArr);
		}
		
		tdlArr = nodeHash.get(EnglishGrammaticalRelations.ADVERBIAL_MODIFIER);
		if(tdlArr != null) {
			return getTopGov(tdlArr);
		}
		
		tdlArr = nodeHash.get(EnglishGrammaticalRelations.NP_ADVERBIAL_MODIFIER);
		if(tdlArr != null) {
			return getTopDep(tdlArr);
		}
		
		tdlArr = nodeHash.get(EnglishGrammaticalRelations.NOMINAL_SUBJECT);
		if(tdlArr != null) {
			return getTopDep(tdlArr);
		}
		
		tdlArr = nodeHash.get(EnglishGrammaticalRelations.DIRECT_OBJECT);
		if(tdlArr != null) {
			return getTopDep(tdlArr);
		}
		
		tdlArr = nodeHash.get(EnglishGrammaticalRelations.NUMERIC_MODIFIER);
		if(tdlArr != null) {
			return getTopGov(tdlArr);
		}
		
		tdlArr = nodeHash.get(GrammaticalRelation.DEPENDENT);
		if(tdlArr != null) {
			return getTopDep(tdlArr);
		}
		
		tdlArr = nodeHash.get(GrammaticalRelation.ROOT);
		if(tdlArr != null) {
			return getTopDep(tdlArr);
		}
		
		return null;
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
	 * Determines and outputs the best distance pairing between nouns 
	 * @param comparisonMatrix		The double[][] of distance values between all nouns
	 * @return						The double containing the best matching between nouns
	 */
	private double calculateBestComparison(double[][] comparisonMatrix) {
		int size = comparisonMatrix.length;
		
		if(size == 0) {
			return 0;
		}
		
		double bestCombination = 1.1;
		
		int counter = 0;
		for(; counter < size; counter++) {
			int i = counter;
			double newCombination = 0;
			Set<Integer> usedIndices = new HashSet<Integer>();
			for(;i < size; i++) {
				double currComp = 1.1;
				int indexToRemove = 0;
				int j = 0;
				for(; j < size; j++) {
					double newComp = comparisonMatrix[i][j];
					if(!usedIndices.contains(j) && newComp < currComp) {
						currComp = newComp;
						indexToRemove = j;
					} 
				}
				usedIndices.add(indexToRemove);
				newCombination += currComp;
			}
			if(bestCombination > newCombination) {
				bestCombination = newCombination;
			}
		}
		
		return bestCombination;
	}
	
	/**
	 * Constructs a matrix with the distance between all nouns in one list to the nouns in another list
	 * If the two sets are not the same size, the default distance value is 0.5
	 * @param nounList1		The first Set of nouns
	 * @param nounList2		The second Set of nouns
	 * @return				The double[][] of distance values between all nouns
	 */
	private double[][] getComparisonMatrix(Set<String> nounList1, Set<String> nounList2) {
		
		int sizeNounList1 = nounList1.size();
		int sizeNounList2 = nounList2.size();
		int maxSize = Math.max(sizeNounList1, sizeNounList2);
		double[][] comparisonMatrix = new double[maxSize][maxSize];
		
		int i = 0;
		Iterator<String> iterator1 = nounList1.iterator();
		for(; i < maxSize; i++) {
			int j = 0;
			Iterator<String> iterator2 = nounList2.iterator();
			for(; j < maxSize; j++) {
				if(iterator1.hasNext()) {
					String noun1 = iterator1.next();
					if(iterator2.hasNext()) {
						String noun2 = iterator2.next();
						comparisonMatrix[i][j] = wordnet.getDistance(noun1.toLowerCase(), noun2.toLowerCase(), "n");
					} else {
						comparisonMatrix[i][j] = 0.5;
					}
				} else {
					if(iterator2.hasNext()) {
						iterator2.next();
						comparisonMatrix[i][j] = 0.5;
					} else {
						comparisonMatrix[i][j] = 0.5;
					}
				}
			}
		}
		
		return comparisonMatrix;
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
//	/**
//	 * Determines if two words are similar based on their noun list
//	 * @param firstNounList		The Set of nouns in the first word
//	 * @param secondNounList	The Set of nouns in the second word
//	 * @return					Boolean true if the two words are similar based on the threshold
//	 */
//	public boolean isSimilar(Set<String> firstNounList, Set<String> secondNounList) {
//		double comparissonVal = compareKeywords(firstNounList, secondNounList);
//		return isSimilar(comparissonVal);
//	}
//	
//	/**
//	 * Runs the comparison algorithm to determine the distance between two words
//	 * @param firstNounList			The Set of nouns in the first word
//	 * @param secondNounList		The Set of nouns in the second word
//	 * @return						Double containing the distance between the two words
//	 */
//	public double compareKeywords(Set<String> firstNounList, Set<String> secondNounList) {
//		System.err.println(">>>>>>>>>>>>>>>>>COMPARING: " + firstNounList + " to " + secondNounList);
//		// need to iterate through and break apart the URIs
//		firstNounList = getInstanceForSet(firstNounList);
//		secondNounList = getInstanceForSet(secondNounList);
//		System.err.println(">>>>>>>>>>>>>>>>> " + firstNounList);
//		System.err.println(">>>>>>>>>>>>>>>>> " + secondNounList);
//		
//		Set<String> firstMainNouns = getMainNouns(firstNounList);
//		Set<String> secondMainNouns = getMainNouns(secondNounList);
//		System.err.println(">>>>>>>>>>>>>>>>>FOUND MAIN NOUNS IN SET 1: " + firstMainNouns);
//		System.err.println(">>>>>>>>>>>>>>>>>FOUND MAIN NOUNS IN SET 2: " + secondMainNouns);
//
//		Set<String> firstOtherNouns = new HashSet<String>();
//		Set<String> secondOtherNouns = new HashSet<String>();
//
//		Iterator<String> iterator = firstNounList.iterator();
//		while(iterator.hasNext()) {
//			String val = iterator.next();
//			if(!firstMainNouns.contains(val)) {
//				firstOtherNouns.add(val);
//			}
//		}
//		
//		iterator = secondNounList.iterator();
//		while(iterator.hasNext()) {
//			String val = iterator.next();
//			if(!secondMainNouns.contains(val)) {
//				secondOtherNouns.add(val);
//			}
//		}
//		
//		double[][] mainComparisonMatrix = getComparisonMatrix(firstMainNouns, secondMainNouns);
//		double[][] otherComparisonMatrix = getComparisonMatrix(firstOtherNouns, secondOtherNouns);
//
//		double mainCompVal = calculateBestComparison(mainComparisonMatrix) * mainNounWeight;
//		double otherCompVal = calculateBestComparison(otherComparisonMatrix) * otherNounWeight;
//		
//		System.err.println(">>>>>>>>>>>>>>>>>COMPARING: " + firstMainNouns + " to " + secondMainNouns + " gives value = " + mainCompVal);
//		System.err.println(">>>>>>>>>>>>>>>>>COMPARING: " + firstOtherNouns + " to " + secondOtherNouns + " gives value = " + otherCompVal);
//
//		return mainCompVal + otherCompVal;
//	}
//	
//
//	/**
//	 * Determines the main noun by concatenating the nouns and using NLP to find the gov/dep depending on the grammatical relationship found
//	 * @param nounList		The Set containing the list of all nouns
//	 * @return				The Set containing the list of main nouns
//	 */
//	private Set<String> getMainNouns(Set<String> nounList) {
//		if(nounList.size() == 1) {
//			return nounList;
//		}
//		
//		String sentence = "";
//		Iterator<String> nounIt = nounList.iterator();
//		while(nounIt.hasNext()) {
//			sentence = sentence.concat(nounIt.next()).concat(" ");
//		}
//		sentence = sentence.trim();
//		
//		// get the grammatical relationships in the sentence
//		Hashtable<GrammaticalRelation, Vector<TypedDependency>> nodeHash = NaturalLanguageProcessingHelper.getTypeDependencyHash(lp, sentence);
//
//		Vector<TypedDependency> tdlArr = nodeHash.get(EnglishGrammaticalRelations.NOUN_COMPOUND_MODIFIER);
//		if(tdlArr != null) {
//			return getTopGov(tdlArr);
//		}
//		
//		// if NLP determines something isn't a noun, look for adj/adv
//		tdlArr = nodeHash.get(EnglishGrammaticalRelations.ADJECTIVAL_MODIFIER);
//		if(tdlArr != null) {
//			return getTopGov(tdlArr);
//		}
//		
//		tdlArr = nodeHash.get(EnglishGrammaticalRelations.ADVERBIAL_MODIFIER);
//		if(tdlArr != null) {
//			return getTopGov(tdlArr);
//		}
//		
//		tdlArr = nodeHash.get(EnglishGrammaticalRelations.NP_ADVERBIAL_MODIFIER);
//		if(tdlArr != null) {
//			return getTopDep(tdlArr);
//		}
//		
//		tdlArr = nodeHash.get(EnglishGrammaticalRelations.NOMINAL_SUBJECT);
//		if(tdlArr != null) {
//			return getTopDep(tdlArr);
//		}
//		
//		tdlArr = nodeHash.get(EnglishGrammaticalRelations.DIRECT_OBJECT);
//		if(tdlArr != null) {
//			return getTopDep(tdlArr);
//		}
//		
//		tdlArr = nodeHash.get(EnglishGrammaticalRelations.NUMERIC_MODIFIER);
//		if(tdlArr != null) {
//			return getTopGov(tdlArr);
//		}
//		
//		tdlArr = nodeHash.get(GrammaticalRelation.DEPENDENT);
//		if(tdlArr != null) {
//			return getTopDep(tdlArr);
//		}
//		
//		return null;
//	}
//	
//	
//	/**
//	 * Gets the instance name from a Set of URI's
//	 * @param uriList		The Set containing the URI's to get the instances
//	 * @return				Set containing the instance values of all the URI's 
//	 */
//	private Set<String> getInstanceForSet(Set<String> uriList) {
//		Set<String> retSet = new HashSet<String>();
//		Iterator<String> it = uriList.iterator();
//		while(it.hasNext()) {
//			String uri = it.next();
//			String instance = Utility.getInstanceName(uri);
//			retSet.add(instance);
//		}
//		
//		return retSet;
//	}
	
}
