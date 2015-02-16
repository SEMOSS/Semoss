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

import java.util.ArrayList;
public class LetterPairSimilarity {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String one = "FirstNamePatient";
		String two = "PatientFirstName";
		System.out.println(compareStrings(one,two));
	}
	
	/** @return an array of adjacent letter pairs contained in the input string */
	   private static String[] letterPairs(String str) {
		//   System.out.println("letterPairs terms "+ str);
	       int numPairs = str.length()-1;
	       if(numPairs<0)
	    	   numPairs = 0;
	       String[] pairs = new String[numPairs];
	       for (int i=0; i<numPairs; i++) {
	           pairs[i] = str.substring(i,i+2);
	       }
	       return pairs;
	   }
	   
	   /** @return an ArrayList of 2-character Strings. */
	   private static ArrayList wordLetterPairs(String str) {
	       ArrayList allPairs = new ArrayList();
	       // Tokenize the string and put the tokens/words into an array
	       String[] words = str.split("\\s");
	       // For each word
	       for (int w=0; w < words.length; w++) {
	           // Find the pairs of characters
	           String[] pairsInWord = letterPairs(words[w]);
	           for (int p=0; p < pairsInWord.length; p++) {
	               allPairs.add(pairsInWord[p]);
	           }
	       }
	       return allPairs;
	   }
	   
	   /** @return lexical similarity value in the range [0,1] */
	   public static double compareStrings(String str1, String str2) {
	       ArrayList pairs1 = wordLetterPairs(str1.toUpperCase());
	       ArrayList pairs2 = wordLetterPairs(str2.toUpperCase());
	       int intersection = 0;
	       int union = pairs1.size() + pairs2.size();
	       for (int i=0; i<pairs1.size(); i++) {
	           Object pair1=pairs1.get(i);
	           for(int j=0; j<pairs2.size(); j++) {
	               Object pair2=pairs2.get(j);
	               if (pair1.equals(pair2)) {
	                   intersection++;
	                   pairs2.remove(j);
	                   break;
	               }
	           }
	       }
	       return (2.0*intersection)/union;
	   }
	

}
