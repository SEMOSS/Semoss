/**
 * 
 */
package prerna.algorithm.learning.matching;

import cern.colt.Arrays;
import edu.cmu.lti.lexical_db.ILexicalDatabase;
import edu.cmu.lti.lexical_db.NictWordNet;
import edu.cmu.lti.ws4j.RelatednessCalculator;
import edu.cmu.lti.ws4j.impl.WuPalmer;
import net.sf.extjwnl.JWNLException;
import net.sf.extjwnl.data.IndexWord;
import net.sf.extjwnl.data.POS;
import net.sf.extjwnl.dictionary.Dictionary;

/**
 * This class is used to perform semantic Java word net comparisons
 * 
 * @author https://github.com/jaytaylor/jaws
 *
 */
public class JawsSemanticMatching {
	private Dictionary dictionary;

	public JawsSemanticMatching() {
		try {
			this.dictionary = Dictionary.getDefaultResourceInstance();
		} catch (JWNLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * This method will generate the semantic score using WuPalmer.
	 * 
	 * @param item
	 * @param match
	 * @return 3 columns returned comparisonItem, comparisonMatch, score
	 */
	public String[] generateSemanticScore(Object[] item, Object[] match) {

		String[] columns = new String[3];
		Object[] cleanWord1 = new Object[item.length];
		Object[] cleanWord2 = new Object[item.length];

		Object[] score = new Object[item.length];

		for (int i = 0; i < item.length; i++) {
			String wordItem = (String) item[i];
			String wordMatch = (String) match[i];

			// clean camel case words for analysis
			wordItem = cleanWord(wordItem);
			wordMatch = cleanWord(wordMatch);

			// look up word in dictionary to standardize word is treated as a
			// noun i.e. systems = system
			IndexWord wordIndex1 = lookUpNoun(wordItem);
			IndexWord wordIndex2 = lookUpNoun(wordMatch);

			// if word is not found in dictionary treat as original word for
			// comparison or get base word for comparison
			if (wordIndex1 == null) {
				wordItem = (String) item[i];
			} else {
				// TODO do we need to do this?
				// System.out.println("***********************************
				// before lemma" + wordItem);
				wordItem = wordIndex1.getLemma();
			}

			if (wordIndex2 == null) {
				wordMatch = (String) match[i];
			} else {
				// System.out.println("***********************************
				// before lemma" + wordMatch);

				wordMatch = wordIndex2.getLemma();
			}
			cleanWord1[i] = "\"" + wordItem + "\"";
			cleanWord2[i] = "\"" + wordMatch + "\"";
			double wuScore = calculateWuPalmer(wordItem, wordMatch);
			if (wuScore > 1) {
				wuScore = 1;
			}
			score[i] = wuScore;
		}

		columns[0] = rStringColumn(cleanWord1);
		columns[1] = rStringColumn(cleanWord2);
		columns[2] = rStringColumn(score);

		return columns;
	}

	private String rStringColumn(Object[] col) {
		String stringCol = Arrays.toString(col);
		stringCol = stringCol.replace("[", "(");
		stringCol = stringCol.replace("]", ")");
		System.out.println(stringCol);
		return stringCol;
	}

	private double calculateWuPalmer(String word1, String word2) {
		ILexicalDatabase db = new NictWordNet();
		RelatednessCalculator rc = new WuPalmer(db);
		double s = rc.calcRelatednessOfWords(word1, word2);
		return s;
	}

	private IndexWord lookUpNoun(String word) {
		IndexWord noun = null;
		try {
			word = cleanWord(word);
			noun = dictionary.lookupIndexWord(POS.NOUN, word);
		} catch (JWNLException e) {
			e.printStackTrace();
		}
		if (null == noun) {
			System.out.println("********************************************************* NOUN NOT FOUND " + word);

		}
		return noun;
	}

	/**
	 * Break up camel case words.
	 * 
	 * @param word
	 * @return
	 */
	private String cleanWord(String word) {
		if (word.equals("System")) {
			System.out.println("debug!!!");
		}
		String cleanWord = "";
		for (String w : word.split("(?<!(^|[A-Z]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z])")) {
			cleanWord += w + "_";
		}
		cleanWord = cleanWord.substring(0, cleanWord.length() - 1);

		return cleanWord;
	}
}
