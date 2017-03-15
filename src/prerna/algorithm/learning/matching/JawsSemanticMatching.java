/**
 * 
 */
package prerna.algorithm.learning.matching;

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
	 * @param objects
	 * @param objects2
	 * @return
	 */
	public double[] generateSemanticScore(Object[] objects, Object[] objects2) {
		double[] score = new double[objects.length];

		for (int i = 0; i < objects.length; i++) {
			String word1 = (String) objects[i];
			String word2 = (String) objects2[i];

			// clean camel case words for analysis
			word1 = cleanWord(word1);
			word2 = cleanWord(word2);

			// look up word in dictionary to standardize word is treated as a
			// noun i.e. systems = system
			IndexWord wordIndex1 = lookUpNoun(word1);
			IndexWord wordIndex2 = lookUpNoun(word2);

			// if word is not found in dictionary treat as original word for
			// comparison or get base word for comparison
			if (wordIndex1 == null) {
				word1 = (String) objects[i];
			} else {
				word1 = wordIndex1.getLemma();
			}

			if (wordIndex2 == null) {
				word2 = (String) objects2[i];
			} else {
				word2 = wordIndex2.getLemma();
			}

			score[i] = calculateWuPalmer(word1, word2);

		}
		return score;
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
