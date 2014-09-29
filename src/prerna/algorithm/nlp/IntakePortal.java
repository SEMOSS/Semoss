package prerna.algorithm.nlp;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;

import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.trees.GrammaticalRelation;
import edu.stanford.nlp.trees.TypedDependency;

public class IntakePortal {
	int MasterSize = 144;
	static StanfordNLPfuncts NLP = new StanfordNLPfuncts();
	private static final Logger logger = LogManager.getLogger(IntakePortal.class.getName());
	
	public static void main(String[] args) throws InvalidFormatException, IOException {
		IntakePortal test = new IntakePortal();
		//	System.out.println("Start");
	//	GroupSimilarityTest();
	//	System.out.println("End");	
		
	//	System.out.println("Start");
	//	NonLearnedMapping();
	//	System.out.println("End");
	
	//	System.out.println("Start");
	//	WordNetMapping();
	//	System.out.println("End");
		
	/*	System.out.println("Start1");
		Scanner s = new Scanner(System.in);
		String A = "SystemTasker";
		String B = "ApplicationTask";
		while(true){
		System.out.println("SCORE ~ " +DBRecSimScore(A, B) +"~"+ A+"~"+B);
		System.out.println("End");
		A = s.nextLine();
		B = s.nextLine();
		}
		*/
		
     	System.out.println("Start");
     	test.DBRecSimTester();
		System.out.println("End");
		
		
	}
	
	private void DBRecSimTester() throws InvalidFormatException, IOException{
		//write to text file
		FileWriter outFile;
		 outFile = new FileWriter("test.txt");
         PrintWriter out = new PrintWriter(outFile);
         
		//done writting to text file
		
		int rows = 3;
		double score2 = 0;
		IntakePortal test = new IntakePortal();
		ArrayList <String> SchemaElements1 = new ArrayList<String>();
		ArrayList <String> SchemaElements2 = new ArrayList<String>();
//		ExcelReadRelationship2(SchemaElements1, SchemaElements2, rows);
		WS4Jwrapper WordNetScorer = new WS4Jwrapper();
		for(int i = 0; i<SchemaElements1.size(); i++){
			for(int j=0; j<SchemaElements2.size(); j++){
			score2 = test.DBRecSimScore(SchemaElements1.get(i), SchemaElements2.get(j));
			System.out.println("SCORE "+"~"+ score2 + "~" + SchemaElements1.get(i) + "~"+SchemaElements2.get(j));
			out.println("SCORE "+"~"+ score2 + "~" + SchemaElements1.get(i).toString() + "~"+SchemaElements2.get(j).toString());
			}
			
		}
		out.close();
		
	}
	
	public static double[][] DBRecSimScore(ArrayList<String> A, ArrayList<String> B) {
		double[][] retScores = new double[A.size()][B.size()];
		for(int a=0;a<A.size();a++) {
			for(int b=0;b<B.size();b++) {
				retScores[a][b] = DBRecSimScore(A.get(a),B.get(b));
			}
		}
		return retScores;
	}
	
	public static double DBRecSimScore(String A, String B) {
		double MasterScore = 0;
		WS4Jwrapper WordNetScorer = new WS4Jwrapper();
		ArrayList<String> PhraseA = new ArrayList<String>();
		ArrayList<String> PhraseB = new ArrayList<String>();
		ArrayList<String> LPhraseA = new ArrayList<String>();
		ArrayList<String> LPhraseB = new ArrayList<String>();
		ArrayList<Double> Apriority = new ArrayList<Double>();
		ArrayList<Double> Bpriority = new ArrayList<Double>();
		ArrayList<Boolean> AIsWord = new ArrayList<Boolean>();
		ArrayList<Boolean> BIsWord = new ArrayList<Boolean>();
		//break_up camel case and underscore division
		A = termseperator(A);
		B = termseperator(B);
		//break single string into arraylist based on space
		PhraseA = word_Break(A);
		PhraseB = word_Break(B);
		//gives priority ranking to arraylist
		Apriority = NLP_prioritization(A, PhraseA);
		Bpriority = NLP_prioritization(B, PhraseB);
		//Lemmatize Terms
		LPhraseA = 	PreProcessLematize(PhraseA);
		LPhraseB =  PreProcessLematize(PhraseB);
		//Determine Word or Not Word
		AIsWord = IsWNWord(PhraseA);
		BIsWord = IsWNWord(PhraseB);
		//Create Similarity Score
		
		MasterScore = WordNetScorer.WN_NLP_Scorer(A, B, LPhraseA, LPhraseB, Apriority, Bpriority, AIsWord, BIsWord);
	    logger.info("Score for "+A+" and "+B+" is "+MasterScore);
	    return MasterScore;
		
		
	}

	private static ArrayList<Boolean> IsWNWord(ArrayList<String> phraseA) {
		WS4Jwrapper WNSimFunc = new WS4Jwrapper();
		double score = 0;
		ArrayList<Boolean> IsWord = new ArrayList<Boolean>();
		for(int i =0; i<phraseA.size(); i++){
			score = WNSimFunc.run2(phraseA.get(i).trim(), "entity");
			if(score == 0)
				IsWord.add(false);
			else
				IsWord.add(true);
		}
		return IsWord;
	}

	private static ArrayList<String> word_Break(String a) {
		ArrayList<String> words1 = new ArrayList<String>();
        
    	StringTokenizer word1tok = new StringTokenizer(a);
    	
    	while(word1tok.hasMoreElements()){
    		words1.add(word1tok.nextElement().toString().trim());
    	}
    	return words1;
		}

	private static ArrayList<Double> NLP_prioritization(String a, ArrayList<String> phraseA) {
		
		List<TypedDependency> tdl = new ArrayList<TypedDependency>();
		Hashtable <GrammaticalRelation, Vector<TypedDependency>> nodeHash = new Hashtable<GrammaticalRelation, Vector<TypedDependency>>();
		ArrayList <TaggedWord> TaggedWords = new ArrayList<TaggedWord>();
		ArrayList <Double> TermP = new ArrayList<Double>();
		//find NLP dependencies
		tdl = NLP.CreateDepList(a, tdl, TaggedWords); //create dependencies
		//organize by dependencies in Hash table
		NLP.setHash(tdl, nodeHash);
	//	System.out.println(nodeHash);
		//NN: Noun Compound Modifier is the Primary relationship. If it exists we know high priority to last work
		// and same low priority to other preceding words
		TermP = NLP.TermPrioritization(a, nodeHash, tdl, phraseA);
	//	System.out.println("prioritization " +TermP);
		//lemmatize all terms
		
		//determine if word/not word
		return TermP;
		
	}

	private static String termseperator(String Term) {
		Term = splitCamelCase(Term);
		Term = Term.replace('_', ' ');
		return Term;
		
	}

	public static double[][] WordNetMappingFunction(ArrayList <String> SchemaElements1, ArrayList <String> SchemaElements2) throws InvalidFormatException, IOException {
		
		double[][] SchemaMatrix = new double[SchemaElements1.size()][SchemaElements2.size()];
		PreProcess(SchemaElements1);
		PreProcess(SchemaElements2);
		
		//create wordsimilarity scores
		
		WS4Jwrapper WordNetScorer = new WS4Jwrapper();
		for(int i = 0; i<SchemaElements1.size(); i++){
			for(int j=0; j<SchemaElements2.size(); j++)
			SchemaMatrix[i][j] = WordNetScorer.dynamicRunner(SchemaElements1.get(i), SchemaElements2.get(j));
			
		}
		return SchemaMatrix;
	}

	private static ArrayList<String> PreProcessLematize(ArrayList<String> schemaElements) {
		//this breaks up the camel case
				for(int i = 0; i<schemaElements.size(); i++){
					schemaElements.set(i, NLP.lemmatize(schemaElements.get(i)).toString().trim());
				}
				return schemaElements;
		
	}

	private static String splitCamelCase(String s) {
		   return s.replaceAll(
		      String.format("%s|%s|%s",
		         "(?<=[A-Z])(?=[A-Z][a-z])",
		         "(?<=[^A-Z])(?=[A-Z])",
		         "(?<=[A-Za-z])(?=[^A-Za-z])"
		      ),
		      " "
		   );
		}


	private static void PreProcess(ArrayList<String> terms) {

		//this breaks up the camel case
		for(int i = 0; i<terms.size(); i++){
			terms.set(i, splitCamelCase(terms.get(i)) );
			terms.set(i, terms.get(i).replace('_', ' '));
		}
		
	}


}
