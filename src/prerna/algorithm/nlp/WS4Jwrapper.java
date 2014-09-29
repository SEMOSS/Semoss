package prerna.algorithm.nlp;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import edu.cmu.lti.lexical_db.ILexicalDatabase;
import edu.cmu.lti.lexical_db.NictWordNet;
import edu.cmu.lti.ws4j.RelatednessCalculator;
import edu.cmu.lti.ws4j.impl.LeacockChodorow;
import edu.cmu.lti.ws4j.util.WS4JConfiguration;

public class  WS4Jwrapper {
        
		
		
        private static ILexicalDatabase db = new NictWordNet();
      //  private static RelatednessCalculator[] rcs = {
      //                  new HirstStOnge(db), new LeacockChodorow(db), new Lesk(db),  new WuPalmer(db), 
      //                  new Resnik(db), new JiangConrath(db), new Lin(db), new Path(db)
      //                  };
        
        
        private static RelatednessCalculator[] rcsLeecocke = { new LeacockChodorow(db)};
        private static RelatednessCalculator[] rcs = { new LeacockChodorow(db)};
        
        
        public void run( String word1, String word2 ) {
                WS4JConfiguration.getInstance().setMFS(true);
                for ( RelatednessCalculator rc : rcs ) { 
                	
                	double s = rc.calcRelatednessOfWords(word1, word2);
                        System.out.println( rc.getClass().getName()+"\t"+s );
                }
        }
        
        public double dynamicRunner( String word1, String word2 ) {
        	double score = 0;
        	List<String> words1 = new ArrayList<String>();
        	List<String> words2 = new ArrayList<String>();
            
        	StringTokenizer word1tok = new StringTokenizer(word1);
        	StringTokenizer word2tok = new StringTokenizer(word2);
        	
        	while(word1tok.hasMoreElements()){
        		words1.add(word1tok.nextElement().toString());
        	}
        	while(word2tok.hasMoreElements()){
        		words2.add(word2tok.nextElement().toString());
        	}
        	 
        	String [] words1array = words1.toArray(new String[words1.size()]);
        	String [] words2array = words2.toArray(new String[words2.size()]);
        	
        	if(words1array.length > 1 || words2array.length >1){
        		score = run3(words1array, words2array, word1, word2); //if one of the two fields is greater than 1 word then run this
        	}else{
        		score = run2(word1, word2); //for two single word comparisons
        	}
        	
        	return score;
        		
        }
        private double run3(String[] words1array, String[] words2array, String word1, String word2) {
        	double matrix_average = 0;
        //	System.out.println("multi element field");
        	// TODO Auto-generated method stub
        	 WS4JConfiguration.getInstance().setMFS(true);
             System.out.print(word1 +"~"+ word2 +"~");
             for ( RelatednessCalculator rc : rcs ) { 
            	 double[][] s =	getSimilarityMatrix(words1array, words2array, rc); //non normalized
             	//double[][] s = rc.getNormalizedSimilarityMatrix(words1array, words2array); //normalized
             	matrix_average = similarityMatrixAnalysis(s);
                     System.out.print( matrix_average + "~" );
             }
             System.out.println("");
             return matrix_average;
		}

		private double[][] getSimilarityMatrix(String[] words1array, String[] words2array, RelatednessCalculator rc) {
			double normalizer = 1.5;
			double[][]  SimMatrix = new double[words1array.length][words2array.length] ;
			for (int i = 0; i < words1array.length; i++) {  
			    for (int j = 0; j < words2array.length; j++) {  
			    	SimMatrix[i][j] = rc.calcRelatednessOfWords(words1array[i], words2array[j]);
			    		if(SimMatrix[i][j] > 20){
			    			SimMatrix[i][j] = rc.calcRelatednessOfWords("car", "automobile")*normalizer;
			    		}
			    }  
			    //System.out.println();
			} 
			
			return SimMatrix;
		}

		private double similarityMatrixAnalysis(double[][] simM) {
		//	System.out.println("SIMMATRIX");
			// TODO Auto-generated method stub
			double simM_avg = 0;
			 double max_val = 0;
		//	 System.out.println("Inside Matrix Analysis");
			 
			//this should only sum and average those relations which don't have a zero value because the other relationships aren't mean to be compared 
			if(simM.length <= simM[0].length){
				 for (int i = 0; i < simM.length; i++) {  
					 max_val = 0;
				    for (int j = 0; j < simM[i].length; j++) {  
				    	if(simM[i][j]>max_val){
									max_val = simM[i][j];
				    	}
				   // 	System.out.print(simM[i][j]+" ");
				    }  
				    simM_avg = simM_avg + max_val;
				//    System.out.println();
				} 
				 simM_avg = simM_avg/simM[0].length;
			}
			
			if(simM.length > simM[0].length){
				 for (int i = 0; i < simM[0].length; i++) {  
					 max_val = 0;
					 for (int j = 0; j < simM.length; j++) {  
						 if(simM[j][i]>max_val){
							 if(simM[j][i]>4);
							 }
						// System.out.print(simM[j][i]+" ");
				    }  
				  //  System.out.println();
				    simM_avg = simM_avg + max_val;
				} 
				 simM_avg = simM_avg/simM.length;
			}
			
			return simM_avg;
		}

		public double run2( String word1, String word2 ) {
			double s = 0;
			double normalizer = 1.5;
			// System.out.println(" single word run");
			WS4JConfiguration.getInstance().setMFS(true);
			
            System.out.print(word1 +"~"+ word2 +"~");
            for ( RelatednessCalculator rcLeecocke : rcsLeecocke ) { 
            	
            	 s = rcLeecocke.calcRelatednessOfWords(word1, word2);
            	if(s>100)
            		s = rcLeecocke.calcRelatednessOfWords("car", "automobile")*normalizer;
                    //System.out.print( s + "~" );
                    
            }
            System.out.println();
            return s;
    }
		
		public double NormalizedLeacockChodorow( String word1, String word2 ) {
			double s = 0;
			double max_normalizer = 3.6888794541139363;
			s = rcs[0].calcRelatednessOfWords(word1, word2);
			s = s/max_normalizer;
			if(s>100){
			s =	rcs[0].calcRelatednessOfWords("car", "automobile");
			s = s/max_normalizer;
			}
			return s;
    }
        public static void main(String[] args) {
            //    long t0 = System.currentTimeMillis();
                WS4Jwrapper test = new WS4Jwrapper();
            //    test.run( "car","automobile" );
            //    long t1 = System.currentTimeMillis();
            //    System.out.println( "Done in "+(t1-t0)+" msec." );
        		System.out.println("Start");
        		System.out.println("WN " + test.NormalizedLeacockChodorow( "element", "outcome" ));
        		System.out.println("LEV " + test.LevenshteinAnalysis("CCDdsd", "CCDdsd"));
        }

        public double WN_NLP_Scorer(String a, String b, ArrayList<String> lPhraseA, ArrayList<String> lPhraseB, ArrayList<Double> apriority, ArrayList<Double> bpriority, ArrayList<Boolean> aIsWord, ArrayList<Boolean> bIsWord) 
		{
			//test too see if the two words are identical
			if(a.equals(b) && b.equals(a) && (a.length() == b.length())){
				return 1;
			}
			double[][]  SimMatrix = new double[lPhraseA.size()][lPhraseB.size()] ;
			double div_term = 0;
			for(int i = 0; i < lPhraseA.size(); i++){
				for(int j = 0; j < lPhraseB.size(); j++){
					//if A(i) and B(i) are words then compare them using WordNet Similarity
					if(aIsWord.get(i) && bIsWord.get(j)){
						//wordnet solution score: range from 0 - 1.
						SimMatrix[i][j] = NormalizedLeacockChodorow(lPhraseA.get(i),lPhraseB.get(j));
						//NLP term importance
						SimMatrix[i][j] = SimMatrix[i][j] * apriority.get(i) * bpriority.get(j);
						System.out.println("WN "+ i+" "+j+" "+SimMatrix[i][j]  +"  "+ lPhraseA.get(i)+ " " +lPhraseB.get(j));
						div_term = div_term + (apriority.get(i) * bpriority.get(j));
					}
					//compare them using Levinshtein distance
					else{
						//word similarity solution
						SimMatrix[i][j] = LevenshteinAnalysis(lPhraseA.get(i),lPhraseB.get(j));
						SimMatrix[i][j] = SimMatrix[i][j] * apriority.get(i) * bpriority.get(j);
						System.out.println("LV "+ i +" "+j+" "+SimMatrix[i][j] +"  "+ lPhraseA.get(i)+ " " +lPhraseB.get(j));
						div_term = div_term + (apriority.get(i) * bpriority.get(j));
					}
					//Threshholding: If a matching between 2 terms is below a certain threshhold consider it zero
					if(SimMatrix[i][j] < .2){
						SimMatrix[i][j] = 0;
						div_term = div_term - (apriority.get(i) * bpriority.get(j));
					}
				}
			}
			
		 	double finalscore =  MatrixSum(SimMatrix, div_term);
			return finalscore;
		}
		
		

		private double MatrixSum(double[][] simMatrix, double div_term) {
			double sum = 0; 
			for (int i = 0; i < simMatrix.length; i++) {  
			    for (int j = 0; j < simMatrix[i].length; j++) { 
			    	sum = sum + simMatrix[i][j];
			    	System.out.print(simMatrix[i][j] + " ");
			    }
			    System.out.println();
			} 
			return (sum/ (div_term+.000000000000001) ); //avoid division by zero
			
		}

		private double LevenshteinAnalysis(String string, String string2) {
			double SimilarityScore = 0;
			//LevenshteinDistance LevAnalyzer = new LevenshteinDistance();
			LetterPairSimilarity LPSmatcher = new LetterPairSimilarity();
			SimilarityScore = LPSmatcher.compareStrings(string, string2);
			return SimilarityScore;
		}

		private double WNNLPScorer(ArrayList<String> lPhraseA,
				ArrayList<String> lPhraseB, ArrayList<Integer> apriority,
				ArrayList<Integer> bpriority, ArrayList<Boolean> aIsWord,
				ArrayList<Boolean> bIsWord) {
			// TODO Auto-generated method stub
			return 0;
		}

		private double test() {
			// TODO Auto-generated method stub
			return 0;
		}
}