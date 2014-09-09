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
package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import edu.cmu.lti.lexical_db.ILexicalDatabase;
import edu.cmu.lti.lexical_db.NictWordNet;
import edu.cmu.lti.ws4j.RelatednessCalculator;
import edu.cmu.lti.ws4j.impl.HirstStOnge;
import edu.cmu.lti.ws4j.impl.JiangConrath;
import edu.cmu.lti.ws4j.impl.LeacockChodorow;
import edu.cmu.lti.ws4j.impl.Lesk;
import edu.cmu.lti.ws4j.impl.Lin;
import edu.cmu.lti.ws4j.impl.Path;
import edu.cmu.lti.ws4j.impl.Resnik;
import edu.cmu.lti.ws4j.impl.WuPalmer;
import edu.cmu.lti.ws4j.util.WS4JConfiguration;

public class  WS4Jwrapper {
        
		
		
        private static ILexicalDatabase db = new NictWordNet();
      //  private static RelatednessCalculator[] rcs = {
      //                  new HirstStOnge(db), new LeacockChodorow(db), new Lesk(db),  new WuPalmer(db), 
      //                  new Resnik(db), new JiangConrath(db), new Lin(db), new Path(db)
      //                  };
        
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
 //            System.out.print(word1 +"~"+ word2 +"~");
             for ( RelatednessCalculator rc : rcs ) { 
            	 double[][] s =	getSimilarityMatrix(words1array, words2array, rc); //non normalized
             	//double[][] s = rc.getNormalizedSimilarityMatrix(words1array, words2array); //normalized
             	matrix_average = similarityMatrixAnalysis(s);
//                     System.out.print( matrix_average + "~" );
             }
//             System.out.println("");
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
			
//            System.out.print(word1 +"~"+ word2 +"~");
            for ( RelatednessCalculator rc : rcs ) { 
            	
            	 s = rc.calcRelatednessOfWords(word1, word2);
            	if(s>100)
            		s = rc.calcRelatednessOfWords("car", "automobile")*normalizer;
//                    System.out.print( s + "~" );
                    
            }
//            System.out.println();
            return s;
    }
        public static void main(String[] args) {
                long t0 = System.currentTimeMillis();
                WS4Jwrapper test = new WS4Jwrapper();
                test.run( "car","plane" );
                long t1 = System.currentTimeMillis();
                System.out.println( "Done in "+(t1-t0)+" msec." );
        }
}