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

public class NeedlemanWunsch {
        char[] mSeqA;
        char[] mSeqB;
        int[][] mD;
        int mScore;
        String mAlignmentSeqA = "";
        String mAlignmentSeqB = "";
        
        void init(char[] seqA, char[] seqB) {
                mSeqA = seqA;
                mSeqB = seqB;
                mD = new int[mSeqA.length + 1][mSeqB.length + 1];
                for (int i = 0; i <= mSeqA.length; i++) {
                        for (int j = 0; j <= mSeqB.length; j++) {
                                if (i == 0) {
                                        mD[i][j] = -j;
                                } else if (j == 0) {
                                        mD[i][j] = -i;
                                } else {
                                        mD[i][j] = 0;
                                }
                        }
                }
        }
        
        void process() {
                for (int i = 1; i <= mSeqA.length; i++) {
                        for (int j = 1; j <= mSeqB.length; j++) {
                                int scoreDiag = mD[i-1][j-1] + weight(i, j);
                                int scoreLeft = mD[i][j-1] - 1;
                                int scoreUp = mD[i-1][j] - 1;
                                mD[i][j] = Math.max(Math.max(scoreDiag, scoreLeft), scoreUp);
                        }
                }
        }
        
        void backtrack() {
                int i = mSeqA.length;
                int j = mSeqB.length;
                mScore = mD[i][j];
                while (i > 0 && j > 0) {                        
                        if (mD[i][j] == mD[i-1][j-1] + weight(i, j)) {                          
                                mAlignmentSeqA += mSeqA[i-1];
                                mAlignmentSeqB += mSeqB[j-1];
                                i--;
                                j--;                            
                                continue;
                        } else if (mD[i][j] == mD[i][j-1] - 1) {
                                mAlignmentSeqA += "-";
                                mAlignmentSeqB += mSeqB[j-1];
                                j--;
                                continue;
                        } else {
                                mAlignmentSeqA += mSeqA[i-1];
                                mAlignmentSeqB += "-";
                                i--;
                                continue;
                        }
                }
                mAlignmentSeqA = new StringBuffer(mAlignmentSeqA).reverse().toString();
                mAlignmentSeqB = new StringBuffer(mAlignmentSeqB).reverse().toString();
        }
        
        private int weight(int i, int j) {
                if (mSeqA[i - 1] == mSeqB[j - 1]) {
                        return 1;
                } else {
                        return -1;
                }
        }
        
        void printMatrix() {
                System.out.println("D =");
                for (int i = 0; i < mSeqA.length + 1; i++) {
                        for (int j = 0; j < mSeqB.length + 1; j++) {
                                System.out.print(String.format("%4d ", mD[i][j]));
                        }
                        System.out.println();
                }
                System.out.println();
        }
        
        void printScoreAndAlignments() {
                System.out.println("Score: " + mScore);
                System.out.println("Sequence A: " + mAlignmentSeqA);
                System.out.println("Sequence B: " + mAlignmentSeqB);
                System.out.println();
        }
        
        public double NeddleWunschscorer(String string1, String string2){
        	 char[] seqA = string1.toCharArray();
             char[] seqB = string2.toCharArray();
             init(seqA, seqB);
             process();
             backtrack();
        	return mScore;
        	
        }
        
        public static void main(String [] args) {               
                String string1 = "PatientAdmit";
                String string2 = "AdmitPatient";
        	//	char[] seqA = { 'A', 'C', 'G', 'T', 'C' };
            //    char[] seqB = { 'A', 'G', 'T', 'C' };
                char[] seqA = string1.toCharArray();
                char[] seqB = string2.toCharArray();
                NeedlemanWunsch nw = new NeedlemanWunsch();
                nw.init(seqA, seqB);
                nw.process();
                nw.backtrack();
                
                nw.printMatrix();
                nw.printScoreAndAlignments();
        }
}