/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.algorithm.cluster;

public abstract class AbstractNumericalMethods {
	
	protected String[][] numericalBinMatrix;
	protected String[][] categoricalMatrix;
	protected String[][] instanceNumberBinOrderingMatrix;
	protected int numericPropNum;
	protected int categoricalPropNum;
	protected int totalPropNum;
	
	protected double[] numericalWeights;
	protected double[] categoricalWeights;
	
	public AbstractNumericalMethods(String[][] numericalBinMatrix, String[][] categoricalMatrix, String[][] instanceNumberBinOrderingMatrix) {
		this.numericalBinMatrix = numericalBinMatrix;
		this.categoricalMatrix = categoricalMatrix;
		this.instanceNumberBinOrderingMatrix = instanceNumberBinOrderingMatrix;
		
		if(numericalBinMatrix != null) {
			numericPropNum = numericalBinMatrix[0].length;
		}
		if(categoricalMatrix != null) {
			categoricalPropNum = categoricalMatrix[0].length;
		}
		totalPropNum = numericPropNum + categoricalPropNum;
	}
	
	public void setCategoricalWeights(double[] categoricalWeights) {
		this.categoricalWeights = categoricalWeights;
	}

	public void setNumericalWeights(double[] numericalWeights) {
		this.numericalWeights = numericalWeights;
	}
	
	public double calculateAdjustmentFactor(int index1, int index2, int numBins) {
		if(numBins == 1) {
			return 1;
		}
		
		return 1 - (double) Math.pow((double) Math.abs(index1 - index2) / (numBins-1), 2.0); 
	}
}
