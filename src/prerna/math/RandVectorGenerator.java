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
package prerna.math;

import java.util.ArrayList;
import java.util.Random;

import org.apache.commons.math3.random.RandomVectorGenerator;

public class RandVectorGenerator implements RandomVectorGenerator{

	ArrayList<Double> startingPoints = new ArrayList<Double>();
	double max = 1;
	int size = 1;
	
	public void setMax(double max) {
		this.max = max;
	}
	
	public void setSize(int size) {
		this.size = size;
	}
	public ArrayList<Double> getStartingPoints() {
		return startingPoints;
	}
	
	@Override
	public double[] nextVector() {
		Random randomGenerator = new Random();
		double val = max/4.0 + randomGenerator.nextDouble()*max*3.0/4.0;
		double[] next = new double[size];
		for(int i=0;i<next.length;i++) {
			next[i] = val;
		}
		startingPoints.add(val);
		return next;
	}

}
