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

import java.util.ArrayList;

import org.apache.commons.math3.optim.MaxEval;
import org.apache.commons.math3.optim.OptimizationData;
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType;
import org.apache.commons.math3.optim.univariate.BrentOptimizer;
import org.apache.commons.math3.optim.univariate.MultiStartUnivariateOptimizer;
import org.apache.commons.math3.optim.univariate.SearchInterval;
import org.apache.commons.math3.optim.univariate.UnivariateObjectiveFunction;
import org.apache.commons.math3.optim.univariate.UnivariateOptimizer;
import org.apache.commons.math3.optim.univariate.UnivariatePointValuePair;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well1024a;

public class ClusteringOptimization extends ClusteringAlgorithm {

	public ClusteringOptimization(ArrayList<Object[]> masterTable, String[] varNames) {
		super(masterTable, varNames);
	}

	public void determineOptimalCluster() {
		ClusterOptFunction f = new ClusterOptFunction();
		f.setClusterOptimization(this);
		f.setNumInstances(numInstances);
		UnivariateOptimizer optimizer = new BrentOptimizer(1E-6, 1E-6);
        RandomGenerator rand = new Well1024a(500);
        MultiStartUnivariateOptimizer multiOpt = new MultiStartUnivariateOptimizer(optimizer, 5, rand);
        UnivariateObjectiveFunction objF = new UnivariateObjectiveFunction(f);
        SearchInterval search = new SearchInterval(2, (int) Math.round(Math.sqrt(masterTable.size()))); //considering range from 2 to square root of number of instances
        MaxEval eval = new MaxEval(200);
        
        OptimizationData[] data = new OptimizationData[]{search, objF, GoalType.MAXIMIZE, eval};
        UnivariatePointValuePair pair = multiOpt.optimize(data);
        
        // must calculate two end points to determine which is larger
        double val = pair.getPoint();
        int numClusterCeil = (int) Math.ceil(val);
        int numClusterFloor = (int) Math.floor(val);
        
        double avg1 = f.value(numClusterCeil);
        double avg2 = f.value(numClusterFloor);
        if(avg1 > avg2) {
        	numClusters = numClusterCeil;
        } else {
        	numClusters = numClusterFloor;
        }
        f.closeWriter();
	}
	
	public int getNumClusters(){
		return numClusters;
	}
	
	
	
}
