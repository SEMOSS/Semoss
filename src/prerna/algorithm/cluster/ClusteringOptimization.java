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
		f.setList(masterTable);
		f.setNames(varNames);
		UnivariateOptimizer optimizer = new BrentOptimizer(1E-6, 1E-6);
        RandomGenerator rand = new Well1024a(500);
        MultiStartUnivariateOptimizer multiOpt = new MultiStartUnivariateOptimizer(optimizer, 5, rand);
        UnivariateObjectiveFunction objF = new UnivariateObjectiveFunction(f);
//        SearchInterval search = new SearchInterval(2, (int) Math.round(Math.sqrt(masterTable.size()))); //considering range from 2 to square root of number of instances
        SearchInterval search = new SearchInterval(2, (int) Math.sqrt(masterTable.size()));
        MaxEval eval = new MaxEval(200);
        
        OptimizationData[] data = new OptimizationData[]{search, objF, GoalType.MAXIMIZE, eval};
        UnivariatePointValuePair pair = multiOpt.optimize(data);
        
        // must calculate two endpoints to determine which is larger
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
