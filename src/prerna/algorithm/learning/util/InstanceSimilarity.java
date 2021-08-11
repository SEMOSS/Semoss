package prerna.algorithm.learning.util;

import java.util.List;
import java.util.Map;

public final class InstanceSimilarity {

	private InstanceSimilarity() {
		
	}
	
	public static double getInstanceSimilarity(List<Object[]> instance1, List<Object[]> instance2, boolean[] isNumeric, String[] attributeNames, Map<String, DuplicationReconciliation> dups) {
		double categoricalSim = calculateInstanceCategoricalSim(instance1, instance2, isNumeric);
		double numericalSim = calculateNumericalSim(instance1, instance2, isNumeric, attributeNames, dups);
		
		return categoricalSim + numericalSim;
	}

	private static double calculateNumericalSim(List<Object[]> instance1, List<Object[]> instance2, boolean[] isNumeric, String[] attributeNames, Map<String, DuplicationReconciliation> dups) {
		double sim = 0;
		int numNumeric = 0;
		for(int i = 0; i < isNumeric.length; i++) {
			if(isNumeric[i]) {
				numNumeric++;
				DuplicationReconciliation dupSolver = dups.get(attributeNames[i]);
				
				Double instance1Val = 0.0;
				if(instance1.size() > 1) {
					for(int j = 0; j < instance1.size(); j++) {
						dupSolver.addValue(instance1.get(j)[i]);
					}
					instance1Val = dupSolver.getReconciliatedValue();
					dupSolver.clearValue();
				} else {
					instance1Val = ((Number) instance1.get(0)[i]).doubleValue();
				}
				
				Double instance2Val = 0.0;
				if(instance2.size() > 1) {
					for(int j = 0; j < instance2.size(); j++) {
						dupSolver.addValue(instance2.get(j)[i]);
					}
					instance2Val = dupSolver.getReconciliatedValue();
					dupSolver.clearValue();
				} else {
					instance2Val = ((Number) instance2.get(0)[i]).doubleValue();
				}
				
				sim += Math.pow(instance1Val - instance2Val, 2);
			}
		}
		
		if(numNumeric == 0) {
			return sim;
		}
		
		return (1 - Math.sqrt(sim)) * ( (double) numNumeric / isNumeric.length);
	}

	private static double calculateInstanceCategoricalSim(List<Object[]> instance1, List<Object[]> instance2, boolean[] isNumeric) {
		double sim = 0;
		int numCategorical = 0;
		for(int i = 0; i < isNumeric.length; i++) {
			if(!isNumeric[i]) {
				numCategorical++;
				int matchCount = 0;
				int totalCount = 0;
				
				for(Object[] values1 : instance1) {
					for(Object[] values2 : instance2) {
						if(values1[i].equals(values2[i])) {
							matchCount++;
						}
						totalCount++;
					}
				}
			    if(totalCount == 0) {
			        throw new IllegalArgumentException("totalCount");
			      }
				sim += (double) matchCount / totalCount;
			}
		}
		
		if(numCategorical == 0) {
			return sim;
		}
		
		return sim / numCategorical * ( (double) numCategorical / isNumeric.length);
	}
	
}
