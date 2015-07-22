package prerna.algorithm.learning.util;

import java.util.List;
import java.util.Map;

public final class InstanceSimilarity {

	private InstanceSimilarity() {
		
	}
	
	public static double getInstanceSimilarity(List<Object[]> instance1, List<Object[]> instance2, boolean[] isNumeric, String[] attributeNames, Map<String, IDuplicationReconciliation> dups) {
		double categoricalSim = calculateInstanceCategoricalSim(instance1, instance2, isNumeric);
		double numericalSim = calculateNumericalSim(instance1, instance2, isNumeric, attributeNames, dups);
		
		return categoricalSim + numericalSim;
	}

	private static double calculateNumericalSim(List<Object[]> instance1, List<Object[]> instance2, boolean[] isNumeric, String[] attributeNames, Map<String, IDuplicationReconciliation> dups) {
		double sim = 0;
		int numNumeric = 0;
		for(int i = 0; i < isNumeric.length; i++) {
			if(isNumeric[i]) {
				numNumeric++;
				IDuplicationReconciliation dupSolver = dups.get(attributeNames[i]);
				Double[] values1 = dupSolver.reconciliatedValues(instance1, isNumeric);
				Double[] values2 = dupSolver.reconciliatedValues(instance2, isNumeric);

				for(int j = 0; j < values1.length; j++) {
					sim += Math.pow(values1[j] - values2[j], 2);
				}
			}
		}
		
		if(numNumeric == 0) {
			return sim;
		}
		
		return Math.sqrt(sim) / numNumeric * ( (double) numNumeric / isNumeric.length);
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
				
				sim += (double) matchCount / totalCount;
			}
		}
		
		if(numCategorical == 0) {
			return sim;
		}
		
		return sim / numCategorical * ( (double) numCategorical / isNumeric.length);
	}
	
}
