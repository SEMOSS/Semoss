package prerna.algorithm.learning.util;

import java.util.List;
import java.util.Map;

/**
 * Utility class that provides methods for calculating similarity between data instances.
 * This class offers static methods to compute similarity scores between instances that
 * may contain both numerical and categorical attributes, handling duplicate reconciliation
 * for instances with multiple values per attribute.
 * 
 * <p>
 * The similarity calculation combines both categorical and numerical similarity measures,
 * weighted by the proportion of each type of attribute in the data. The class is designed
 * as a utility with static methods and cannot be instantiated.
 * </p>
 * 
 * @see {@link DuplicationReconciliation} for handling multiple values per attribute
 */
public final class InstanceSimilarity {

	/**
	 * Private constructor to prevent instantiation of this utility class.
	 */
	private InstanceSimilarity() {
		
	}
	
	/**
	 * Calculates the overall similarity between two data instances.
	 * The similarity combines both categorical and numerical attribute similarities,
	 * weighted by their respective proportions in the dataset.
	 *
	 * @param instance1 The first instance as a list of object arrays (supporting multiple values per attribute).
	 * @param instance2 The second instance as a list of object arrays (supporting multiple values per attribute).
	 * @param isNumeric Boolean array indicating which attributes are numerical.
	 * @param attributeNames Array of attribute names corresponding to the data columns.
	 * @param dups Map of duplication reconciliation objects for handling multiple values per numerical attribute.
	 * @return The similarity score between the two instances (typically between 0.0 and 1.0).
	 */
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
