package prerna.algorithm.learning.util;

import java.util.List;

public interface IDuplicationReconciliation {

	enum ReconciliationMode {MEAN, MODE, MEDIAN, MAX, MIN}

	Double[] reconciliatedValues(List<Object[]> duplicatedValues);
	
	Double[] reconciliatedValues(List<Object[]> duplicatedValues, boolean[] columnsToReconcile);

	boolean isIgnoreEmptyValues();

	void setIgnoreEmptyValues(boolean ignoreEmptyValues);
	
}
