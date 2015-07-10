package prerna.algorithm.learning.util;

import java.util.List;

public class MeanReconciliation implements IDuplicationReconciliation {

	boolean ignoreEmptyValues = true;

	@Override
	public Double[] reconciliatedValues(List<Object[]> duplicatedValues) {
		Double[] reconciliatedValues = new Double[duplicatedValues.get(0).length];
		for(Object[] values : duplicatedValues) {
			for(int i = 0; i < values.length; i++) {
				try {
					reconciliatedValues[i] += (double) values[i];
				} catch (ClassCastException e) {
					if(!ignoreEmptyValues) {
						reconciliatedValues[i]  = Double.NaN;
					}
				}
			}
		}
		return reconciliatedValues;
	}

	@Override
	public Double[] reconciliatedValues(List<Object[]> duplicatedValues, boolean[] columnsToReconcile) {
		Double[] reconciliatedValues = new Double[duplicatedValues.get(0).length];
		for(Object[] values : duplicatedValues) {
			for(int i = 0; i < values.length; i++) {
				if(columnsToReconcile[i]) {
					try {
						reconciliatedValues[i] += (double) values[i];
					} catch (ClassCastException e) {
						if(!ignoreEmptyValues) {
							reconciliatedValues[i]  = Double.NaN;
						}
					}
				}
			}
		}
		return reconciliatedValues;
	}

	@Override
	public boolean isIgnoreEmptyValues() {
		return ignoreEmptyValues;
	}

	@Override
	public void setIgnoreEmptyValues(boolean ignoreEmptyValues) {
		this.ignoreEmptyValues = ignoreEmptyValues;
	}

}
