package prerna.algorithm.learning.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class DuplicationReconciliation {

	public enum ReconciliationMode {MEAN, MODE, MEDIAN, MAX, MIN}

	ReconciliationMode mode;
	private boolean ignoreEmpty;
	private ArrayList<Object> values;
	Double recValue;
	
	public DuplicationReconciliation() {
		this(ReconciliationMode.MEAN);
	}
	
	public DuplicationReconciliation(ReconciliationMode mode) {
		this.mode = mode;
		ignoreEmpty = true;
		values = new ArrayList<Object>();
		recValue = null;
	}
	
	public Double getReconciliatedValue() {
		if(recValue == null) {
			switch(mode) {
				case MEAN: recValue = getMean(values.toArray());
				case MODE: recValue = getMode(values.toArray());
				case MEDIAN: recValue = getMedian(values.toArray());
				case MAX: recValue = getMax(values.toArray()); 
				case MIN: recValue = getMin(values.toArray());
				default: recValue = getMean(values.toArray());
			}
		}
		return recValue;
	}

	public boolean ignoreEmptyValues() {
		return ignoreEmpty;
	}

	public void setIgnoreEmptyValues(boolean ignoreEmptyValues) {
		ignoreEmpty = true;
	}
	
	public void addValue(Object o) {
		values.add(o);
		recValue = null;
	}
	
	private Double getMean(Object[] row) {
		Double total = 0.0;
		double size = row.length;
		
		for(Object value : row) {
			if(ignoreEmpty) {
				if(value == null || !(value instanceof Number)) {
					size -= 1.0;
					continue;
				} else {
					total += ((Number)value).doubleValue();
				}
			} else {
				if(value == null || !(value instanceof Number)) {
					return Double.NaN;
				} else {
					total += ((Number)value).doubleValue();
				}
			}
		}
		return total/size;
	}
	
	private Double getMode(Object[] row) {
		HashMap<Double,Integer> freqs = new HashMap<Double,Integer>();
		for (Object val : row) {
			if(ignoreEmpty) {
				if(val instanceof Number) {
					Integer freq = freqs.get((Double)val);
					freqs.put(((Number)val).doubleValue(), (freq == null ? 1 : freq+1));
				}
			} else {
				if(val == null || !(val instanceof Number)) {
					val = Double.NaN;
				}
				Integer freq = freqs.get((Double)val);
				freqs.put(((Number)val).doubleValue(), (freq == null ? 1 : freq+1));
			}
		}
			
		Double mode = 0.0;
		int maxFreq = 0;
		for (Map.Entry<Double,Integer> entry : freqs.entrySet()) {
			int freq = entry.getValue();
			if (freq > maxFreq) {
				maxFreq = freq;
				mode = entry.getKey();
			}
		}
		return mode;
	}
	
	private Double getMax(Object[] row) {
		Double max = null;
		for(Object value: row) {
			if(value instanceof Number) {
				Double v = ((Number)value).doubleValue();
				if(max == null) {
					max = v;
				}
				if(max < v) {
					max = v;
				}
			}
		}
	
		return (max == null) ? Double.NaN : max;
	}
	
	private Double getMin(Object[] row) {
		Double min = null;
		for(Object value: row) {
			if(value instanceof Number) {
				Double v = ((Number)value).doubleValue();
				if(min == null) {
					min = v;
				}
				else if(min > v) {
					min = v;
				}
			}
		}
	
		return (min == null) ? Double.NaN : min;
	}
	
	private Double getMedian(Object[] row) {
		//Sort the Array with non numeric values at the beginning
		Arrays.sort(row, new Comparator<Object>() {
			public int compare(Object o1, Object o2) {
				if(o1 instanceof Number && o2 instanceof Number) {
					Double d1 = ((Number)o1).doubleValue();
					Double d2 = ((Number)o2).doubleValue();
					return (d1).compareTo(d2);
				} 
				else if(o1 instanceof Number) return 1;
				else if(o2 instanceof Number) return -1;
				else return 0;
			}
		});
		
		//If the last value is not a number, there are no numbers in the column
		if(!(row[row.length - 1] instanceof Number)) {
			return Double.NaN;
		}
		
		int startIndex = 0;
		
		//If we are ignoring empties, get the median based only on numeric values
		if(ignoreEmpty) {
			for(int i = 0; i < row.length; i++) {
				if(row[i] instanceof Number) {
					startIndex = i; break;
				} 
			}
		}
				
		//Get the middle index 
		int medianIndex = (row.length - startIndex - 1)/2;
		
		//If Even length array take the average of the two middle except if one of them is non numeric
		//Else just return the middle whatever it is
		if(row.length % 2 == 0) {
			int medianIndex2 = medianIndex - 1;
			if(medianIndex2 == startIndex) {
				return (Double)row[medianIndex];
			} else {
				Double median1 = ((Number)row[medianIndex]).doubleValue();
				Double median2 = ((Number)row[medianIndex2]).doubleValue();
				return (median1+median2)/2.0;
			}
		} else {
			if(row[medianIndex] instanceof Number) {
				return ((Number)row[medianIndex]).doubleValue();
			} else {
				return Double.NaN;
			}
		}
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
