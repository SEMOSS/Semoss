package prerna.algorithm.learning.similarity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

public class ClusterCenter {

	private List<String> propNames;
	private List<Map<String, Double>> clusterValues;
	
	public ClusterCenter() {
		propNames = new ArrayList<String>();
		clusterValues = new ArrayList<Map<String, Double>>();
	}
	
	public void addToCluster(String propName, String propVal) {
		addToCluster(propName, propVal, 1.0);
	}
	
	public void addToCluster(String propName, String propVal, double coeff) {
		int index = propNames.indexOf(propName);
		Map<String, Double> valCount;
		if(index == -1) { // new property to consider
			propNames.add(propName);
			valCount = new Hashtable<String, Double>();
			valCount.put(propVal, coeff);
			clusterValues.add(valCount);
		} else { // old property
			valCount = clusterValues.get(index);
			if(valCount.containsKey(propVal)) { // old instance val for property
				double count = valCount.get(propVal);
				count += coeff;
				valCount.put(propVal, count);
			} else { // new instance val for property
				valCount.put(propVal, coeff);
			}
		}
	}
	
	public void addToCluster(int index, String propVal) {
		addToCluster(index, propVal, 1.0);
	}
	
	public void addToCluster(int index, String propVal, double coeff) {
		if(clusterValues.size() <= index) {
			fillCluster(index);
		}
		Map<String, Double> valCount = clusterValues.get(index);
		if(valCount.containsKey(propVal)) { // old instance val for property
			double count = valCount.get(propVal);
			count += coeff;
			valCount.put(propVal, count);
		} else { // new instance val for property
			valCount.put(propVal, coeff);
		}
	}
	
	private void fillCluster(int index) {
		int i = clusterValues.size();
		for(; i <= index; i++) {
			clusterValues.add(new HashMap<String, Double>());
		}
	}

	public void removeFromCluster(String propName, String propVal) {
		removeFromCluster(propName, propVal, 1.0);
	}
	
	public void removeFromCluster(String propName, String propVal, double coeff) {
		int index = propNames.indexOf(propName);
		Map<String, Double> valCount;
		if(index == -1) { // property not found
			throw new NullPointerException("Property " + propName + " cannot be found in cluster to remove...");
		} else { // old property
			valCount = clusterValues.get(index);
			if(valCount.containsKey(propVal)) { // reduce count by 1
				double count = valCount.get(propVal);
				count -= coeff;
				valCount.put(propVal, count);
			} else { // value cannot be found
				throw new NullPointerException("Property " + propName + " with value " + propVal + " cannot be found in cluster to remove...");
			}
		}
	}
	
	public void removeFromCluster(int index, String propVal) {
		removeFromCluster(index, propVal, 1.0);
	}
	
	public void removeFromCluster(int index, String propVal, double coeff) {
		Map<String, Double> valCount = clusterValues.get(index);
		if(valCount.containsKey(propVal)) { // reduce count by 1
			double count = valCount.get(propVal);
			count -= coeff;
			valCount.put(propVal, count);
		} else { // value cannot be found
			throw new NullPointerException("Property " + propNames.get(index) + " with value " + propVal + " cannot be found in cluster to remove...");
		}
	}

	public Map<String, Double> get(int index) {
		int size = clusterValues.size();
		if(size - 1 < index) {
			throw new IndexOutOfBoundsException("Index value " + index + ", is larger than the size of the cluster list, " + (size - 1));
		}
		return clusterValues.get(index);
	}
	
	public boolean isEmpty() {
		if(clusterValues.size() == 0) {
			return true;
		}
		return false;
	}

	public List<String> getPropNames() {
		return propNames;
	}
	public void setPropNames(List<String> propNames) {
		this.propNames = propNames;
	}
	public List<Map<String, Double>> getClusterValues() {
		return clusterValues;
	}
	public void setClusterValues(List<Map<String, Double>> clusterValues) {
		this.clusterValues = clusterValues;
	}
	
}
