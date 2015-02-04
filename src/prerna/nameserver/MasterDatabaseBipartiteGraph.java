package prerna.nameserver;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Bipartite Tree structure to connect multiple BiPartiteNodes for connecting keywords to multiple master concepts.
 * @param <T>
 */
public class MasterDatabaseBipartiteGraph<T> {

	private Map<T, Set<T>> keywordMapping;
	private Map<T, Set<T>> mcMapping;
	
	/**
	 * Constructor for MasterDatabaseBipartiteTree
	 */
	public MasterDatabaseBipartiteGraph() {
		keywordMapping = new HashMap<T, Set<T>>();
		mcMapping = new HashMap<T, Set<T>>();
	}

	/**
	 * Adds a node to the master concept set of the bipartite graph and recursively all its connections into the mapping
	 * Assumes all the children/parents (can't have both - see BipartiteNode) are keywords and automatically adds all those nodes and connections to the keyword set
	 * @param node
	 */
	public void addToMCSet(BipartiteNode<T> node) {
		// add children/parents to keywordSet/Values
		// keeps it generic if user defines relationship as keyword parent of master concept or master concept parent of keyword
		if(node.hasChildren()) { // if the node has children
			List<BipartiteNode<T>> children = node.getChildren();
			addValuesToMap(mcMapping, node.data, children);
			addInverseValuesToMap(keywordMapping, node.data, children);
			
			// recursively add all other parents for a given node
			int i = 0;
			int numChildren = children.size();
			for(; i < numChildren; i++) {
				List<BipartiteNode<T>> parents = children.get(i).getParents();
				int j = 0;
				int numOtherParents = parents.size();
				for(; j < numOtherParents; j++) {
					// to prevent infinite recursion 
					if(!mcMapping.containsKey(parents.get(j).data)) {
						addToMCSet(parents.get(j));
					}
				}
			}
		} else { // if the node has parents
			List<BipartiteNode<T>> parents = node.getParents();
			addValuesToMap(mcMapping, node.data, parents);
			addInverseValuesToMap(keywordMapping, node.data, parents);
			
			// recursively add all other parents for a given node
			int i = 0;
			int numParents = parents.size();
			for(; i < numParents; i++) {
				List<BipartiteNode<T>> children = parents.get(i).getChildren();
				int j = 0;
				int numOtherChildren = children.size();
				for(; j < numOtherChildren; j++) {
					// to prevent infinite recursion 
					if(!mcMapping.containsKey(children.get(j).data)) {
						addToMCSet(children.get(j));
					}
				}
			}
		}
	}

	/**
	 * Adds a node to the keyword set of the bipartite graph and recursively all its connections into the mapping
	 * Assumes all the children/parents (can't have both - see BipartiteNode) are master concepts and automatically adds all those nodes and connections to the keyword set
	 * @param node
	 */
	public void addToKeywordSet(BipartiteNode<T> node) {
		// add children/parents to mcSet/Values
		// keeps it generic if user defines relationship as keyword parent of master concept or master concept parent of keyword
		if(node.hasChildren()) { // if the node has children
			List<BipartiteNode<T>> children = node.getChildren();
			addValuesToMap(keywordMapping, node.data, children);
			addInverseValuesToMap(mcMapping, node.data, children);
			
			// recursively add all other parents for a given node
			int i = 0;
			int numChildren = children.size();
			for(; i < numChildren; i++) {
				List<BipartiteNode<T>> parents = children.get(i).getParents();
				int j = 0;
				int numOtherParents = parents.size();
				for(; j < numOtherParents; j++) {
					// to prevent infinite recursion 
					if(!keywordMapping.containsKey(parents.get(j).data)) {
						addToKeywordSet(parents.get(j));
					}
				}
			}
		} else { // if the node has parents
			List<BipartiteNode<T>> parents = node.getParents();
			addValuesToMap(keywordMapping, node.data, parents);
			addInverseValuesToMap(mcMapping, node.data, parents);
			
			// recursively add all other parents for a given node
			int i = 0;
			int numParents = parents.size();
			for(; i < numParents; i++) {
				List<BipartiteNode<T>> children = parents.get(i).getChildren();
				int j = 0;
				int numOtherChildren = children.size();
				for(; j < numOtherChildren; j++) {
					// to prevent infinite recursion 
					if(!keywordMapping.containsKey(children.get(j).data)) {
						addToKeywordSet(children.get(j));
					}
				}
			}
		}
	}
	
	/**
	 * Adds the bipartite connections between the key and all it's values to the map
	 * @param map			The map to put all the connections
	 * @param key			The key to add in the map
	 * @param valuesList	The list of values for the key
	 */
	private void addValuesToMap(Map<T, Set<T>> map, T key, List<BipartiteNode<T>> valuesList) {
		int i = 0;
		int size = valuesList.size();
		
		Set<T> values = new HashSet<T>(size);
		for(; i < size; i++) {
			values.add(valuesList.get(i).data);
		}
		if(map.containsKey(key)) {
			map.get(key).addAll(values);
		} else {
			map.put(key, values);
		}
	}
	
	/**
	 * Adds the bipartite connections between a list of key and the value to add
	 * @param map			The map to put all the connections to
	 * @param value			The value for all the keys
	 * @param keyList		The list of keys to give the value
	 */
	private void addInverseValuesToMap(Map<T, Set<T>> map, T value, List<BipartiteNode<T>> keyList) {
		int i = 0;
		int size = keyList.size();
		
		for(; i < size; i++) {
			T key = keyList.get(i).data;
			if(map.containsKey(key)) {
				map.get(key).add(value);
			} else {
				Set<T> values = new HashSet<T>();
				values.add(value);
				map.put(key, values);
			}
		}
		
	}

	public Map<T, Set<T>> getKeywordMapping() {
		return keywordMapping;
	}

	public void setKeywordMapping(Map<T, Set<T>> keywordMapping) {
		this.keywordMapping = keywordMapping;
	}

	public Map<T, Set<T>> getMcMapping() {
		return mcMapping;
	}

	public void setMcMapping(Map<T, Set<T>> mcMapping) {
		this.mcMapping = mcMapping;
	}
	
}
