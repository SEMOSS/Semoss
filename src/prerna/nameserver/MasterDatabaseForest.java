package prerna.nameserver;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Combines multiple trees and stores as parent -> child relationships in a HashMap
 * @param <T>
 */
public class MasterDatabaseForest<T> {

	// map the children values for each node
	private Map<T, Set<T>> valueMapping;
	
	/**
	 * Constructor for the master concept forest 
	 */
	public MasterDatabaseForest() {
		valueMapping = new HashMap<T, Set<T>>();
	}
	
	/**
	 * Adds the tree of all nodes connected to the inputed node
	 * Updates the value mapping object which contains the parent-> child values from the tree
	 * @param node	The inputed node connected to a tree
	 */
	public void addNodes(TreeNode<T> node) {
		TreeNode<T> rootNode = node.getRootNode();
		
		Set<T> childrenValues = new HashSet<T>();
		if(rootNode.hasChildren()) {
			List<TreeNode<T>> children = rootNode.getChildren();
			int numChildren = children.size();
			int i = 0;
			for(; i < numChildren; i++) {
				TreeNode<T> childNode = children.get(i);
				childrenValues.add(childNode.data);
				recursivelyAddChildrenNodes(childNode);
			}
		}
		if(valueMapping.containsKey(rootNode.data)) {
			Set<T> currentSet = valueMapping.get(rootNode.data);
			currentSet.addAll(childrenValues);
		} else {
			valueMapping.put(rootNode.data, childrenValues); // put an empty set if the node is a leaf
		}
	}
	
	/**
	 * Recursively traverses down a node to add all the parent->child relationships into the value mapping
	 * @param node	The node which is traversed down to add all the values
	 */
	private void recursivelyAddChildrenNodes(TreeNode<T> node) {
		Set<T> childrenValues = new HashSet<T>();
		if(node.hasChildren()) {
			List<TreeNode<T>> children = node.getChildren();
			int numChildren = children.size();
			int i = 0;
			for(; i < numChildren; i++) {
				TreeNode<T> childNode = children.get(i);
				childrenValues.add(childNode.data);
				recursivelyAddChildrenNodes(childNode);
			}
		}
		if(valueMapping.containsKey(node.data)) {
			Set<T> currentSet = valueMapping.get(node.data);
			currentSet.addAll(childrenValues);
		} else {
			valueMapping.put(node.data, childrenValues); // put an empty set if the node is a leaf
		}
	}

	public Map<T, Set<T>> getValueMapping() {
		return valueMapping;
	}

	public void setValueMapping(Map<T, Set<T>> valueMapping) {
		this.valueMapping = valueMapping;
	}
	
	
}
