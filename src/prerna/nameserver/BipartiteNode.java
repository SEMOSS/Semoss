/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.nameserver;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Meant for use within MasterDB creation. In this context, all the children are keywords and master concepts are parents (or vice-versa).
 * Constrained so that a node can either have children or parents, but not both, to preserve the bipartite structure.
 * @param <T>
 */
public class BipartiteNode<T> extends BasicNode<T> {

	// NOTE: THESE NODES CANNOT HAVE CHILDREN AND PARENTS
	// the list of children for the node
	private List<BipartiteNode<T>> children;
	// the values for all the children;
	private Set<T> childrenValues;
	// the list of parents for the node
	private List<BipartiteNode<T>> parents;
	// the values for all the parents;
	private Set<T> parentValues;
	
	public BipartiteNode(T data) {
		super(data);
	}
	
	/**
	 * Adds a new parent with the inputed value
	 * @param parent	The value of the parent node
	 * @return			Returns the parent node
	 */
	public BipartiteNode<T> addParent(T parent) {
		if(this.childrenValues != null && !this.childrenValues.isEmpty()) {
			throw new IllegalArgumentException("The node " + this + " already has children.  Cannot add parents.");
		}
//		if(this.parentValues != null && this.parentValues.contains(parent)) {
//			throw new IllegalArgumentException("The value " + parent + " already exists in the map");
//		}
//		if(this.data.equals(parent)) {
//			throw new IllegalArgumentException("The value " + parent + " has the same value as the node you are trying to add to");
//		}
		
		BipartiteNode<T> parentNode = new BipartiteNode<T>(parent);
		parentNode.children = new LinkedList<BipartiteNode<T>>();
		parentNode.childrenValues = new HashSet<T>();
		parentNode.children.add(this);
		parentNode.childrenValues.add(this.data);
		
		if(this.parents == null) {
			this.parents = new LinkedList<BipartiteNode<T>>();
		}
		this.parents.add(parentNode);
		if(this.parentValues == null) {
			this.parentValues = new HashSet<T>();
		}
		this.parentValues.add(parent);
		return parentNode;
	}
	
	/**
	 * Adds the inputed node as a parent
	 * @param parentNode	The node to add as a parent
	 * @return				Returns the child node
	 */
	public BipartiteNode<T> addParent(BipartiteNode<T> parentNode) {
		if(this.childrenValues != null && !this.childrenValues.isEmpty()) {
			throw new IllegalArgumentException("The node " + this + " already has children.  Cannot add parents.");
		}
//		if(this.parentValues != null && this.parentValues.contains(parentNode.data)) {
//			throw new IllegalArgumentException("The value " + parentNode + " already exists in the map");
//		}
//		if(this.data.equals(parentNode.data)) {
//			throw new IllegalArgumentException("The value " + parentNode + " has the same value as the node you are trying to add to");
//		}
		if(parentNode.parents != null && !parentNode.parents.isEmpty()) {
			throw new IllegalArgumentException("The node " + parentNode + " has parent nodes.  Cannot give it a child node.");
		}
		
		if(parentNode.children == null) {
			parentNode.children = new LinkedList<BipartiteNode<T>>();
		}
		parentNode.children.add(this);
		if(parentNode.childrenValues == null) {
			parentNode.childrenValues = new HashSet<T>();
		}
		parentNode.childrenValues.add(this.data);
		
		if(this.parents == null) {
			this.parents = new LinkedList<BipartiteNode<T>>();
		}
		if(this.parentValues == null) {
			this.parentValues = new HashSet<T>();
		}
		this.parents.add(parentNode);
		this.parentValues.add(parentNode.data);
		return parentNode;
	}
	
	/**
	 * Remove the specific node from the list of parents
	 * @param parentNode	The node to remove as a parent
	 * @return				Returns true if the node is a parent, false if the node is not a parent
	 */
	public boolean removeParent(BipartiteNode<T> parentNode) {
		if(this.parents == null || !this.parents.contains(parentNode)) {
			return false;
		}
		// remove the node as a parent
		this.parents.remove(parentNode);
		this.parentValues.remove(parentNode.data);
		
		//remove this as a child of the parent node
		parentNode.children.remove(this);
		parentNode.childrenValues.remove(this.data);
		return true;
	}
	
	/**
	 * Remove the parent with the inputed value
	 * @param parent	The node value to remove as a parent
	 * @return			Returns true if a parent had the value, false if no parent had the value
	 */
	public boolean removeParent(T parent) {
		if(this.parents == null || this.parents.isEmpty() || !this.parentValues.contains(parent)) {
			return false;
		}
		int i = 0;
		int numParents = this.parents.size();
		for(; i < numParents; i++) {
			if(this.parents.get(numParents).data == parent) {
				BipartiteNode<T> parentNode = this.parents.get(i);
				// remove the node as a parent
				this.parentValues.remove(parent);
				this.parents.remove(i);
				
				// remove this as a child of the parent node
				parentNode.children.remove(this);
				parentNode.childrenValues.remove(this.data);
				return true;
			}
		}
		
		return false;
	}
	
	/**
	 * Adds a new child node with the inputed value
	 * @param child		The value for the child node
	 * @return			Returns the child node
	 */
	public BipartiteNode<T> addChild(T child) {
		if(this.parentValues != null && this.parentValues.isEmpty()) {
			throw new IllegalArgumentException("The node " + this + " already has parents.  Cannot add children.");
		}
//		if(this.childrenValues != null && this.childrenValues.contains(child)) {
//			throw new IllegalArgumentException("The value " + child + " already exists in the map");
//		}
//		if(this.data.equals(child)) {
//			throw new IllegalArgumentException("The value " + child + " has the same value as the node you are trying to add to");
//		}
		
		BipartiteNode<T> childNode = new BipartiteNode<T>(child);
		childNode.parents = new LinkedList<BipartiteNode<T>>();
		childNode.parentValues = new HashSet<T>();
		childNode.parents.add(this);
		childNode.parentValues.add(this.data);
		
		if(this.children == null) {
			this.children = new LinkedList<BipartiteNode<T>>();
		}
		this.children.add(childNode);
		if(this.childrenValues == null) {
			this.childrenValues = new HashSet<T>();
		}
		this.childrenValues.add(child);

		return childNode;
	}
	
	/**
	 * Adds the inputed node as a child 
	 * @param childNode		The node to add as a child
	 * @return				Returns the child node
	 */
	public BipartiteNode<T> addChild(BipartiteNode<T> childNode) {
		if(this.parentValues != null && this.parentValues.isEmpty()) {
			throw new IllegalArgumentException("The node " + this + " already has parents.  Cannot add children.");
		}
//		if(this.childrenValues != null && this.childrenValues.contains(childNode.data)) {
//			throw new IllegalArgumentException("The value " + childNode + " already exists in the map");
//		}
//		if(this.data.equals(childNode.data)) {
//			throw new IllegalArgumentException("The value " + childNode + " has the same value as the node you are trying to add to");
//		}
		if(childNode.children != null && !childNode.children.isEmpty()) {
			throw new IllegalArgumentException("The node " + childNode + " has child nodes.  Cannot give it a parent node.");
		}
		
		if(childNode.parents == null) {
			childNode.parents = new LinkedList<BipartiteNode<T>>();
		}
		childNode.parents.add(this);
		if(childNode.parentValues == null) {
			childNode.parentValues = new HashSet<T>();
		}
		childNode.parentValues.add(this.data);

		if(this.children == null) {
			this.children = new LinkedList<BipartiteNode<T>>();
		}
		this.children.add(childNode);
		if(this.childrenValues == null) {
			this.childrenValues = new HashSet<T>();
		}
		this.childrenValues.add(childNode.data);

		return childNode;
	}
	
	/**
	 * Remove the specific node from the list of children
	 * @param childNode		The node to remove as a child
	 * @return				Returns true if the node is a child, false if the node is not a child
	 */
	public boolean removeChild(BipartiteNode<T> childNode) {
		if(this.children == null || !this.children.contains(childNode)) {
			return false;
		}
		// remove the node as a child
		this.children.remove(childNode);
		this.childrenValues.remove(childNode.data);
		
		//remove this as a parent of the child node
		childNode.parents.remove(this);
		childNode.parentValues.remove(this.data);
		return true;
	}
	
	/**
	 * Remove the child with the inputed value
	 * @param parent	The node value to remove as a child
	 * @return			Returns true if a child had the value, false if no child had the value
	 */
	public boolean removeChild(T child) {
		if(this.children == null || this.children.isEmpty() || this.children.contains(child)) {
			return false;
		}
		int i = 0;
		int numChildren = children.size();
		for(; i < numChildren; i++) {
			if(children.get(numChildren).data == child) {
				BipartiteNode<T> childNode = this.children.get(i);
				// remove the node as a child
				this.childrenValues.remove(child);
				this.children.remove(i);
				
				// remove this as a child of the parent node
				childNode.parents.remove(this);
				childNode.parentValues.remove(this.data);
				return true;
			}
		}
		
		return false;
	}
	
	/**
	 * Determines if the node has children
	 * @return	Boolean if the node has children
	 */
	public boolean hasChildren() {
		if(this.childrenValues != null && this.childrenValues.size() > 0) {
			return true;
		}
		return false;
	}
	
	/**
	 * Determines if the node has parents
	 * @return	Boolean if the node has parents 
	 */
	public boolean hasParent() {
		if(this.parentValues != null && this.parentValues.size() > 0) {
			return true;
		}
		return false;
	}

	public List<BipartiteNode<T>> getChildren() {
		return children;
	}

	public void setChildren(List<BipartiteNode<T>> children) {
		this.children = children;
	}

	public Set<T> getChildrenValues() {
		return childrenValues;
	}

	public void setChildrenValues(Set<T> childrenValues) {
		this.childrenValues = childrenValues;
	}

	public List<BipartiteNode<T>> getParents() {
		return parents;
	}

	public void setParents(List<BipartiteNode<T>> parents) {
		this.parents = parents;
	}

	public Set<T> getParentValues() {
		return parentValues;
	}

	public void setParentValues(Set<T> parentValues) {
		this.parentValues = parentValues;
	}
	
}
