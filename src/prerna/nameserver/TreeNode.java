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
 * Basic Tree graph class
 * @param <T>
 */
public class TreeNode<T> extends BasicNode<T> {

	// the parent for the node
	private TreeNode<T> parent;
	// the list of children for the node
	private List<TreeNode<T>> children;
	// the list of values for children
	private Set<T> childrenValues;
	
	public TreeNode(T data) {
		super(data);
	}

	/**
	 * Adds a parent to the node
	 * @param parent	The value for the parent node
	 * @return			Returns the parent node
	 */
	public TreeNode<T> addParent(T parent) {
		if(this.parent != null) {
			throw new IllegalArgumentException("The node " + this + " already has a parent.  Cannot add another parent.");
		}
		
		TreeNode<T> parentNode = new TreeNode<T>(parent);
		parentNode.children = new LinkedList<TreeNode<T>>();
		parentNode.children.add(this);
		parentNode.childrenValues = new HashSet<T>();
		parentNode.childrenValues.add(this.data);

		this.parent = parentNode;
		return parentNode;
	}
	
	/**
	 * Adds the inputed node as the parent 
	 * @param childNode		The node to add as a parent
	 * @return				Returns the parent node
	 */
	public TreeNode<T> addParent(TreeNode<T> parentNode) {
		if(this.parent != null) {
			throw new IllegalArgumentException("The node " + this + " already has a parent.  Cannot add another parent.");
		}
		
		if(parentNode.children == null) {
			parentNode.children = new LinkedList<TreeNode<T>>();
		}
		parentNode.children.add(this);
		if(parentNode.childrenValues == null) {
			parentNode.childrenValues = new HashSet<T>();
		}
		parentNode.childrenValues.add(this.data);
		
		this.parent = parentNode;
		return parentNode;
	}
	
	/**
	 * Removes the parent for the node
	 */
	public void removeParent() {
		this.parent = null;
	}
	
	/**
	 * Adds a new child node with the inputed value
	 * @param child		The value for the child node
	 * @return			Returns the child node
	 */
	public TreeNode<T> addChild(T child) {
		if(childrenValues != null && this.childrenValues.contains(child)) {
			throw new IllegalArgumentException("The value " + child + " already exists in the map");
		}
		
		TreeNode<T> childNode = new TreeNode<T>(child);
		childNode.parent = this;
		
		if(this.children == null) {
			this.children = new LinkedList<TreeNode<T>>();
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
	public TreeNode<T> addChild(TreeNode<T> childNode) {
		if(childrenValues != null && childrenValues.contains(childNode.data)) {
			throw new IllegalArgumentException("The value " + childNode + " already exists in the map");
		}
		if(childNode.hasParent()) {
			throw new IllegalArgumentException("The child node " + childNode + " already has a parent");
		}
		childNode.parent = this;

		if(this.children == null) {
			this.children = new LinkedList<TreeNode<T>>();
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
	public boolean removeChild(TreeNode<T> childNode) {
		if(this.children == null || !this.children.contains(childNode)) {
			return false;
		}
		// remove the node as a child
		this.children.remove(childNode);
		this.childrenValues.remove(childNode.data);
		
		// remove this node as the parent of the child
		childNode.parent = null;
		return true;
	}
	
	/**
	 * Remove the child with the inputed value
	 * @param parent	The node value to remove as a child
	 * @return			Returns true if a child had the value, false if no child had the value
	 */
	public boolean removeChild(T child) {
		if(this.children == null || this.children.isEmpty() || !this.children.contains(child)) {
			return false;
		}
		int i = 0;
		int numChildren = this.children.size();
		for(; i < numChildren; i++) {
			if(children.get(numChildren).data == child) {
				// remove the node as a child
				TreeNode<T> childNode = children.get(i);
				this.children.remove(i);
				this.childrenValues.remove(childNode.data);

				// remove this node as a parent of the child
				childNode.parent = null;
				return true;
			}
		}
		
		return false;
	}
	
	/**
	 * Gets the root node of a tree
	 * @param node	The inputed node which is traversed to get the parent
	 * @return		The root node of the tree
	 */
	public TreeNode<T> getRootNode() {
		if(this.parent == null) {
			return this;
		}
		
		boolean foundRoot = false;
		TreeNode<T> rootNode = this;
		while(!foundRoot) {
			rootNode = rootNode.getParent();
			if(!rootNode.hasParent()) {
				foundRoot = true;
			}
		}
		return rootNode;
	}

	public boolean hasParent() {
		if(this.parent != null) {
			return true;
		}
		return false;
	}
	
	public boolean hasChildren() {
		if(this.children != null && this.children.size() > 0) {
			return true;
		}
		return false;
	}
	
	public TreeNode<T> getParent() {
		return parent;
	}

	public void setParent(TreeNode<T> parent) {
		this.parent = parent;
	}

	public List<TreeNode<T>> getChildren() {
		return children;
	}

	public void setChildren(List<TreeNode<T>> children) {
		this.children = children;
	}

	public Set<T> getChildrenValues() {
		return childrenValues;
	}

	public void setChildrenValues(Set<T> childrenValues) {
		this.childrenValues = childrenValues;
	}
}
