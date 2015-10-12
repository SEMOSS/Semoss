/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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

package prerna.ds;


import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutorService;

public class SimpleTreeBuilder
{
	// core set of issues I am trying to solve
	// the ability to add columns on the fly - Complete
	// ability to remove columns - Complete
	// ability to flatten it at any point and generate a table - Complete
	// Ability to add a single node  - Complete
	// ability to add columns with link to any of the existing columns - Complete
	// ability to remove a single node - Complete
	// Balance the tree automatically - need to see where to hook it in
	// stop flattening at an individual point - Done
	
	// 2 adjacent nodes from the metamodel are selected
	// The information is sent
	// Out of this, the key is from the existing node
	// The vector of all the SimpleTreeNodes are created based on where they are 
	// based on the nodes - we reach every leaf of this node then add the value node from the incoming hashtable to it
	// when a node is deleted, it is demarcated on the binary tree that this has been filtered out
	// with all the children of this node being deleted
	
	// What happens if I go with basically adding the node only to the last one
	// in this case, I dont need to worry about cloning the node multiple times
	// I need to check this out
	// and everytime there is a new one coming in
	// I need to flatten the existing structure and add to that one again
	// which means I need a way to fast search it
	// this is not difficult
	// I just need to keep the index and then add to it
	// so instead of adding a child
	// I just add to that node directly
	
	// distributed
	// What I am trying to do is
	// spray the nodes
	// so that I can load the data on a different machine if need be
	// any node can be a network node
	// SimpleNetworkNode extends SimpleTreeNode
	// TreeNetworkNode extends TreeNode
	// 
	
	
	// actual value tree root
	private SimpleTreeNode lastAddedNode = null;
	private SimpleTreeNode filteredRoot = null;
	private String rootLevel;
	// see if the child can be a single node
	// this means you can only add to the last child
	// nothing before it
	boolean singleNode = false;
	// type to the root node of the type
	// type is expresed as string
	Hashtable <String, TreeNode> nodeIndexHash = new Hashtable <String, TreeNode>();
	//Hashtable <String, Integer> typeToLevel = new Hashtable <String, Integer>();
	Hashtable <String, Hashtable> nodeTypeFilters = new Hashtable <String, Hashtable>();
	
	// hosting everything else for the thread runner
	//***************************
	ISEMOSSNode [] seeds = null;
	ILinkeableEngine [] engines = null;
	String [] childs = null;
	TreePhaser phaser = null;
	ArrayList threadList = null;
	int runLevel = 0;
	ExecutorService service = null;
	String finalChildType = null;
	//****************************
//	public SimpleTreeBuilder() {
//		
//	}

	public SimpleTreeBuilder(String rootLevel) {
		this.rootLevel = rootLevel;
	}
	
	public void adjustType(String type, boolean recurse)
	{
		// the point here is to ensure that the type is being adjusted so that the levels are taken care of
		// i.e. say if you extend one side of the tree, but however, the other side is really empty
		// you want to extend this in a balanced manner
		// consider Lab - CP / AP
		// now I extend CP, however AP is empty what this basically means is 
		// when I actually extend CP further or delete a node, there is a asymmetric tree growth which I am trying to avoid
		
		// we start of by picking all the instances for this type
		// then we methodically look to see if any of them have a left child
		// if they do
		// we then pick that type and create an empty node and attach it to those that do not have a children
		// the only word of caution is at a later point
		// when we want to add a node of the same type - we will need to ensure that the left child which is empty has been taken care of
		// I am not sure if we need to have to add this to TreeNode i.e. there is not a need to index this
		// actually I do, otherwise, it wont work.. :(
		
		// we start of by picking all the instances for this type
		TreeNode rootNodeForType = nodeIndexHash.get(type);
		if(rootNodeForType != null)
		{
			Vector nodeGetter = new Vector();
			nodeGetter.addElement(rootNodeForType);
			Vector <SimpleTreeNode> typeInstances = rootNodeForType.getInstanceNodes(nodeGetter, new Vector<SimpleTreeNode>());
			
			if(typeInstances == null)
				return;
			
			// then we methodically look to see if any of them have a left child
			// if they do
			// we then pick that type and create an empty node and attach it to those that do not have a children
			SimpleTreeNode child = null;
//			for(int instanceIndex = 0;instanceIndex < typeInstances.size() && (child == null || (child != null && child.leaf.getKey().equalsIgnoreCase(SimpleTreeNode.EMPTY)));instanceIndex++)
			for(int instanceIndex = 0;instanceIndex < typeInstances.size() && (child == null );instanceIndex++)
				child = typeInstances.elementAt(instanceIndex).leftChild;

			if(child == null) // nothing to do here 
				return; 
			// the only word of caution is at a later point
			// when we want to add a node of the same type - we will need to ensure that the left child which is empty has been taken care of
			String childType = ((ISEMOSSNode)child.leaf).getType();
			boolean newNode = false;
			for(int instanceIndex = 0;instanceIndex < typeInstances.size();instanceIndex++)
			{
				if(typeInstances.elementAt(instanceIndex).leftChild == null)
				{
					// reset it to something useful
					System.err.println("Adjusting... " + ((ISEMOSSNode)typeInstances.elementAt(instanceIndex).leaf).getType() + " Adding " + type + "  Child Type  " + childType);
					StringClass dummySEMOSSNode = new StringClass(SimpleTreeNode.EMPTY, SimpleTreeNode.EMPTY, childType);
					TreeNode dummyIndexNode = getNode(dummySEMOSSNode);
					if(dummyIndexNode == null) {
						dummyIndexNode = createNode(dummySEMOSSNode, true);
						// do the create routine here
						TreeNode root = nodeIndexHash.get(childType);
						root = root.insertData(dummyIndexNode);
						nodeIndexHash.put(childType, root);
					}
					else
					{
						SimpleTreeNode instanceNode = new SimpleTreeNode(dummySEMOSSNode);
						dummyIndexNode.addInstance(instanceNode);
					}
					typeInstances.elementAt(instanceIndex).addChild(dummyIndexNode.instanceNode.lastElement());
					
					newNode = true;
					//typeInstances.elementAt(instanceIndex).leftChild = dummyIndexNode.instanceNode.lastElement(); // set it to the last added one
				}
				// if the child is null and if it is not the same type
				// then insert a node here
				// not sure if we will ever do this, but
				// just setting it up
			}
			if(recurse && newNode)
				adjustType(childType, recurse);
		}			
	}
	
	public void balanceLevel(String level) {
		TreeNode rootNode = nodeIndexHash.get(level);
		if(rootNode != null)
		{
			String parentLevel = ((ISEMOSSNode)rootNode.instanceNode.firstElement().parent.leaf).getType();
			Vector nodeGetter = new Vector();
			
			TreeNode parent = nodeIndexHash.get(parentLevel);
			nodeGetter.addElement(parent);
			Vector <SimpleTreeNode> typeInstances = parent.getInstanceNodes(nodeGetter, new Vector<SimpleTreeNode>());

			for(int i = 0; i < typeInstances.size(); i++) {
				SimpleTreeNode parentNode = typeInstances.get(i);
				
				if(parentNode.leftChild==null) {
					
				}
				StringClass dummySEMOSSNode = new StringClass(SimpleTreeNode.EMPTY, level);
				TreeNode dummyIndexNode = getNode(dummySEMOSSNode);
				if(dummyIndexNode == null) {
					dummyIndexNode = createNode(dummySEMOSSNode, true);
					TreeNode root = nodeIndexHash.get(level);
					root = root.insertData(dummyIndexNode);
					nodeIndexHash.put(level, root);
				}

				else
				{
					SimpleTreeNode instanceNode = new SimpleTreeNode(dummySEMOSSNode);
					parentNode.leftChild = instanceNode;
					instanceNode.parent = parentNode;
					dummyIndexNode.addInstance(instanceNode);
				}
			}
		}
	}

	
	// I need to introduce a routine here for multi insert
	public TreeNode createNode(ISEMOSSNode node, boolean child)
	{
		TreeNode retNode = null;
		TreeNode typeIndexRoot = nodeIndexHash.get(node.getType());
		boolean newNode = false;
		boolean hasChild = false;
		retNode = getNode(node);
		if(retNode != null)
			hasChild = hasChild(retNode);
		if(retNode == null)
		{
			retNode = new TreeNode(node);
			newNode = true;
		}
		if(newNode || (child && !hasChild && !singleNode))
		{
			//System.err.println(node.getKey()+ "    Has child is " + hasChild + "   newNode" + newNode);
			//retNode = new TreeNode(node);
			SimpleTreeNode instanceNode = new SimpleTreeNode(node);
			//System.err.println("Adding instance " + node.getKey());
			retNode.addInstance(instanceNode);
			// add it as a sibling
			// if the child is false
			// and the type index root is not null
			// and the instances do not have a parent
			// add this guy as a sibling
			if(newNode && !child &&  typeIndexRoot != null && typeIndexRoot.getInstances() != null && typeIndexRoot.getInstances().elementAt(0).parent == null)
			{
				SimpleTreeNode rightMostNode = typeIndexRoot.getInstances().elementAt(0);
				//rightMostNode = SimpleTreeNode.getRight(rightMostNode);
				SimpleTreeNode rightSibling = rightMostNode.rightSibling;
				if(rightSibling != null) {
					rightSibling.leftSibling = instanceNode;
					instanceNode.rightSibling = rightSibling;
				}
				rightMostNode.rightSibling = instanceNode;
				instanceNode.leftSibling = rightMostNode;
			}
			
			//if it is a new node and it is not a child and nodeIndexHash size is 0
			if(newNode && !child && nodeIndexHash.keySet().size()==0) {
				finalChildType = node.getType();
			}
		}
		addToNodeIndex(node.getType(), retNode);
		return retNode;
	}
	
	public boolean hasChild(TreeNode node)
	{
		boolean hasChild = false;
		Vector <SimpleTreeNode> instances = node.getInstances();
		for(int instanceIndex = 0;instanceIndex < instances.size()&& !hasChild ;instanceIndex++)
			hasChild = (instances.get(instanceIndex).leftChild != null);
		return hasChild;
	}

	
//********************* ADDITION METHODS ****************************//
	
	// this is for adding more than one pair of nodes at a time
	// the node array can be thought of as a row in a table--one node for each type
	// this assumes that the node array passed in spans the whole width of the table--every type gets a node
	public synchronized SimpleTreeNode addNodeArray(ISEMOSSNode... nodeArray)
	{
		SimpleTreeNode leafNode = null;
		// this case is when data needs to be added to every node type and the full array of nodes is important
		// I cannot consider each pair in the array individually as once I get to the second pair, I lose it's relationship to the first node
		// need to keep track of the value nodes I create as I go through the array to only append to the value node I care about
		ISEMOSSNode finalNode = nodeArray[nodeArray.length-1];
		if(finalChildType == null || !nodeIndexHash.containsKey(finalNode.getType()))
			finalChildType = finalNode.getType();		

		SimpleTreeNode parentInstanceNode = createNode(nodeArray[0], false).getInstances().lastElement();
		for(int nodeIdx = 0; nodeIdx < nodeArray.length - 1; nodeIdx++){
			// check if our relationship already exists -- only need to check the last element because we are adding a continuous row so only care about the instance we just added (or got)
			ISEMOSSNode childSEMOSSNode = nodeArray[nodeIdx+1];
			boolean childExists = parentInstanceNode.hasChild(childSEMOSSNode);
			if (childExists){
				//how do i get this child instance node???
				TreeNode childIndexNode = getNode(childSEMOSSNode);
				List<SimpleTreeNode> childInstances = childIndexNode.getInstances();
				boolean foundNode = false;
				for(int childIdx = childInstances.size() - 1; childIdx >=0 && !foundNode; childIdx -- ){ // start from the back because its likely to be the last one
					SimpleTreeNode childInst = childInstances.get(childIdx);
					if(childInst.parent.equal(parentInstanceNode)){
						parentInstanceNode = childInst;
						foundNode = true;
					}
				}
				continue;
			}
			
			// if relationship doesn't exist, need to create a new child instance
			TreeNode retNode = getNode(childSEMOSSNode);
			if(retNode == null)
			{
				retNode = new TreeNode(childSEMOSSNode);
			}
			SimpleTreeNode childInstanceNode = new SimpleTreeNode(childSEMOSSNode);
			leafNode = childInstanceNode;
			retNode.addInstance(childInstanceNode);
			
			SimpleTreeNode.addLeafChild(parentInstanceNode, childInstanceNode);
			addToNodeIndex(childSEMOSSNode.getType(), retNode);

			lastAddedNode = parentInstanceNode;
			parentInstanceNode = childInstanceNode;
		}
		return leafNode;
	}

	// parent node
	// and the child node
	// adds the child node to the instances

	public synchronized void addNode(ISEMOSSNode parentNode, ISEMOSSNode node)
	{
		// this case is when data needs to be added to the lower most node
		// consider the case of Modify Referrals - Patient ID and to this I need to add Eligibility
		// I send in Modify Referrals, Eligibility
		// this needs to navigate down to the lower most and then add it to the lower most
		// set the final child type
		if(finalChildType == null || !nodeIndexHash.containsKey(node.getType()))
			finalChildType = node.getType();		
		TreeNode parentIndexNode = createNode(parentNode, false);
		
		// before all of this, I need to find if this parent already has this node
		
		
		Vector <SimpleTreeNode> parentInstances = parentIndexNode.getInstances();
		
		boolean childExists = false;	
		
		for(int instanceIndex = 0;instanceIndex < parentInstances.size() && !childExists;instanceIndex++)
		{
			SimpleTreeNode parentInstanceNode = parentInstances.elementAt(instanceIndex);
			childExists = parentInstanceNode.hasChild(node);
		}		
		
		if(childExists)
			return;	
		
		for(int instanceIndex = 0;instanceIndex < parentInstances.size();instanceIndex++)
		{
			SimpleTreeNode parentInstanceNode = parentInstances.elementAt(instanceIndex);
			// add at the same level
			if((parentInstanceNode.leftChild == null) || (parentInstanceNode.leftChild != null && ((ISEMOSSNode)parentInstanceNode.leftChild.leaf).getType().equalsIgnoreCase(node.getType())))
			{
				// logic of adding it to every child
				TreeNode childIndexNode = createNode(node, true);
				SimpleTreeNode childInstanceNode = childIndexNode.getInstances().lastElement();
				//System.err.println("Landed into this logic " + parentInstanceNode.leaf.getKey());
				//synchronized (parentNode) 
				{
				SimpleTreeNode.addLeafChild(parentInstanceNode, childInstanceNode);
				}
				lastAddedNode = parentInstanceNode;	
			}
			// else go down one level and then add it
			else if((parentInstanceNode.leftChild != null && !((ISEMOSSNode)parentInstanceNode.leftChild.leaf).getType().equalsIgnoreCase(node.getType())))
			{
				//System.err.println("Skipping this " + parentNode.getKey() + "<<>> " + node.getKey());
				SimpleTreeNode daNode = parentInstanceNode.leftChild;
				while(daNode != null)
				{
					//System.err.println("Da Node is " + daNode.leaf.getKey());
					
					addLeafNode(daNode, node);
					daNode = daNode.rightSibling;	
				}
				//SimpleTreeNode.addLeafChild(parentInstanceNode, childInstanceNode);
			}
		}		
	}
	
	/**
	 * 
	 * */
	public void append(SimpleTreeNode node, SimpleTreeNode node2merge) {
		//Recursive Algorithm for merging
		//Use in the case node and node2merge come from structurally identical Simple Trees
		//assumes right child does not matter/get used in a SimpleTree
		
		/*
		 * Need to adapt  this to also take into account join
		 * empty node checks 
		 * null checks
		 * different types of appends
		 * append with duplicates, without duplicates, etc.
		 * */
		
		SimpleTreeNode rightNode = SimpleTreeNode.getRight(node);
		SimpleTreeNode leftNode2Merge = SimpleTreeNode.getLeft(node2merge);
		SimpleTreeNode rightMergeSibling = null;
		while(leftNode2Merge!=null) {
			//find if the node exists in the tree node
			TreeNode tn = this.getNode((ISEMOSSNode)leftNode2Merge.leaf);
			
			//If the node doesn't exist in the tree, take the node, sever connection from right sibling and add node to level, repeat for right sibling
			if(tn == null) {
				//Add to the Value Tree
				rightNode.rightSibling = leftNode2Merge;
				leftNode2Merge.leftSibling = rightNode;
				rightMergeSibling = leftNode2Merge.rightSibling;
				
				leftNode2Merge.rightSibling = null;
				rightNode = rightNode.rightSibling;
				
				//Update the Index Tree
				appendToIndexTree(leftNode2Merge);
			} 
			//If the node does exist in the tree, determine if the node exists on this branch
			else {				
				List<SimpleTreeNode> instanceList = tn.getInstances();
				SimpleTreeNode equivalentInstance = null;
				
				boolean foundNode = false;
				SimpleTreeNode instance = null;
				SimpleTreeNode mergeInstance;
				for(int i = 0; i < instanceList.size(); i++) {
					instance = instanceList.get(i);
					equivalentInstance = instance;
					mergeInstance = leftNode2Merge; 
					while(!foundNode && instance.equal(mergeInstance)){
						if(instance.parent==null) {
							foundNode = true; 
							break;
						}
						instance = instance.parent;
						mergeInstance = mergeInstance.parent;
					}
					if(foundNode) {
						break;
					}
				}
				//If the node exists on the branch, append the children
				if(foundNode) { 
//					equivalentInstance = instance; 
					// there is a child, recursively go through method with subset
					if(equivalentInstance.leftChild != null) {
						append(equivalentInstance.leftChild, leftNode2Merge.leftChild);
					} 
					// if no children, add new node children to found instance
//					else if(leftNode2Merge.leftChild != null){ 
//						equivalentInstance.leftChild = leftNode2Merge.leftChild;
//						leftNode2Merge.parent = equivalentInstance;
//					}
					// continue for right siblings
					rightMergeSibling = leftNode2Merge.rightSibling;
				} 
				//if the node doesn't exist on the branch simply add it
				else {
					//Add to the Value Tree
					rightNode.rightSibling = leftNode2Merge;
					leftNode2Merge.leftSibling = rightNode;
					rightMergeSibling = leftNode2Merge.rightSibling;
					
					leftNode2Merge.rightSibling = null;
					rightNode = rightNode.rightSibling;

					//Recursively Update the Index Tree with leftNode2Merge and it's children
					appendToIndexTree(leftNode2Merge); 
				}
				//append(equivalentInstance.leftChild, leftNode2Merge.leftChild);
			}
			leftNode2Merge = rightMergeSibling;
		}		
	}
	
	/**
	 * This method recursively adds a SimpleTreeNode and it's children to the appropriate index trees
	 * 
	 * */
	void appendToIndexTree(SimpleTreeNode node) {

		if(node == null) {
			return;
		} else if(node.leaf == null) {
			throw new IllegalArgumentException("node has null leaf");
		}
		
		ISEMOSSNode n = (ISEMOSSNode) node.leaf;
		TreeNode rootIndexNode = nodeIndexHash.get(n.getType());
		
		//If the index tree for the level does not exist, add it
		if(rootIndexNode==null) {
			rootIndexNode = new TreeNode(n);
			rootIndexNode.instanceNode.add(node);
			nodeIndexHash.put(n.getType(), rootIndexNode);
		
			appendToIndexTree(node.leftChild);
			appendToIndexTree(node.rightSibling);
			return;
		

		} else {

			//loop through node and all siblings
			while(node != null) {
				TreeNode newNode = getNode( (ISEMOSSNode)node.leaf);
				// search first
				if(newNode == null) {
					// if not found 
					// create new node and set instances vector to the new value node and update the new root
					newNode = new TreeNode(node.leaf);
					newNode.addInstance(node);
					TreeNode newRoot = rootIndexNode.insertData(newNode);
					nodeIndexHash.put(n.getType(), newRoot);
					rootIndexNode = newRoot;
	
				} else {
					// if found add instance to existing TreeNode 
					newNode.getInstances().add(node);
				}
				
				if(node.leftChild != null) {
					appendToIndexTree(node.leftChild);
				}
				node = node.rightSibling;
			}
		}
	}
	
	void appendToFilteredIndexTree() {
		
	}
	
	//This method adds a 'node' vs appendToIndexTree which adds a 'tree'
	public void addToNodeIndex(String nodeType, TreeNode node)
	{
		TreeNode root = node;
		//System.err.println("Adding type " + nodeType + " <> " + node.leaf.getKey());
		if(nodeIndexHash.containsKey(nodeType))
		{
			// need to search first to make sure this node is not there
			root = nodeIndexHash.get(nodeType);
			Vector<TreeNode> searcher = new Vector<TreeNode>();
			searcher.add(root);
			if(!root.search(searcher, node, false))
			{
				root = nodeIndexHash.get(nodeType).insertData(node);
			}
		}
		nodeIndexHash.put(nodeType, root);
	}
	
	public void addLeafNode(SimpleTreeNode parentNode, ISEMOSSNode node)
	{
		if((parentNode.leftChild == null) || (parentNode.leftChild != null && ((ISEMOSSNode)parentNode.leftChild.leaf).getType().equalsIgnoreCase(node.getType())))
		{
			// logic of adding it to every child
			TreeNode childIndexNode = createNode(node, true);
			SimpleTreeNode childInstanceNode = childIndexNode.getInstances().lastElement();
			//System.err.println("Landed into this logic " + parentNode.leaf.getKey());
			//synchronized(parentNode)
			{
				SimpleTreeNode.addLeafChild(parentNode, childInstanceNode);
			}
			lastAddedNode = parentNode;
		}
		else if((parentNode.leftChild != null && !((ISEMOSSNode)parentNode.leftChild.leaf).getType().equalsIgnoreCase(node.getType())))
		{
			//System.err.println("Skipping this " + parentNode.leaf.getKey() + "<<>> " + node.getKey());
			SimpleTreeNode daNode = parentNode.leftChild;
			while(daNode != null)
			{
				//System.err.println("Da Node is " + daNode.leaf.getKey());
				addLeafNode(daNode, node);
				daNode = daNode.rightSibling;
			}
			//SimpleTreeNode.addLeafChild(parentInstanceNode, childInstanceNode);
		}
	}


//********************* END ADDITION METHODS ****************************//

	
	
	
//********************* REDUCTION METHODS ****************************//
	
	
	public void quickRefresh() {
		Set<String> levels = this.nodeIndexHash.keySet();
		for(String level: levels){
			this.quickRefresh(level);
		}
	}
	
	public void quickRefresh(String level) {
		TreeNode root = nodeIndexHash.get(level);
		if(root != null) {
			TreeNode newRoot = TreeNode.refresh(root, false);
			nodeIndexHash.put(level, newRoot);
		}
	}
	
	public void refresh() {
		Set<String> levels = this.nodeIndexHash.keySet();
		for(String level: levels){
			this.refresh(level);
		}
	}
	
	public void refresh(String level) {
		TreeNode root = nodeIndexHash.get(level);
		if(root != null) {
			TreeNode newRoot = TreeNode.refresh(root, true);
			nodeIndexHash.put(level, newRoot);
		}
	}
	
	/**
	 * 
	 * @param level
	 * @param height - distance from @param to leaf level in the simple tree
	 * 
	 * removes branches that do not reach to the leaf level
	 */
	public void removeBranchesWithoutMaxTreeHeight(String level, int height) {
		TreeNode rootNodeForType = nodeIndexHash.get(level);
		if(rootNodeForType == null) return;
		
		ValueTreeColumnIterator it = new ValueTreeColumnIterator(rootNodeForType);
		while(it.hasNext()) {
			SimpleTreeNode nextNode = it.next();
			removeEmptyRows(nextNode, 0, height, true);
		}
	}
	
	private void removeEmptyRows(SimpleTreeNode n, int start, int height, boolean first) {
		if(start < height-1) {
			
//			if(n.rightSibling!=null && !first) {
//				removeEmptyRows(n.rightSibling, start, height, false);
//			}
//			
			while(n!= null) {
				SimpleTreeNode rightSibling = n.rightSibling;
				if (n.leftChild == null) {
					//remove this node, and go up the tree
					while(n!=null) {
						SimpleTreeNode parentNode = n.parent;
						removeFromIndexTree(n);
						SimpleTreeNode.deleteNode(n);
						n = parentNode;
						if(n != null && n.leftChild != null) {
	                        break;
						}
					}
				} else {
					SimpleTreeNode child = n.leftChild;
					removeEmptyRows(child, start+1, height, false);
				}
				
				n = rightSibling;
			}
		}
	}

	/**
	 * 
	 * @param type
	 */
	public void removeType(String type)	{
		
		if(!nodeIndexHash.containsKey(type))
			return;
		
		//Determine if the column is a root column
		TreeNode typeRoot = nodeIndexHash.get(type);
		if(typeRoot==null) {
			nodeIndexHash.remove(type);
			return;
		}
		
		ValueTreeColumnIterator getFirst = new ValueTreeColumnIterator(typeRoot, true);
		SimpleTreeNode typeInstanceNode = null;
		if(getFirst.hasNext()) {
			typeInstanceNode = getFirst.next();
			getFirst = null;
		}
		else {
			nodeIndexHash.remove(type);
			return;
		}
		
		SimpleTreeNode parent = typeInstanceNode.parent;
		
		if(parent != null) {
			String parentType = ((ISEMOSSNode)parent.leaf).getType();
			SimpleTreeNode FilteredNode = null;
			
			TreeNode parentTypeRoot = nodeIndexHash.get(parentType);
			Iterator<SimpleTreeNode> iterator = new ValueTreeColumnIterator(parentTypeRoot, true);
			while(iterator.hasNext()) {
				SimpleTreeNode parentNode = iterator.next();
				
				//Grab left child and remove from tree
				SimpleTreeNode nodeToDelete = parentNode.leftChild;
				parentNode.leftChild = null;
				
				//for each node in the leftChild
				while(nodeToDelete != null) {
					
					SimpleTreeNode grandChildNode = nodeToDelete.leftChild;
					if(grandChildNode != null) {					
						if(parentNode.leftChild == null) {
							parentNode.leftChild = grandChildNode;
						} else {
							SimpleTreeNode newLeftSibling = SimpleTreeNode.getRight(parentNode.leftChild);
							newLeftSibling.rightSibling = grandChildNode;
							grandChildNode.leftSibling = newLeftSibling;
						}
					}
					
					SimpleTreeNode filteredGrandChildNode = nodeToDelete.rightChild;
					if(filteredGrandChildNode != null) {
						if(parentNode.rightChild == null) {
							parentNode.rightChild = filteredGrandChildNode;
						} else {
							SimpleTreeNode newFilteredLeftSibling = SimpleTreeNode.getRight(parentNode.rightChild);
							newFilteredLeftSibling.rightSibling = filteredGrandChildNode;
							filteredGrandChildNode.leftSibling = newFilteredLeftSibling;
						}
					}	
					nodeToDelete = nodeToDelete.rightSibling;
				}
				SimpleTreeNode.setParent(parentNode.leftChild, parentNode);
				
				
				//DELETE THE RIGHT SIDE
				nodeToDelete = parentNode.rightChild;
				parentNode.rightChild = null;
				
				while(nodeToDelete != null) {
					
					SimpleTreeNode grandChildNode = nodeToDelete.leftChild;
					
					if(grandChildNode != null) {					
						if(parentNode.rightChild == null) {
							parentNode.rightChild = grandChildNode;
						} else {
							SimpleTreeNode newLeftSibling = SimpleTreeNode.getRight(parentNode.rightChild);
							newLeftSibling.rightSibling = grandChildNode;
							grandChildNode.leftSibling = newLeftSibling;
						}
					}
					
					SimpleTreeNode filteredGrandChildNode = nodeToDelete.rightChild;
					if(filteredGrandChildNode != null) {
						if(parentNode.rightChild == null) {
							parentNode.rightChild = filteredGrandChildNode;
						} else {
							SimpleTreeNode newFilteredLeftSibling = SimpleTreeNode.getRight(parentNode.rightChild);
							newFilteredLeftSibling.rightSibling = filteredGrandChildNode;
							filteredGrandChildNode.leftSibling = newFilteredLeftSibling;
						}
					}
					nodeToDelete = nodeToDelete.rightSibling;
				}
				SimpleTreeNode.setParent(parentNode.rightChild, parentNode);
				
			}
		}
		else // this is the case when the type is the root
		{
			typeInstanceNode = SimpleTreeNode.getLeft(typeInstanceNode);
			if(typeInstanceNode != null) {
				
				SimpleTreeNode leftMostNode = typeInstanceNode.leftChild;
				SimpleTreeNode rightMostSibling = SimpleTreeNode.getRight(leftMostNode);
				
				while(typeInstanceNode != null) {
					// need to make sure new root are all siblings
					
					SimpleTreeNode targetNode = typeInstanceNode.leftChild;
					if(targetNode != leftMostNode) { // this occurs for the first iteration
						rightMostSibling.rightSibling = targetNode;
						targetNode.leftSibling = rightMostSibling;
					}
					rightMostSibling = SimpleTreeNode.getRight(targetNode); // update the new most right sibling

					while(targetNode != null) {
						targetNode.parent = null;
						targetNode = targetNode.rightSibling;
					}
					
					typeInstanceNode = typeInstanceNode.rightSibling;
				}
			}
		}
		
		// if I take it off the main hashtable
		// it will get GC'ed. I am not sure I need to travel individually and do it
		nodeIndexHash.remove(type);
	}

	
	public void removeDuplicates(String type) {
		//Logic
		//for each branch, sort then compress
		if(!nodeIndexHash.containsKey(type)) return;

		SimpleTreeNode simpleTreeNode = new ValueTreeColumnIterator(nodeIndexHash.get(type)).next();
		boolean root = (simpleTreeNode.parent == null);
		//TODO : fix for now, make better
		if(root) {
			this.unfilterColumn(type);
		}
		ISEMOSSNode parentNode;
		
		Comparator<SimpleTreeNode> simpleTreeComparator = new Comparator<SimpleTreeNode>() {
			@Override
			public int compare(SimpleTreeNode n1, SimpleTreeNode n2) {
				if(n1.equal(n2)) {
					return 0;
				}
				else if(n1.left(n2)) {
					return -1;
				}
				else {
					return 1;
				}
			}
		};
		
		if(!root) {
			parentNode = (ISEMOSSNode)simpleTreeNode.parent.leaf;
			type = parentNode.getType();
		}
		
		List<SimpleTreeNode> branchList;
		ValueTreeColumnIterator iterator = new ValueTreeColumnIterator(nodeIndexHash.get(type), true);
		if(root) {
			branchList = new ArrayList<SimpleTreeNode>();
			while(iterator.hasNext()) {
				SimpleTreeNode nextNode = iterator.next();
				nextNode.rightSibling = null;
				nextNode.leftSibling = null;
				branchList.add(nextNode);
			}
			sortBranch(branchList, simpleTreeComparator, root);
		} else {
			while(iterator.hasNext()) {
				SimpleTreeNode node = iterator.next().leftChild;
				SimpleTreeNode nextNode;
				branchList = new ArrayList<SimpleTreeNode>();
				while(node != null) {
					nextNode = node.rightSibling;
					node.rightSibling = null;
					node.leftSibling = null;
					branchList.add(node);
					node = nextNode;
				}
				sortBranch(branchList, simpleTreeComparator, root);
			}
		}
	}
	
	private void sortBranch(List<SimpleTreeNode> branchList, Comparator<SimpleTreeNode> simpleTreeComparator, boolean root) {
		Collections.sort(branchList, simpleTreeComparator);
		
		boolean keepGoing = (branchList.size() > 1);
		int i = 0;
		while(keepGoing) {
			SimpleTreeNode n1 = branchList.get(i);
			SimpleTreeNode n2 = branchList.get(i+1);
			
			if(n1.equal(n2)) {
				consolidate(n1, n2);
				branchList.remove(i+1);
			} else {
				i++;
			}
			
			keepGoing = i < branchList.size() - 1;
			if(!keepGoing && !root) {
				SimpleTreeNode newLeftChild = branchList.get(0);
				SimpleTreeNode parent = newLeftChild.parent;
				parent.leftChild = newLeftChild;
			}
		}
		
//		if(root) {
			for(i = 0; i < branchList.size()-1; i++) {	
				SimpleTreeNode n1 = branchList.get(i);
				SimpleTreeNode n2 = branchList.get(i+1);
				n1.rightSibling = n2;
				n2.leftSibling = n1;
			}
//		}
	}
	
	private void consolidate(SimpleTreeNode node, SimpleTreeNode node2consolidate) {
		SimpleTreeNode nodeLeftChild = node.leftChild;
		SimpleTreeNode nodeRightChild = node.rightChild;
		
		SimpleTreeNode cNodeLeftChild = node2consolidate.leftChild;
		SimpleTreeNode cNodeRightChild = node2consolidate.rightChild;
		
		//Combine the left side
		if(nodeLeftChild == null && cNodeLeftChild != null) {
			
			SimpleTreeNode.setParent(cNodeLeftChild, node);
			node.leftChild = cNodeLeftChild;
		
		} else if(nodeLeftChild != null && cNodeLeftChild != null){
			
			SimpleTreeNode.setParent(cNodeLeftChild, node);
			nodeLeftChild = SimpleTreeNode.getRight(nodeLeftChild);
			nodeLeftChild.rightSibling = cNodeLeftChild;
			cNodeLeftChild.leftSibling = nodeLeftChild;
			
//			cNodeLeftChild.leftSibling = null;
//			SimpleTreeNode.getRight(cNodeLeftChild).rightSibling = nodeLeftChild;d
//			node.leftChild = cNodeLeftChild;
		
		}
			
		//Combine the right side
		if(nodeRightChild == null && cNodeRightChild != null) {
			
			SimpleTreeNode.setParent(cNodeRightChild, node);
			node.rightChild = cNodeRightChild;
			
		} else if(nodeRightChild != null && cNodeRightChild != null) {
		
			SimpleTreeNode.setParent(cNodeRightChild, node);
			nodeRightChild = SimpleTreeNode.getRight(nodeRightChild);
			nodeRightChild.rightSibling = cNodeRightChild;
			cNodeRightChild.leftSibling = nodeRightChild;
		
		}
		
		this.removeFromIndexTree(node2consolidate);
	}
	
	/**
	 * Finds the TreeNode in the index tree with the value of @param n, 
	 * and removes the instance of n from the index tree
	 * 
	 * @param n		The instance to be removed from the index tree
	 */
	private void removeFromIndexTree(SimpleTreeNode n) {
		TreeNode foundNode = this.getNode((ISEMOSSNode)n.leaf);
		if(foundNode!=null) {
			if(!foundNode.getInstances().remove(n)) {
				foundNode.filteredInstanceNode.remove(n);
			}
		}
	}
	
	/**
	 * Currently rebuilds Index Tree with remaining TreeNodes after deletion. Can implement rebalance as an option to clean up Index Tree.
	 * 
	 * @param level Column header
	 * @return TreeNode root node of rebuilt/rebalanced Index Tree
	 */
	public TreeNode refreshIndexTree(String level) {
		TreeNode refreshedNode = null;
		boolean isRebuild = true;
		
		ArrayList<TreeNode> nodeList = new ArrayList<TreeNode>();
		TreeNode root = this.nodeIndexHash.get(level);
		
		nodeList = deleteIndexFilters(root, isRebuild);
		
		if (isRebuild) {
			refreshedNode = rebuild(nodeList);
		} else {
			// TODO: implement rebalance under conditions where it would be faster to rebalance rather than rebuild Index Tree
		}
		
		return refreshedNode;
	}
	
	/**
	 * Removes all right children in the BTree: This hard deletes all filtered values. Works recursively. Also deletes filteredRoot to remove filtered root values.
	 * 
	 * @param root SimpleTreeNode from Value Tree that needs to have filtered values removed
	 */
	public void deleteFilteredValues(SimpleTreeNode node) {
		if (!node.hasChild()) {
			this.filteredRoot = null;
			return;
		}
		node.rightChild = null;
		if (node.leftChild != null) {
			if (node.leftChild.hasChild()) {
				deleteFilteredValues(node.leftChild);
			}
		}
		if (node.rightSibling != null) {
			deleteFilteredValues(node.rightSibling);
		}
		this.filteredRoot = null;
	}
	
	/**
	 * Gathers list of TreeNodes that will be used to either rebuild Index Tree or contain TreeNodes that must be deleted during rebalance
	 * 
	 * @param isRebuild
	 *            true if method is being used to rebuild index tree, false if used to rebalance index tree
	 * @return ArrayList of TreeNodes that will either be kept or deleted
	 */
	private ArrayList<TreeNode> deleteIndexFilters(TreeNode root, boolean isRebuild) {
		ArrayList<TreeNode> nodeList = new ArrayList<TreeNode>();
		
		CompleteIndexTreeIterator iterator = new CompleteIndexTreeIterator(root); // needs to iterate through logically deleted nodes, which IndexTreeIterator does not do currently
		while (iterator.hasNext()) {
			root = iterator.next();
			if (root.filteredInstanceNode.size() > 0) {
				root.filteredInstanceNode = new Vector<SimpleTreeNode>();
			}
			if (isRebuild) {
				if (root.instanceNode.size() > 0) {
					nodeList.add(root); // when rebuilding, we need nodes that will be kept
				}
			} else {
				if (root.instanceNode.size() == 0) {
					nodeList.add(root); // when rebalancing, we need nodes that will be deleted
				}
			}

		}
		return nodeList;
	}

	/**
	 * Rebuilds the Index Tree with the TreeNodes contained in keepList
	 * 
	 * @param keepList ArrayList containing TreeNodes that are to be kept in the new Index Tree
	 * @return TreeNode of new Index Tree
	 */
	private TreeNode rebuild(ArrayList<TreeNode> keepList) {
		TreeNode newRoot = keepList.get(0);
		newRoot.cleanNode();
		keepList.remove(0);
		
		for (TreeNode node : keepList) {
			node.cleanNode();
			newRoot = newRoot.insertData(node);
		}
		
		return newRoot;
	}

	/**
	 * Removes all rows containing specified ISEMOSSNode
	 * 
	 * @param node ISEMOSSNode of value to be deleted
	 */
	public void removeNode(ISEMOSSNode node)
	{
		String type = node.getType();
		if(nodeIndexHash.containsKey(type))
		{
			TreeNode rootNode = nodeIndexHash.get(type);
			TreeNode searchNode = new TreeNode(node); // TreeNode without the references that rootNode has; is this necessary?
			Vector<TreeNode> searchVector = new Vector<TreeNode>();
			searchVector.addElement(rootNode);
			TreeNode foundNode = rootNode.getNode(searchVector, searchNode, false); // can we just feed rootNode instead of searchNode?
			
			if(foundNode != null)
			{
				// get all the instances and delete
				Vector<SimpleTreeNode> instances = foundNode.getInstances();
				Vector<SimpleTreeNode> filteredInstances = foundNode.getFilteredInstances();
				
				boolean hasInstances = instances.size() > 0;
				boolean hasFilters = filteredInstances.size() > 0;
				
				if (hasInstances) {
					while (instances.size() > 0) {
						removeInstanceFromBTree(instances.get(0));
					}
				}
				if (hasFilters) {
					while (filteredInstances.size() > 0) {
						removeInstanceFromBTree(filteredInstances.get(0));
					}
				}
			}
		}
	}
	
	/**
	 * Removes value from both Value Tree and Index Tree
	 * 
	 * @param n SimpleTreeNode of value to be deleted
	 */
	private void removeInstanceFromBTree(SimpleTreeNode n) {
		SimpleTreeNode node = findParentWithSibling(n);
		removeAllChildrenFromBTree(node, true);
	}
	
	/**
	 * Searches up the Value Tree recursively until it has found a node with siblings or is at root level. Once found, the parent is removed from the Value Tree, along with all its children.
	 * 
	 * @param n SimpleTreeNode to start searching from
	 * @return most parent SimpleTreeNode
	 */
	private SimpleTreeNode findParentWithSibling(SimpleTreeNode n) {
		SimpleTreeNode rightSibling = n.rightSibling;
		SimpleTreeNode leftSibling = n.leftSibling;
		SimpleTreeNode parent = n.parent;
		
		boolean hasParent = parent != null;
		boolean hasRightSibling = rightSibling != null;
		boolean hasLeftSibling = leftSibling != null;
		
		// rewire siblings
		if (hasParent) {
			if (!hasLeftSibling && hasRightSibling) {
				parent.leftChild = rightSibling;
				rightSibling.leftSibling = null;
			} else {
				n = findParentWithSibling(parent); // node has no siblings; find first parent node that does or root
			}
		} else if (hasLeftSibling && hasRightSibling) { // in the middle of two nodes
			rightSibling.leftSibling = leftSibling;
			leftSibling.rightSibling = rightSibling;
		} else if (hasLeftSibling && !hasRightSibling) {
			leftSibling.rightSibling = null;
		} else { // neither parent nor left sibling, but has right sibling
			rightSibling.leftSibling = null;
		}
		
		return n;
	}
	
	/**
	 * Recursively goes through entire subtree and removes each SimpleTreeNode from the Index Tree
	 * 
	 * @param n
	 * @param isFirstNode
	 */
	private void removeAllChildrenFromBTree(SimpleTreeNode n, boolean isFirstNode) {
		SimpleTreeNode instanceChild = n.leftChild;
		SimpleTreeNode filteredChild = n.rightChild;
		SimpleTreeNode rightSibling = n.rightSibling;
		
		boolean hasInstances = instanceChild != null;
		boolean hasFilters = filteredChild != null;
		boolean hasRight = rightSibling != null;
		
		this.removeFromIndexTree(n);

		if (hasRight && !isFirstNode) {
			removeAllChildrenFromBTree(rightSibling, false);
		} else if (hasFilters) {
			removeAllChildrenFromBTree(filteredChild, false);
		}
		if (hasInstances) {
			removeAllChildrenFromBTree(instanceChild, false);
		}
	}
	
	
//*********************END REDUCTION METHODS ****************************//


	

	
//******************** FILTER METHODS **************************//
	
	
	public void filterTree(List<ITreeKeyEvaluatable> objectsToFilter) {
		for(ITreeKeyEvaluatable i: objectsToFilter) {
			filterTree(i);
		}
	}
	
	public void filterTree(ITreeKeyEvaluatable objectToFilter) {
		TreeNode foundNode = this.getNode((ISEMOSSNode)objectToFilter);
		if(foundNode != null) {
			List<SimpleTreeNode> nodeList = new ArrayList<SimpleTreeNode>();
			
			for(SimpleTreeNode n: foundNode.instanceNode) {
				nodeList.add(n);
			}
			
			for(SimpleTreeNode n: nodeList) {
				SimpleTreeNode filteredNode = filterSimpleTreeNode(n);
				filterTreeNode(filteredNode, true);
			}
			
			//foundNode.filteredInstanceNode.addAll(foundNode.instanceNode);
			//foundNode.instanceNode.clear();
		}
	}

	private SimpleTreeNode filterSimpleTreeNode(SimpleTreeNode node2filter) {
		final SimpleTreeNode returnNode = node2filter;
		//TODO: simplify logic
		SimpleTreeNode parentNode = node2filter.parent;
		boolean root = (parentNode == null);
		
		if(!root) {
			if(SimpleTreeNode.countNodeChildren(parentNode)==1) {
				return filterSimpleTreeNode(parentNode);
			}
		}
		
		SimpleTreeNode nodeRightSibling = node2filter.rightSibling;
		SimpleTreeNode nodeLeftSibling = node2filter.leftSibling;
		
		//isolate node2filter from siblings and rewire the connections
		if(node2filter.rightSibling != null && node2filter.leftSibling != null) {
			//in the middle
			nodeRightSibling.leftSibling = nodeLeftSibling;
			nodeLeftSibling.rightSibling = nodeRightSibling;	
		} 
		else if(node2filter.rightSibling == null && node2filter.leftSibling != null) {
			//right most
			nodeLeftSibling.rightSibling = null;
		} 
		else if(node2filter.rightSibling != null && node2filter.leftSibling == null) {
			//left most
			if(!root) {
				parentNode.leftChild = nodeRightSibling;
			}
			nodeRightSibling.leftSibling = null;
		} else {
			//only child
			if(!root) {
				parentNode.leftChild = null;
			}
		}
		
		node2filter.rightSibling = null;
		node2filter.leftSibling = null;
		
		//put the node2filter on the right side or attach to filtered root node if root
		if(root) {
			if(filteredRoot == null) {
				filteredRoot = node2filter;
			} else {
//				SimpleTreeNode rightFilteredNode = SimpleTreeNode.getRight(filteredRoot);
//				rightFilteredNode.rightSibling = node2filter;
//				node2filter.leftSibling = rightFilteredNode;
				
				filteredRoot.leftSibling = node2filter;
				node2filter.rightSibling = filteredRoot;
				filteredRoot = node2filter;
			}
		} else {
			if(parentNode.rightChild == null) {
				parentNode.rightChild = node2filter;
			} else {
//				SimpleTreeNode rightFilteredChild = parentNode.rightChild;
//				rightFilteredChild = SimpleTreeNode.getRight(rightFilteredChild);
//				rightFilteredChild.rightSibling = node2filter;
//				node2filter.leftSibling = rightFilteredChild;
				
				SimpleTreeNode rightFilteredChild = parentNode.rightChild;
				node2filter.leftSibling = rightFilteredChild;
				SimpleTreeNode nextRight = rightFilteredChild.rightSibling;
				rightFilteredChild.rightSibling = node2filter;
				if(nextRight != null) {
					nextRight.leftSibling = node2filter;
					node2filter.rightSibling = nextRight;
				}
			}
		}
		
		return returnNode;
	}
	
	private void filterTreeNode(SimpleTreeNode instance2filter, boolean firstLevel) {
		
		TreeNode foundNode = this.getNode((ISEMOSSNode)instance2filter.leaf);
		
		if(foundNode != null) {
		
			//Need to manually clear the instannceNode list for the first level to avoid concurrentModificationException
			//if(!firstLevel) {
				if(foundNode.instanceNode.remove(instance2filter)) {
					foundNode.filteredInstanceNode.add(instance2filter);
				}
			//}
			
			if(!firstLevel && instance2filter.rightSibling!=null) {
				filterTreeNode(instance2filter.rightSibling, false);
			}
			
			if(instance2filter.leftChild!=null) {
				filterTreeNode(instance2filter.leftChild, false);
			}
		}
	}
	
	public void unfilterColumn(String column) {
		
		//what happens when you try to filter out everything and unfilter?
		SimpleTreeNode root = this.getRoot();
		boolean check = true;
		if(root == null) {
			root = this.getFilteredRoot();
			check = false;
			if(root == null) return;
		}
		
		
		if(((ISEMOSSNode)(root.leaf)).getType().equals(column) && check) {
			if(filteredRoot != null) {
				///SimpleTreeNode root = this.getRoot();
				root = SimpleTreeNode.getRight(root);
				
				unfilterTreeNode(filteredRoot);
				root.rightSibling = filteredRoot;
				filteredRoot.leftSibling = root;
				
//				unfilterTreeNode(filteredRoot);
				filteredRoot = null;
			}
		} else if(((ISEMOSSNode)(root.leaf)).getType().equals(column) && !check) {
			unfilterTreeNode(filteredRoot);
			filteredRoot = null;
		} else {
			
			TreeNode unfilterIndexTree = nodeIndexHash.get(column);
			
			ValueTreeColumnIterator it = new ValueTreeColumnIterator(unfilterIndexTree, true);
			ITreeKeyEvaluatable l;
			if(it.hasNext()) {
				l = it.next().parent.leaf;
			} else {
				return;
			}

			ISEMOSSNode parent = (ISEMOSSNode)l;
			String parentType = parent.getType();
			
			TreeNode parentIndexTree = nodeIndexHash.get(parentType);
			ValueTreeColumnIterator iterator = new ValueTreeColumnIterator(parentIndexTree);
		

			while(iterator.hasNext()) {
				SimpleTreeNode simpleTree = iterator.next();
				
				if(simpleTree.rightChild!=null) {
					SimpleTreeNode leftChild = simpleTree.leftChild;
					SimpleTreeNode rightChild = simpleTree.rightChild;
					
					unfilterTreeNode(simpleTree.rightChild);
					
					if(leftChild==null) {
						simpleTree.leftChild = rightChild;
					} else {
						SimpleTreeNode rightMostLeftChild = SimpleTreeNode.getRight(leftChild);
						rightMostLeftChild.rightSibling = rightChild;
						rightChild.leftSibling = rightMostLeftChild;
					}
					simpleTree.rightChild = null;
				}
			}
			
			FilteredValueTreeColumnIterator fiterator = new FilteredValueTreeColumnIterator(unfilterIndexTree);
			while(fiterator.hasNext()) {
				SimpleTreeNode unfilteredNode = unfilterSimpleTreeNode(fiterator.next());
				if(unfilteredNode != null) {
					unfilterTreeNode(unfilteredNode);
				}
			}
		}
	}
	
	private SimpleTreeNode unfilterSimpleTreeNode(SimpleTreeNode node) {
		SimpleTreeNode parentNode = node.parent;
		boolean root = (parentNode==null);
		
		if(!root) {
			if(SimpleTreeNode.countNodeChildren(parentNode)==1) {
				return unfilterSimpleTreeNode(parentNode);
			} else {
				
				SimpleTreeNode nodeRightSibling = node.rightSibling;
				SimpleTreeNode nodeLeftSibling = node.leftSibling;
				
				//isolate node2filter from siblings and rewire the connections
				if(node.rightSibling != null && node.leftSibling != null) {
					//in the middle
					nodeRightSibling.leftSibling = nodeLeftSibling;
					nodeLeftSibling.rightSibling = nodeRightSibling;	
				} 
				else if(node.rightSibling == null && node.leftSibling != null) {
					//right most
					nodeLeftSibling.rightSibling = null;
				} 
				else if(node.rightSibling != null && node.leftSibling == null) {
					//left most
					if(!root) {
						parentNode.rightChild = nodeRightSibling;
					}
					nodeRightSibling.leftSibling = null;
				} else {
					//only child
					parentNode.leftChild = null;
				}
				
				node.rightSibling = null;
				node.leftSibling = null;
				
				if(parentNode.leftChild==null) {
					parentNode.leftChild = node;
				} else {
					SimpleTreeNode rightMostLeftChild = SimpleTreeNode.getRight(node);
					rightMostLeftChild.rightSibling = node;
					node.leftSibling = rightMostLeftChild;
				}
				
				if(this.isFiltered(parentNode)) {
					return null;
				}
				else {
					return parentNode;
				}
			}
		} 
		else {
			//in the case the node is a root
			
			SimpleTreeNode rightSibling = node.rightSibling;
			SimpleTreeNode leftSibling = node.leftSibling;
			
			if(rightSibling!=null && leftSibling != null) {
				//middle
				leftSibling.rightSibling = rightSibling;
				rightSibling.leftSibling = leftSibling;
			} else if(rightSibling==null && leftSibling != null) {
				//right most
				leftSibling.rightSibling = null;
			} else if(leftSibling==null && rightSibling != null) {
				//left most
				filteredRoot = rightSibling;
				rightSibling.leftSibling = null;
			} else {
				//only one
				filteredRoot = null;
			}
			
			node.rightSibling = null;
			node.leftSibling = null;
			
			SimpleTreeNode rootNode = this.getRoot();
			rootNode = SimpleTreeNode.getRight(rootNode);
			rootNode.rightSibling = node;
			node.leftSibling = rootNode;
			
			return null;
		}
	}
	
	private void unfilterTreeNode(SimpleTreeNode instance) {
		TreeNode foundNode = this.getNode((ISEMOSSNode)instance.leaf);
		
		if(foundNode != null) {
			//This should never return false
			if(foundNode.filteredInstanceNode.remove(instance)) {
				foundNode.instanceNode.add(instance);
			}
			
			if(instance.rightSibling!=null) {
				unfilterTreeNode(instance.rightSibling);
			}
			if(instance.leftChild!=null) {
				unfilterTreeNode(instance.leftChild);
			}
		}
	}

	public boolean isFiltered(SimpleTreeNode node) {
		TreeNode tnode = this.getNode(((ISEMOSSNode)node.leaf));
		boolean filtered = false;
		if(tnode.filteredInstanceNode.contains(node)) {
			filtered = true;
		}
		return filtered;
	}
	
	
//******************** END FILTER METHODS ************************//
	

//******************** GETTER METHODS ****************************//
	
	//TODO: make this better, not sure if lastAddedNode is reliable
	public SimpleTreeNode getRoot() {
		TreeNode root = nodeIndexHash.get(this.rootLevel);
		
		ValueTreeColumnIterator it = new ValueTreeColumnIterator(root);
		SimpleTreeNode rootNode = null;
		if(it.hasNext()) {
			rootNode = it.next();
			return SimpleTreeNode.getLeft(rootNode);
		} else {
			return null;
		}
	}
	
	public SimpleTreeNode getFilteredRoot() {
		return filteredRoot;
	}
	
	public int getNodeTypeCount(String nodeType) {
		int count = 0;
		TreeNode typeRoot = nodeIndexHash.get(nodeType);
		IndexTreeIterator it = new IndexTreeIterator(typeRoot);
		while(it.hasNext()) {
			it.next();
			count++;
		}
		return count;
	}
	
	
	public TreeNode getNode(ISEMOSSNode node)
	{
		TreeNode typeIndexRoot = nodeIndexHash.get(node.getType());
		TreeNode retNode = null;
		if(typeIndexRoot != null)
		{
			if(typeIndexRoot.leaf.isEqual(node)) {
				return typeIndexRoot;
			}
			else {
				Vector <TreeNode> rootNodeVector = new Vector<TreeNode>();
				rootNodeVector.add(typeIndexRoot);
				// find the node which has
				retNode = typeIndexRoot.getNode(rootNodeVector, new TreeNode(node), false);
			}
		}
		return retNode;
	}
	
	public Vector <String> findLevels()	{
		
		//if(finalChildType == null) return null;

		Vector <String> retVector = new Vector<String>();
		
		TreeNode aNode = nodeIndexHash.get(rootLevel);
		SimpleTreeNode sNode = new ValueTreeColumnIterator(aNode).next();//aNode.instanceNode.elementAt(0);
		while(sNode != null)
		{
			retVector.add(((ISEMOSSNode)sNode.leaf).getType());
			sNode = sNode.parent;
		}
		Collections.reverse(retVector);
		
		sNode = new ValueTreeColumnIterator(aNode).next().leftChild;
		while(sNode != null) {
			retVector.add(((ISEMOSSNode)sNode.leaf).getType());
			sNode = sNode.leftChild;
		}
		return retVector;
	}
	
	// gets all the semoss nodes i.e. the leafs within the simple tree node
	public Vector<ISEMOSSNode> getSInstanceNodes(String type)
	{
		TreeNode typeRoot = nodeIndexHash.get(type);
		Vector <ISEMOSSNode> retVector = new Vector<>();
		
		Iterator<SimpleTreeNode> it = new ValueTreeColumnIterator(typeRoot);
		while(it.hasNext()) {
			retVector.add((ISEMOSSNode)it.next().leaf);
		}
		return retVector;
	}
	
	public void setRootLevel(String root) {
		this.rootLevel = root;
	}
	
//	public Hashtable<SimpleTreeNode, Vector<SimpleTreeNode>> getPath(String fromType, String toType)
//	{
//		TreeNode typeRoot = nodeIndexHash.get(fromType);
//		Vector <TreeNode> searchVector = new Vector<TreeNode>();
//		searchVector.add(typeRoot);
//		// I need to write the logic to walk the tree and get all instances
//		// gets all the instances
//		Vector <SimpleTreeNode> allInstanceVector = typeRoot.getInstanceNodes(searchVector, new Vector<SimpleTreeNode>());
//		
//		Hashtable<SimpleTreeNode, Vector<SimpleTreeNode>> values = new Hashtable<SimpleTreeNode, Vector<SimpleTreeNode>>();
//		for(int instanceIndex = 0;instanceIndex < allInstanceVector.size();instanceIndex++)
//		{
//			
//			Vector <SimpleTreeNode> instanceVector = new Vector<SimpleTreeNode>();
//			SimpleTreeNode fromNode = allInstanceVector.elementAt(instanceIndex);
//			instanceVector.addElement(fromNode);
//			//SimpleTreeNode instanceNode = typeRoot.getInstances().elementAt(0);
//			//instanceNode = instanceNode.getLeft(instanceNode);
//			//instanceVector.add(instanceNode);
//			//Vector output = typeRoot.getInstanceNodes(rootVector, new Vector());
//			Vector <SimpleTreeNode> output = SimpleTreeNode.getChildsOfType(instanceVector, toType, new Vector<SimpleTreeNode>());
//			values.put(fromNode, output);
//			
//			// need to remove the duplicates
//			System.out.println("Total Number of instances are " + output.size() + output.elementAt(0).leaf.getKey());
//		}
//		System.out.println("Output values is " + values);
//		return values;
//	}

//******************** UNUSED METHODS **********************//
	public void addFilter(String nodeType, String filter)
	{
		Hashtable filters = new Hashtable();
		if(nodeTypeFilters.containsKey(nodeType))
			filters = nodeTypeFilters.get(nodeType);
		
		// put it in
		filters.put(filter, filter);
		
		// set the filter back
		nodeTypeFilters.put(nodeType, filters);
	}

	// add a filter so that it is not kept when flattening is done
	public void removeFilter(String nodeType, String filter)
	{
		Hashtable filters = new Hashtable();
		if(nodeTypeFilters.containsKey(nodeType))
			filters = nodeTypeFilters.get(nodeType);
		
		// put it in
		filters.remove(filter); //, filter);
		
		// set the filter back
		nodeTypeFilters.put(nodeType, filters);
	}
	
	// flatten for a particular type
	public Vector flattenFromType(String type)
	{
		// pick the type root from the index
		// Run through every node and add the instances to a vector
		// push to flatten roots on the SimpleTreeNode and flatten it
		System.out.println("Calling Flatten");
		TreeNode typeRoot = nodeIndexHash.get(type);
		SimpleTreeNode instanceNode = typeRoot.getInstances().elementAt(0);
		instanceNode = instanceNode.getLeft(instanceNode);
		Vector <SimpleTreeNode> rootVector = new Vector<SimpleTreeNode>();
		rootVector.add(instanceNode);
		//instanceNode.printNode(rootVector, true, 1);
		// the flatten takes the following arguments
		//the node / parent to flatten from, the output vector that will have the values
		// filter hashtable - optional
		Vector outputVector = new Vector();
		if(nodeTypeFilters.size() == 0)
			instanceNode.flattenRoots(instanceNode.getLeft(instanceNode),outputVector);
		else
			instanceNode.flattenRoots(instanceNode.getLeft(instanceNode),outputVector, nodeTypeFilters);			
		
		//Vector output = typeRoot.getInstanceNodes(rootVector, new Vector());
		//System.out.println("Total Number of instances are " + output.size());
		return null;
	}

	// flatten for a particular type .. until a particular type
	public Vector flattenFromType(String type, String untilType)
	{
		// pick the type root from the index
		// Run through every node and add the instances to a vector
		// push to flatten roots on the SimpleTreeNode and flatten it
		TreeNode typeRoot = nodeIndexHash.get(type);
		SimpleTreeNode instanceNode = typeRoot.getInstances().elementAt(0);
		instanceNode = instanceNode.getLeft(instanceNode);
		Vector <SimpleTreeNode> rootVector = new Vector<SimpleTreeNode>();
		rootVector.add(instanceNode);
		//instanceNode.printNode(rootVector, true, 1);
		Vector outputVector = new Vector();
		if(nodeTypeFilters.size() == 0)
			instanceNode.flattenRoots(instanceNode, untilType, outputVector);
		else
			instanceNode.flattenRoots(instanceNode, untilType, outputVector, nodeTypeFilters);
		//Vector output = typeRoot.getInstanceNodes(rootVector, new Vector());
		//System.out.println("Total Number of instances are " + output.size());
		return null;
	}
	
	// returns whether to terminate or move forward
	public boolean triggerNext()
	{
		if((runLevel + 1) >= engines.length)
		{
			System.out.println("Ending now.. " + finalChildType);
			runLevel++;
			System.out.println("Levels... " + findLevels());

			//finalChildType = "TYPE" + runLevel;
			Vector <ISEMOSSNode> childNodes = getSInstanceNodes(finalChildType);
			System.out.println("Number of child nodes... " + childNodes.size());
			return true;
		}
		else
		{
			runLevel++;
			finalChildType = "TYPE" + runLevel;
			// need to set the new set of seeds
			// reset seeds
			System.err.println("Now at run level.... " + runLevel + finalChildType);
			Vector <ISEMOSSNode> childNodes = getSInstanceNodes(finalChildType);
			System.out.println("Number of child nodes... " + childNodes.size());
			seeds = new ISEMOSSNode[childNodes.size()];
			for(int seedIndex = 0;seedIndex < childNodes.size();seedIndex++)
				seeds[seedIndex] = childNodes.elementAt(seedIndex);
			//seeds = (StringClass[])childNodes.toArray();
			execEngine();
			return false;
		}
	}
	
	public void execEngine()
	{
		int numProc = Runtime.getRuntime().availableProcessors();
		numProc = 8;
		int seedSplitter = seeds.length / numProc;
		
		String [] childTypeToGet = new String[1];
		childTypeToGet[0] = "IGNORE FOR NOW";
		//childTypeToGet[0] = childs[runLevel];
		int lastIndex = 0;
				
		for(int threadIndex = 0;threadIndex < numProc;threadIndex++)
		{
			int nextIndex = lastIndex + seedSplitter;
			// get the thread
			ISEMOSSNode [] curParents = Arrays.copyOfRange(seeds, lastIndex, nextIndex);
			TreeThreader threader = (TreeThreader) threadList.get(threadIndex);
			threader.setChildTypes(childTypeToGet);
			((SampleHashEngine)this.engines[runLevel]).parLevel = runLevel;
			((SampleHashEngine)this.engines[runLevel]).childLevel = (runLevel + 1);
			threader.setEngine(this.engines[runLevel]);
			threader.setParents(curParents);
			if(runLevel == 0) // first time
				service.execute(threader);
			lastIndex = nextIndex;
		}		
	}
	
	public long printTime()
	{
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		long time1 = System.nanoTime();
		long mtime1 = System.currentTimeMillis();
		//System.out.println("Start >> Nano " + dateFormat.format(date));
		System.out.println("Start >> Nano " + time1);
		return mtime1;
	}

}

