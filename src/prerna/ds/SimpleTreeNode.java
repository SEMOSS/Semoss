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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

public class SimpleTreeNode {
	
	// gets the sibling tree
	SimpleTreeNode leftSibling = null;
	SimpleTreeNode rightSibling = null;
	//SimpleTreeNode2 eldestLeftSibling = null; // some performance to make sure this is fast enough
	SimpleTreeNode parent = null;
	SimpleTreeNode rightChild = null;
	SimpleTreeNode leftChild = null;
	//static String numberList = "";
	public static final String EMPTY = "_____";
	
	ITreeKeyEvaluatable leaf = null;
	
	//int fanout = 4;
	
	//boolean root = true;
	
	public SimpleTreeNode(ITreeKeyEvaluatable leaf)
	{
		this.leaf = leaf;
	}
	
	//This should be a depth first search, double check if that is the case
	public boolean search(Vector <SimpleTreeNode> nodes, SimpleTreeNode searchNode, boolean found)
	{
		Vector <SimpleTreeNode> childNodes = new Vector<SimpleTreeNode>();
		for(int nodeIndex = 0;nodeIndex < nodes.size() && !found;nodeIndex++)
		{
			SimpleTreeNode node = nodes.get(nodeIndex);
			// search all siblings
			do
			{
				found = node.equal(searchNode);
				if(found)
					break;
				node = node.rightSibling;
			}while(node != null && !found);
			// if not found then go for the one where it is the first left
			// and then add that child node and move
			node = nodes.get(nodeIndex);
			
			// find where this node lands
			// same logic as embed
			while(node != null)
			{
				//System.out.print(node.leaf.getKey());
				// see if the node is less than this current node, then add the left child
				if(node.left(searchNode) && node.leftChild != null)
				{
					childNodes.add(node.leftChild);
					break;
				}
				else if(!found && node.rightSibling == null && node.rightChild != null)
				{
					childNodes.add(node.rightChild );
					break;
				}
				node = node.rightSibling;
			}
		}
		if(!found && childNodes.size() > 0)
			return search(childNodes, searchNode, false);
		else {
			return found;
		}
	}
	
	public boolean hasChild(ISEMOSSNode node)
	{
		boolean found = false;
		SimpleTreeNode left = this.leftChild;
		while(left != null && !found)
		{
			found = left.leaf.isEqual(node);
			left = left.rightSibling;
		}
		return found;
	}
	
	
	/**
	 * 
	 * @param node
	 * @return
	 */
	public static SimpleTreeNode root(SimpleTreeNode node)
	{
		if(node.parent != null)
		{
			return root(node.parent);
		}
		else return getLeft(node);
	}
	
	
	
//	public void printNodes(SimpleTreeNode2 rightChild2) {
//		// TODO Auto-generated method stub
//		System.out.println(rightChild2.leaf.getKey());
//		if(rightChild2.rightSibling != null)
//			printNodes(rightChild2.rightSibling);
//		else
//			System.out.println("=====");
//	}

	public boolean equal(SimpleTreeNode node2Embed) {
		return this.leaf.isEqual(node2Embed.leaf);
	}

	
	public int countSiblings(SimpleTreeNode counterNode)
	{
		int count = 1;
		counterNode = getLeft(counterNode);
		// expects you to give the left most node
		while(counterNode != null && counterNode.rightSibling != null)
		{
			counterNode = counterNode.rightSibling;
			count++;
		}
		return count;
	}

	public int countChildren(SimpleTreeNode counterNode)
	{
		int count = 1;
		// expects you to give the left most node
		while(counterNode.rightSibling != null)
		{
			if(counterNode.leftChild != null)
				count++;
			if(counterNode.rightChild != null)
				count++;
			counterNode = counterNode.rightSibling;
		}
		return count;
	}
	
	public int countNodeChildren(SimpleTreeNode counterNode)
	{
		int count = 0;
		SimpleTreeNode leftChild = counterNode.leftChild;
		if(leftChild == null) {
			return count;
		} else {
			count++;
		}
		while(leftChild.rightSibling != null) {
			count++;
			leftChild = leftChild.rightSibling;
		}
		return count;
	}


	public boolean hasChildren(SimpleTreeNode counterNode)
	{
		boolean has = false;
		// expects you to give the left most node
		while(counterNode.rightSibling != null && !has)
		{
			if(counterNode.leftChild != null)
				has = true;
			if(counterNode.rightChild != null)
				has = true;
			counterNode = counterNode.rightSibling;
		}
		return has;
	}
	
	public boolean hasChild() {
		return this.leftChild != null || this.rightChild != null;
	}

	public void setParent(SimpleTreeNode leftList, SimpleTreeNode parent) {
		//should i put a check to make sure leftList does not equal parent?
		if(leftList == parent) return;
		
		while(leftList != null) {
			leftList.parent = parent;
			leftList = leftList.rightSibling;
		}
	}
	
	public static SimpleTreeNode getLeft(SimpleTreeNode node)
	{
		if(node.leftSibling==null) {
			return node;
		}
		else {
			return getLeft(node.leftSibling);
		}
	}

	public static SimpleTreeNode getRight(SimpleTreeNode node)
	{
		if(node.rightSibling==null) {
			return node;
		} else {
			return getRight(node.rightSibling);
		}
	}
	

	public boolean left(SimpleTreeNode node) {
		return this.leaf.isLeft(node.leaf);
	}
		
	
	public static String serializeTree(String output, Vector<SimpleTreeNode> nodes, boolean parent, int level) {
		return serializeTree(new StringBuilder(output), nodes, parent, level);
	}
	
	private static String serializeTree(StringBuilder output, Vector <SimpleTreeNode> nodes, boolean parent, int level)
	{
		Vector childNodes = new Vector();
		for(int nodeIndex = 0;nodeIndex < nodes.size();nodeIndex++)
		{
			SimpleTreeNode node = nodes.elementAt(nodeIndex);
			do {
				output.append(node.leaf.toString());
				if(node.leftChild != null) {
					childNodes.add(node.leftChild);
				}

				if(node.rightChild != null) {
					childNodes.add(node.rightChild);
				}
				
				node = node.rightSibling;
				if(node != null) {
					output.append("@@@");
				}
				
			} while(node != null);
			if(!parent) {
				output.append("|||");
			}
		}
		output.append("///");
		if(childNodes.size() > 0)
			return serializeTree(output, childNodes, false, level+1);
		else
			return output.toString();
	}
	
	public static SimpleTreeNode deserializeTree(String output)
	{
		SimpleTreeNode rootNode = null;
		boolean parent = true;
		Vector <SimpleTreeNode> parentNodes = new Vector();
		// each one of this is a new line
		String[] mainTokens = output.split("/{3}");
		for(String line: mainTokens)
		{
			Vector <SimpleTreeNode> nextLevel = new Vector();
//			String line = mainTokens.nextToken();
			int count = 0;
			if(!parent)
			{
				// next is to zoom out the pipes
				//TreeNode curParent = parentNodes.elementAt(count);
				SimpleTreeNode curParentNode = null;
//				System.out.println("Total number of parents " + parentNodes.size());
//				StringTokenizer leftRightTokens = new StringTokenizer(line, "|");
				String[] leftRightTokens = line.split("\\|{3}");
				for(String leftChildString: leftRightTokens)
				{
					if(curParentNode == null)
					{
						curParentNode = parentNodes.elementAt(count);
						count++;
					}
//					System.out.println("[" + curParentNode + "]");
//					System.out.println("Cur Parent Node is " + curParentNode.leaf.getKey());
//					String leftChildString = leftRightTokens.nextToken();
//					String rightChildString = leftRightTokens.nextToken();
					Object [] stringOfNodes = createStringOfNodes(leftChildString, nextLevel);
					SimpleTreeNode leftNode = (SimpleTreeNode)stringOfNodes[0];
					nextLevel = (Vector<SimpleTreeNode>)stringOfNodes[1];
					// set the parent here
					// do the right node only if the parent has no sibling kind of
//					stringOfNodes = createStringOfNodes(rightChildString, nextLevel);
//					SimpleTreeNode rightNode = (SimpleTreeNode)stringOfNodes[0];
//					nextLevel = (Vector<SimpleTreeNode>)stringOfNodes[1];

					curParentNode.leftChild = leftNode;
					leftNode.parent = curParentNode;
//					rightNode.parent = curParentNode;
					
//					if(curParentNode.leftSibling != null)
//						curParentNode.leftSibling.rightChild = curParentNode.leftChild;
//					if(curParentNode.rightSibling == null)
//						curParentNode.rightChild = rightNode;
					// move on next
					curParentNode = curParentNode.rightSibling;
				}
			}
			else
			{
//				System.out.println("Parent.. ");
				Object [] stringOfNodes = createStringOfNodes(line, nextLevel);
				rootNode = (SimpleTreeNode)stringOfNodes[0];
				nextLevel = (Vector<SimpleTreeNode>)stringOfNodes[1];
//				System.out.println("Next level is " + nextLevel.get(0).leaf.getKey());
				parent = false;
			}
			parentNodes = nextLevel;
		}
		return rootNode;
	}
	
	public static Object [] createStringOfNodes(String childString, Vector <SimpleTreeNode> inVector)
	{
		// nasty.. I dont have a proper object eeks
		Object [] retObject = new Object[2];
		// final loop is the <> loop
//		StringTokenizer leftString = new StringTokenizer(childString, "@");
		String[] leftString = childString.split("@{3}");
		SimpleTreeNode leftNode = null;
		for(String leftNodeKey: leftString)
		{
			String[] classString = leftNodeKey.split("#{3}");
			ISEMOSSNode sNode = null;
			try {
				sNode = (ISEMOSSNode) Class.forName(classString[0]).getConstructor(String.class, Boolean.class).newInstance(classString[1], true);
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NoSuchMethodException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
//			String leftNodeKey = leftString.nextToken();
//			ISEMOSSNode sNode = new StringClass(leftNodeKey, true);
			SimpleTreeNode node = new SimpleTreeNode(sNode);
			
			if(leftNode == null)
			{
				leftNode = node;
				retObject[0] = node;
				inVector.addElement(node);
//				System.out.println("Adding Key " + leftNodeKey);
				// also need to set the parent here
			}
			else
			{
				leftNode.rightSibling = node;
				node.leftSibling = leftNode;
				leftNode = node;
				//inVector.addElement(node);
			}
		}				
		retObject[1] = inVector;
		return retObject;
	}
	
	
//	public static SimpleTreeNode2 join(Vector <SimpleTreeNode2> fromNodes, Vector <SimpleTreeNode2> toNodeRoot,boolean parent, SimpleTreeNode2 resNodeRoot)
//	{
//		Vector childNodes = new Vector();
//		for(int nodeIndex = 0;nodeIndex < fromNodes.size();nodeIndex++)
//		{
//			SimpleTreeNode2 node = fromNodes.elementAt(nodeIndex);
//			do{
//				//output += node.leaf.getKey();
//				if(toNodeRoot.get(0).search(toNodeRoot, node, false))
//				{
//					SimpleTreeNode2 newNode = new SimpleTreeNode2(node.leaf);
//					if(resNodeRoot == null)
//						resNodeRoot = newNode;
//					//else
//					//	resNodeRoot = resNodeRoot.insertData(newNode);
//				}
//				if(node.leftChild != null)
//					childNodes.add(node.leftChild);
//				//else
//				//	childNodes.add(new TreeNode(new IntClass(0)));
//				if(node.rightChild != null)
//					childNodes.add(node.rightChild);
//				//else
//				//	childNodes.add(new TreeNode(new IntClass(0)));
//				node = node.rightSibling;
//			}while(node != null);
//		}
//		if(childNodes.size() > 0)
//			return join(childNodes, toNodeRoot, false, resNodeRoot);
//		else
//			return resNodeRoot;
//	}
	
	public void addChild(SimpleTreeNode node)
	{
		node.parent = this;
		//System.out.println("Leaf is " + node.leaf.getKey());
		if(this.leftChild != null && !this.leftChild.leaf.getKey().equalsIgnoreCase(SimpleTreeNode.EMPTY))
		{
			SimpleTreeNode rightMost = getRight(this.leftChild);
			rightMost.rightSibling = node;
			node.leftSibling = rightMost;
//			this.printNodes(rightMost);
//			System.out.println(rightMost.toString() + "-" + rightMost.leaf.getValue() + " ----------- " + node.leaf.getValue() + "-" + node.toString());
		}
		else
		{
			// what kind of cookie logic is this//
			/*SimpleTreeNode prevLeftChild = null;
			if(this.leftChild != null)
				prevLeftChild = this.leftChild.leftChild; // what is this doing here
			node.leftChild = prevLeftChild;*///
			this.leftChild = node;			
		}

		node = node.rightSibling;
		while (node != null){
			node.parent = this;
			node = node.rightSibling;
		}
	}

	public static void addLeafChild(SimpleTreeNode parentNode, SimpleTreeNode node)
	{
		// the idea here is it will go to the ultimate child of this node and add it there
		// this is useful when there is a non-linear join that will happen
		// I will get to this later
		// use type comparison for siblings vs. ultimate child
		
		if(parentNode.leftChild != null)
		{
			//System.err.println("The value of the node is " +parentNode.leftChild.leaf.getKey() );

			if(((ISEMOSSNode)parentNode.leftChild.leaf).getType().equalsIgnoreCase(((ISEMOSSNode)node.leaf).getType())) // if it is the same type dont bother
				parentNode.addChild(node);
			else
			{
				SimpleTreeNode targetNode = parentNode.leftChild;
				
				do
				{
					Vector<SimpleTreeNode> vec = new Vector<SimpleTreeNode>();
					vec.add(node);
					String serialized = SimpleTreeNode.serializeTree("", vec, true, 0);
					SimpleTreeNode newNode = SimpleTreeNode.deserializeTree(serialized);
					addLeafChild(targetNode, newNode); // move on to this node
					targetNode = targetNode.rightSibling;// move to the next node on the sibling list//
				} while(targetNode != null);
			}
		}
		else {
			parentNode.addChild(node);
		}
		
	}
		
	//TODO: need to take into account filtered values
	public static void deleteNode(SimpleTreeNode node)
	{
		// realign parents
		
		// if the node the the left child, catch the parent and set the 
		// first possibility - this has no right sibling - possibly the best scenario - delete
		if(node.rightSibling == null && node.leftSibling == null)
		{
			if(node.parent != null) node.parent.leftChild = null;
			node.parent = null;
		}
		else if(node.leftSibling != null && node.rightSibling == null)
		{
			node.parent = null;
			node.leftSibling.rightSibling = null; // nullify this node
			node.leftSibling = null;
		}
		else if(node.rightSibling != null && node.leftSibling == null)
		{
			if(node.parent != null) node.parent.leftChild = node.rightSibling;
			node.rightSibling.leftSibling = null; // nullify this node
			node.parent = null;
			node.rightSibling = null;
		}		
		else if(node.rightSibling != null && node.leftSibling != null)
		{
			SimpleTreeNode leftSibling = node.leftSibling;
			node.rightSibling.leftSibling = leftSibling;
			leftSibling.rightSibling = node.rightSibling;
			node.parent = null;
			node.rightSibling = null;
			node.leftSibling = null;
		}		

	}

	public SimpleTreeNode getLeaf(SimpleTreeNode node, boolean left)
	{
		if(node.leftChild != null && left)
			return getLeaf(node.leftChild, true);
//		if(node.rightSibling == null && node.rightChild != null && !left)
//			return getLeaf(node.rightChild, false);
		else return node;
	}
		
	// x the method to use
	public void flattenTree(Vector<SimpleTreeNode> parentNodeList, SimpleTreeNode node)
	{
		if(node.leftChild != null && node.rightChild != null)
		{
			if(node.leftChild != null)
			{
				Vector <SimpleTreeNode> newList = getNewVector(parentNodeList);
				//Collections.copy(newList, parentNodeList);
				newList.add(node);
				SimpleTreeNode leftChild = node.leftChild;
				while(leftChild != null)
				{
					flattenTree(newList, leftChild);
					leftChild = leftChild.rightSibling;
				}
			}
			if(node.rightChild != null && node.rightSibling == null)
			{
				Vector newList = getNewVector(parentNodeList);
				//Collections.copy(newList, parentNodeList);
				newList.add(node);
				SimpleTreeNode leftChild = node.rightChild;
				while(leftChild != null)
				{
					flattenTree(newList, leftChild);
					leftChild = leftChild.rightSibling;
				}
			}
		}
		else
		{
			//System.out.println("Current Array is " + parentNodeList.size());
			//for(int idx = 0;idx < parentNodeList.size();idx++)
			//	System.err.print(">>" + parentNodeList.get(idx).leaf.getKey() + ":");
			//System.err.print(node.leaf.getKey());
			//System.err.println("----");
		}
	}

	// this is the method that will flatten the simple TreeNode
	public void flattenUnBalancedTree(Vector<SimpleTreeNode> parentNodeList, SimpleTreeNode node)
	{
			if(node.leftChild != null)
			{
				Vector <SimpleTreeNode> newList = getNewVector(parentNodeList);
				//System.err.print(".");
				//Collections.copy(newList, parentNodeList);
				newList.add(node);
				SimpleTreeNode leftChild = node.leftChild;
				while(leftChild != null)
				{
					flattenUnBalancedTree(newList, leftChild);
					leftChild = leftChild.rightSibling;
				}
			}
			else
			{
				//System.out.println("Current Array is " + parentNodeList.size());
				//for(int idx = 0;idx < parentNodeList.size();idx++)
				//	System.err.print(">>" + parentNodeList.get(idx).leaf.getKey() + "TT" + ((ISEMOSSNode)parentNodeList.get(idx).leaf).getType() + ":");
				//System.err.print(node.leaf.getKey());
				//System.err.println("\n");
				//System.out.println(".");
			}
	}
	
	// this is the method that will flatten the simple TreeNode
	// with filter
	public void flattenUnBalancedTree(Vector<SimpleTreeNode> parentNodeList, SimpleTreeNode node, Hashtable <String, Hashtable> filterHash)
	{
			if(node.leftChild != null)
			{
				Vector <SimpleTreeNode> newList = getNewVector(parentNodeList);
				//System.err.print(".");
				//Collections.copy(newList, parentNodeList);
				ISEMOSSNode sNode = (ISEMOSSNode)node.leaf;
				if(!(filterHash.containsKey(sNode.getType()) && filterHash.get(sNode.getType()).containsKey(sNode.getValue())))
				{
					newList.add(node);
					SimpleTreeNode leftChild = node.leftChild;
					while(leftChild != null)
					{
						flattenUnBalancedTree(newList, leftChild);
						leftChild = leftChild.rightSibling;
					}
				}
			}
			else
			{
				//System.out.println("Current Array is " + parentNodeList.size());
				//for(int idx = 0;idx < parentNodeList.size();idx++)
				//	System.err.print(">>" + parentNodeList.get(idx).leaf.getKey() + "TT" + ((ISEMOSSNode)parentNodeList.get(idx).leaf).getType() + ":");
				//System.err.print(node.leaf.getKey());
				//System.err.println("\n");
				//System.out.println(".");
			}
	}

	
	// flatten until some specific point
	public void flattenUnBalancedTree(Vector<SimpleTreeNode> parentNodeList, SimpleTreeNode node, String untilType)
	{
			// still needs to be implemented
			if(node.leftChild != null && !((ISEMOSSNode)node.leaf).getType().equalsIgnoreCase(untilType))
			{
				Vector <SimpleTreeNode> newList = getNewVector(parentNodeList);
				//System.err.print(".");
				//Collections.copy(newList, parentNodeList);
				newList.add(node);
				SimpleTreeNode leftChild = node.leftChild;
				while(leftChild != null)
				{
					flattenUnBalancedTree(newList, leftChild, untilType);
					leftChild = leftChild.rightSibling;
				}
			}
			else
			{
				//System.out.println("Current Array is " + parentNodeList.size());
				for(int idx = 0;idx < parentNodeList.size();idx++)
					System.err.print(">>" + parentNodeList.get(idx).leaf.getKey() + ":");
				// show this only if the node type is the untilType
				if(((ISEMOSSNode)node.leaf).getType().equalsIgnoreCase(untilType))
					System.err.print(node.leaf.getKey());
				System.err.println("\n");
			}
	}

	// flatten until some specific point
	// with filter
	public void flattenUnBalancedTree(Vector<SimpleTreeNode> parentNodeList, SimpleTreeNode node, String untilType, Hashtable <String, Hashtable> filterHash)
	{
			// still needs to be implemented
			if(node.leftChild != null && !((ISEMOSSNode)node.leaf).getType().equalsIgnoreCase(untilType))
			{
				Vector <SimpleTreeNode> newList = getNewVector(parentNodeList);
				//System.err.print(".");
				//Collections.copy(newList, parentNodeList);
				ISEMOSSNode sNode = (ISEMOSSNode)node.leaf;
				if(!(filterHash.containsKey(sNode.getType()) && filterHash.get(sNode.getType()).containsKey(sNode.getValue())))
				{
					newList.add(node);
					SimpleTreeNode leftChild = node.leftChild;
					while(leftChild != null)
					{
						flattenUnBalancedTree(newList, leftChild, untilType);
						leftChild = leftChild.rightSibling;
					}
				}
			}
			else
			{
				//System.out.println("Current Array is " + parentNodeList.size());
				for(int idx = 0;idx < parentNodeList.size();idx++)
					System.err.print(">>" + parentNodeList.get(idx).leaf.getKey() + ":");
				// show this only if the node type is the untilType
				if(((ISEMOSSNode)node.leaf).getType().equalsIgnoreCase(untilType))
					System.err.print(node.leaf.getKey());
				System.err.println("\n");
			}
	}

	public static  Vector getChildsOfType(Vector <SimpleTreeNode> nodes, String type, Vector <SimpleTreeNode> results)
	{
		Vector <SimpleTreeNode> nextRound = new Vector();
		while(nodes.size() > 0)
		{
			SimpleTreeNode targetNode = nodes.remove(0);
			//System.out.println("Trying... " + targetNode.leaf.getKey());
			if(((ISEMOSSNode)targetNode.leaf).getType().equalsIgnoreCase(type))
			{
				//System.err.println("Into the results.. ");
				results = addAllSiblings(targetNode, results);
			}
			else
			{
				//System.err.println("Adding for future");
				if(targetNode.leftChild != null && ((ISEMOSSNode)targetNode.leftChild.leaf).getType().equalsIgnoreCase(type))
					nextRound.addElement(targetNode.leftChild); 
				else
					addAllSiblings(targetNode.leftChild, nextRound);
			}
			//System.out.println("Results...  " + results.size());
		}
		if(nextRound.size() == 0)
			return results;
		else
			return getChildsOfType(nextRound, type, results);
	}
	
	private static Vector <SimpleTreeNode> addAllSiblings(SimpleTreeNode node, Vector <SimpleTreeNode> inVector)
	{
		while(node != null)
		{
			inVector.addElement(node);
			//System.out.println("Added " + node.leaf.getKey());
			node = node.rightSibling;
		}
		return inVector;
	}
	
	/**
	 * Will flatten the data based on all the right siblings and children of the node input
	 * Will add the flattened data into the table List<Object[]> input
 	 * @param table						The List<Object[]> that will contain the flattened data
	 * @param node						The node to grab all right siblings and children to get the flattened data
	 * @param parentNodeList			A Vector used to recursively store all the parent-child relationships in a row of the data
	 * @param levels					The number of levels to place null values such that the data is not a jagged matrix
	 */
	public void flattenTreeFromRoot(SimpleTreeNode node, Vector<Object> parentNodeList, List<Object[]> table, int levels)
	{
		while(node != null)
		{
			flattenTree(table, parentNodeList, node, levels);
			node = node.rightSibling;
		}
	}
	
	/**
	 * Recursive method to flattened out the tree
 	 * @param table						The List<Object[]> that will store the flattened data
	 * @param parentNodeList			The Vector used to store all the parent-child relationship in a row of the data
	 * @param node						The node in the recursive step of iterating through the tree
	 * @param levels					The number of levels to place null values such that the data is not a jagged matrix
	 */
	public void flattenTree(List<Object[]> table, Vector<Object> parentNodeList, SimpleTreeNode node, int levels)
	{
		if(node.leftChild != null)
		{
			Vector<Object> newList = getNewVector(parentNodeList);
			newList.add(node.leaf.getValue());
			SimpleTreeNode leftChild = node.leftChild;
			while(leftChild != null)
			{
				flattenTree(table, newList, leftChild, levels);
				leftChild = leftChild.rightSibling;
			}
		}
		else {
			Vector<Object> newList = getNewVector(parentNodeList);
			newList.add(node.leaf.getValue());
			while(newList.size() < levels) {
				newList.add(null);
			}
			table.add(newList.toArray());
		}
	}
	
	/**
	 * Will flatten the data based on all the right siblings and children of the node input
	 * Will add the flattened raw data into the table List<Object[]> input
 	 * @param table						The List<Object[]> that will contain the flattened raw data
	 * @param node						The node to grab all right siblings and children to get the flattened data
	 * @param parentNodeList			A Vector used to recursively store all the parent-child relationships in a row of the data
	 * @param levels					The number of levels to place null values such that the data is not a jagged matrix
	 */
	public void flattenRawTreeFromRoot(SimpleTreeNode node, Vector<Object> parentNodeList, List<Object[]> table, int levels)
	{
		while(node != null)
		{
			flattenRawTree(table, parentNodeList, node, levels);
			node = node.rightSibling;
		}
	}
	
	/**
	 * Recursive method to flattened out the tree with raw values
 	 * @param table						The List<Object[]> that will store the flattened raw data
	 * @param parentNodeList			The Vector used to store all the parent-child relationship in a row of the data
	 * @param node						The node in the recursive step of iterating through the tree
	 * @param levels					The number of levels to place null values such that the data is not a jagged matrix
	 */
	public void flattenRawTree(List<Object[]> table, Vector<Object> parentNodeList, SimpleTreeNode node, int levels)
	{
		if(node.leftChild != null)
		{
			Vector<Object> newList = getNewVector(parentNodeList);
			newList.add(node.leaf.getRawValue());
			SimpleTreeNode leftChild = node.leftChild;
			while(leftChild != null)
			{
				flattenRawTree(table, newList, leftChild, levels);
				leftChild = leftChild.rightSibling;
			}
		}
		else {
			Vector<Object> newList = getNewVector(parentNodeList);
			
			try {
				node.leaf.getRawValue();
			} catch (NullPointerException e) {
				e.printStackTrace();
			}
			newList.add(node.leaf.getRawValue());
//			while(newList.size() < levels) {
//				newList.add(null);
//			}
			if(newList.size() == levels){
				table.add(newList.toArray());
			}
			else {
				System.err.println("not a complete row " + newList.toString());
			}
		}
	}
	
	// need to accomodate rows to fill
	public void flattenRoots(SimpleTreeNode node, Vector outputVector)
	{
		while(node != null)
		{
			flattenUnBalancedTree(outputVector, node);
			node = node.rightSibling;
		}
	}

	// need to accomodate when I have rows to fill
	public void flattenRoots(SimpleTreeNode node, String untilType, Vector outputVector)
	{
		while(node != null)
		{
			flattenUnBalancedTree(outputVector, node, untilType);
			node = node.rightSibling;
		}
	}

	// need to accomodate rows to fill - with the filter
	public void flattenRoots(SimpleTreeNode node, Vector outputVector, Hashtable filter)
	{
		while(node != null)
		{
			flattenUnBalancedTree(outputVector, node, filter);
			node = node.rightSibling;
		}
	}

	// need to accomodate when I have rows to fill
	public void flattenRoots(SimpleTreeNode node, String untilType, Vector outputVector, Hashtable filter)
	{
		while(node != null)
		{
			flattenUnBalancedTree(outputVector, node, untilType, filter);
			node = node.rightSibling;
		}
	}
	
	
	public SimpleTreeNode mergeTrees(SimpleTreeNode fromNode, SimpleTreeNode toNode, boolean inner)
	{
		// if inner is true
		// merge from left to right
		// if inner is false
		// merge from out to in
		
		// logic, for everynode 
		
		
		return null;
	}
	
	public Vector getNewVector(Vector vector)
	{
		Vector retVec = new Vector();
		for(int idx = 0;idx < vector.size();idx++)
			retVec.addElement(vector.elementAt(idx));
		return retVec;
	}
	
//	public static void wait(BufferedReader reader, SimpleTreeNode2 node) throws Exception
//	{
//		System.out.println(">>");
//		//reader.readLine();
//		Vector rootNodes = new Vector();
//		rootNodes.add(node);
//		node.printNode(rootNodes, true, 1);
//		System.out.println("Numbers >>  " + numberList);
//
//	}
}

