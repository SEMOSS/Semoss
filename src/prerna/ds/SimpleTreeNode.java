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

import java.lang.reflect.InvocationTargetException;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class SimpleTreeNode {
	
	private static final Logger LOGGER = LogManager.getLogger(BTreeDataFrame.class.getName());
	
	// gets the sibling tree
	SimpleTreeNode leftSibling = null;
	SimpleTreeNode rightSibling = null;
	//SimpleTreeNode2 eldestLeftSibling = null; // some performance to make sure this is fast enough
	SimpleTreeNode parent = null;
	SimpleTreeNode rightChild = null;
	SimpleTreeNode leftChild = null;
	//static String numberList = "";
	public static final String EMPTY = "";
	
	Map <String, Integer> childCount = new Hashtable<String, Integer>();
	
	ITreeKeyEvaluatable leaf = null;
	
	//int fanout = 4;
	
	//boolean root = true;
	boolean hardFiltered;
	int transitivelyFiltered;
	
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
//		if(node.parent != null)
//		{
//			return root(node.parent);
//		}
//		else return getLeft(node);
		
		while(node.parent != null) {
			node = node.parent;
		}
		return getLeft(node);
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
	
	public static int countNodeChildren(SimpleTreeNode counterNode)
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

	/**
	 * 
	 * @param parentNode
	 * @return true if parentNode has one and only one child, false otherwise
	 */
	public static boolean hasOneChild(SimpleTreeNode parentNode) {
		
		SimpleTreeNode leftChild = parentNode.leftChild;
		if(leftChild == null) {
			return false;
		}
		
		return leftChild.rightSibling == null;
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

	public static void setParent(SimpleTreeNode leftList, SimpleTreeNode parent) {
		//should i put a check to make sure leftList does not equal parent?
		if(leftList == parent) return;
		
		while(leftList != null) {
			leftList.parent = parent;
			leftList = leftList.rightSibling;
		}
	}
	
	/**
	 * 
	 * @param node
	 * @return the left most sibling of the argument
	 */
	public static SimpleTreeNode getLeft(SimpleTreeNode node) {
		while(node.leftSibling != null) {
			node = node.leftSibling;
		}
		return node;
	}

	/**
	 * 
	 * @param node
	 * @return the right most sibling of the argument
	 */
	public static SimpleTreeNode getRight(SimpleTreeNode node) {
		while(node.rightSibling != null) {
			node = node.rightSibling;
		}
		return node;
	}
	

	public boolean left(SimpleTreeNode node) {
		return this.leaf.isLeft(node.leaf);
	}
		
	public static String serializeTree(SimpleTreeNode root) {
		LOGGER.debug("Serializing tree with root value: " + root.leaf.getValue());
		long startTime = System.currentTimeMillis();
		
		Vector<SimpleTreeNode> nodes = new Vector<SimpleTreeNode>();
		nodes.add(root);
		
		LOGGER.debug("Finished serializing tree with root value "+ root.leaf.getValue()+", "+(System.currentTimeMillis() - startTime)+" ms");
		return serializeTree(new StringBuilder(""), nodes, true, 0);
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
		LOGGER.debug("Deserializing tree...");
		long startTime = System.currentTimeMillis();
		
		SimpleTreeNode rootNode = null;
		boolean parent = true;
		Vector <SimpleTreeNode> parentNodes = new Vector();
		// each one of this is a new line
		String[] mainTokens = output.split("/{3}(?!/)"); // preserve if instance ends in a / character
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
		
		LOGGER.debug("Finished Deserialization with root value: "+rootNode.leaf.getValue()+", "+(System.currentTimeMillis() - startTime)+" ms");
		return rootNode;
	}
	
	public static Object [] createStringOfNodes(String childString, Vector <SimpleTreeNode> inVector)
	{
		// nasty.. I dont have a proper object eeks
		Object [] retObject = new Object[2];
		// final loop is the <> loop
//		StringTokenizer leftString = new StringTokenizer(childString, "@");
		
		String[] leftString = childString.split("@{3}(?!@)"); //split on the LAST 3 @ signs
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
//			SimpleTreeNode rightMost = getRight(this.leftChild);
//			rightMost.rightSibling = node;
//			node.leftSibling = rightMost;
			
			SimpleTreeNode child = this.leftChild; // getting the first child
			SimpleTreeNode rightNode = child.rightSibling; // getting the second child
			
			child.rightSibling = node; // adding it to the right of the first node
			node.leftSibling = child; // sure setting the left node
			if(rightNode != null) {				
				rightNode.leftSibling = node; // insert
				node.rightSibling = rightNode; // insert complete
			}
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
		
		incrementCount(node.leaf); // put the count
		node = node.rightSibling;
		while (node != null){ // I have no idea why this is being done ?
			node.parent = this;
			node = node.rightSibling;
		}
	}
	
	public void incrementCount(ITreeKeyEvaluatable node)
	{
		// insert the count
		String nodeValue = node.getKey();
		Integer childCounter = 0;
		if(childCount.containsKey(nodeValue))
			childCounter = childCount.get(nodeValue);
		childCounter++;
		childCount.put(nodeValue, childCounter);		
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
		
	/**
	 * 
	 * @param node - the node to be deleted from its tree
	 * 
	 * separates the node from its siblings and parents, and realigns the relationships
	 * Ex: A--> B--> C
	 * 		Deleting B would result in A--> C
	 */
    public static void deleteNode(SimpleTreeNode node)
    {
    	//TODO: need to take into account filtered values
          //only child
          if(node.rightSibling == null && node.leftSibling == null)
          {
                 if(node.parent != null) node.parent.leftChild = null;
                 node.parent = null;
          }
          //right most
          else if(node.leftSibling != null && node.rightSibling == null)
          {
                 node.leftSibling.rightSibling = null; // nullify this node
                 node.parent = null;
                 node.leftSibling = null;
          }
          //left most
          else if(node.rightSibling != null && node.leftSibling == null)
          {
                 if(node.parent != null) node.parent.leftChild = node.rightSibling;
                 node.rightSibling.leftSibling = null; // nullify this node
                 node.parent = null;
                 node.rightSibling = null;
          }
          //middle
          else if(node.rightSibling != null && node.leftSibling != null)
          {
                 SimpleTreeNode leftSibling = node.leftSibling;
                 node.rightSibling.leftSibling = leftSibling;
                 leftSibling.rightSibling = node.rightSibling;
                 node.parent = null;
                 node.rightSibling = null;
                 node.leftSibling = null;
          }
          // kill reference to
          node = null;
    }


	public SimpleTreeNode getLeaf(SimpleTreeNode node, boolean left)
	{
//		if(node.leftChild != null && left)
//			return getLeaf(node.leftChild, true);
////		if(node.rightSibling == null && node.rightChild != null && !left)
////			return getLeaf(node.rightChild, false);
//		
//		else return node;
		
		while(node.leftChild != null) {
			node = node.leftChild;
		}
		return node;
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

	
	// new methods here >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	
	public void addSibling(SimpleTreeNode node)
	{
		//SimpleTreeNode daNode = this;
		node.leftSibling = this.rightSibling;
		this.rightSibling.leftSibling = node;
		this.rightSibling = node;		
	}
	
	public static  Hashtable<String, Integer> getChildsOfTypeCount(Vector <SimpleTreeNode> nodes, String type, Hashtable <String, Integer> results)
	{
		Vector <SimpleTreeNode> nextRound = new Vector();
		while(nodes.size() > 0)
		{
			SimpleTreeNode targetNode = nodes.remove(0);
			//System.out.println("Trying... " + targetNode.leaf.getKey());
			if(((ISEMOSSNode)targetNode.leaf).getType().equalsIgnoreCase(type))
			{
				//System.err.println("Into the results.. ");
				// get to the parent
				// get the number of nodes on this one
				results = getAllSiblingCount(targetNode, results);
				//results = addAllSiblings(targetNode, results);
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
			return getChildsOfTypeCount(nextRound, type, results);
	}

	// when the parent is the same... I need to find a way to add it
	// i.e. everytime it consolidates.. I need to add it
	// but as I do this.. I need to keep track of which tree branch I am climbing
	public static  Hashtable<String, Integer> getParentsOfTypeCount(Vector <SimpleTreeNode> nodes, String type, Hashtable <String, Integer> results, boolean first)
	{
		// I need to first get the parents of this type.. chances are all that we are looking for is parent.. ie the to type or may be not
		// if that is the case.. I can stop the loop and return
		// else I need to see if this is the first time
		// if first time.. I need to get the child count values in the parent
		// and allocate it
		// once that is done
		// then it is a recursive loop as I climb the branch. I am continuously trying to figure out if there is already a value and then input it. 
		//Hashtable <String, Integer> nextCountHash = new Hashtable<String, Integer>();
		Vector <SimpleTreeNode> nextRound = new Vector<SimpleTreeNode>();
		String curType = null;
		while(nodes.size() > 0)
		{
			SimpleTreeNode targetNode = nodes.remove(0);
			curType = ((ISEMOSSNode)targetNode.leaf).getType();
			
			if(targetNode.parent != null) //  && !((ISEMOSSNode)targetNode.parent.leaf).getType().equalsIgnoreCase(type))
			{
				
				int count = 0;
				String parentKey = targetNode.parent.leaf.getKey();
				String parentType = ((ISEMOSSNode)targetNode.parent.leaf).getType();
				
				//System.out.println("Parent.. " + parentKey);

				if(first)
					count = targetNode.parent.childCount.get(targetNode.leaf.getKey()); // get it from the parent
				else
					count = results.get(curType+targetNode.leaf.getKey());
				
				if(results.containsKey(parentKey))
					count = count + results.get(parentKey);
				
				// now set the parent in the next count hash
				if(parentType.equalsIgnoreCase(type))
					results.put(parentKey, count);
				else
					results.put(parentType + parentKey, count);
				
				// remove this key
				// it fails when you have a 0.0
				results.remove(curType + targetNode.leaf.getKey());
				
				if(!parentType.equalsIgnoreCase(type))
					nextRound.add(targetNode.parent);
			}
			else
			{
				// really nothing much to do
			}
		}
		//results = nextCountHash;
		if(nextRound.size() == 0)
			return results;
		else
			return getParentsOfTypeCount(nextRound, type, results, false);
	}



	// this is when there are multple parent types
	// which also need to be recorded
	// for the final child type
	
	// when the parent is the same... I need to find a way to add it
	// i.e. everytime it consolidates.. I need to add it
	// but as I do this.. I need to keep track of which tree branch I am climbing
	// the immediate known parent hash says which key to associate this with
	// the value string is what it needs to go to
	// It can never be more than one value kind of ?
	
	public static  Hashtable<String, Hashtable<String, Integer>> getChildOfTypeCountMulti(Vector <SimpleTreeNode> nodes, String parentTypes, String type, Hashtable <String, Hashtable<String, Integer>> results, Hashtable <String, String> immediateKnownParentHash)
	{
		// I need to first get the parents of this type.. chances are all that we are looking for is parent.. ie the to type or may be not
		// if that is the case.. I can stop the loop and return
		// else I need to see if this is the first time
		// if first time.. I need to get the child count values in the parent
		// and allocate it
		// once that is done
		// then it is a recursive loop as I climb the branch. I am continuously trying to figure out if there is already a value and then input it. 
		//Hashtable <String, Integer> nextCountHash = new Hashtable<String, Integer>();
		Vector <SimpleTreeNode> nextRound = new Vector<SimpleTreeNode>();
		Hashtable <String, String> nextRoundImmediateParentHash = new Hashtable<String, String>();
		Vector <String> keysToDrop = new Vector<String>();
		String curType = null;
		while(nodes.size() > 0)
		{
			SimpleTreeNode targetNode = nodes.remove(0);
			curType = ((ISEMOSSNode)targetNode.leaf).getType();
			
			String mainParentKey = null;
			// do the nulling process
			if(targetNode.leftChild != null && !curType.equalsIgnoreCase(type)) //  && !((ISEMOSSNode)targetNode.parent.leaf).getType().equalsIgnoreCase(type))
			{
				int count = 0;
				String thisKey = targetNode.leaf.getKey();
				String thisType = ((ISEMOSSNode)targetNode.leaf).getType();
				
				// I need to find my immediate parent
				// and then find which node is this parent a part of
				SimpleTreeNode parent = targetNode.parent;
				
				if(immediateKnownParentHash.size() == 0 || parent == null) // ok this is the first time // making a wrong assumption.. what if the dude wants to start from somewhere in the middle ? - this needs to be somehting else to indicate it is the first time
				{
					// this is the first time... so I am going to record this at this point
					if(parentTypes.contains(";" + thisType + ";"))
					{
						results.put(thisKey, new Hashtable()); // not doing much here
						mainParentKey = thisKey;
					}
				}
				else if(parent != null)
				{
					//String parentKey = parent.leaf.getKey();
					// ah interesting.. I need to break this out
					// now I do not know why I have the stuff as a vector
					// ok made it into a string
					//mainParentKey = thisKey;
					if(immediateKnownParentHash.containsKey(parent.leaf.getKey()))
						mainParentKey = immediateKnownParentHash.get(parent.leaf.getKey());
					if(mainParentKey != null && results.containsKey(mainParentKey)) // the program gets a sigh of relief here.. phew.. !!
					{
						// get this hashtable
						Hashtable <String, Integer> valueHash = new Hashtable<String, Integer>();
						if(parentTypes.contains(";" + thisType + ";"))
						{		
							//results.remove(mainParentKey);
							keysToDrop.add(mainParentKey);
							// modify the mainParent Key
							mainParentKey = mainParentKey + "_" + thisKey;
							// put this new key back	
							results.put(mainParentKey, valueHash);
						}
					}
				}				
				// now do the logic of adding the child to the next nodes
				// and also do the setting up in terms of the immediateparent Hash
				//if(parent != null && immediateKnownParentHash.containsKey(parent))
				//	immediateKnownParentHash.remove(targetNode.parent);
				
				// now set the new one
				nextRoundImmediateParentHash.put(targetNode.leaf.getKey(), mainParentKey);
				
				// now set all the child siblings into the nextRoung
				SimpleTreeNode nodeRunner = targetNode.leftChild;
				while(nodeRunner != null)
				{
					nextRound.add(nodeRunner);
					nodeRunner = nodeRunner.rightSibling;
				}
			}
			else
			{
				// we have come to the right point
				// woo hoo.. all that need to do at this point is
				// work through the child
				// pick the count and then 
				String key = targetNode.leaf.getKey();
				int count = targetNode.parent.childCount.get(key);
				
				mainParentKey = immediateKnownParentHash.get(targetNode.parent.leaf.getKey());
				// find the results
				Hashtable <String, Integer> curHash = results.get(mainParentKey);
				
				// possibly they have it already ? who knows
				if(curHash.containsKey(key))
					count = count + curHash.get(key);
				
				// put it back to curHash
				curHash.put(key, count);
				
				// put it back please
				results.put(mainParentKey, curHash);
			}
		}// end of while I bet.. 
		
		for(int keyIndex = 0;keyIndex < keysToDrop.size();results.remove(keysToDrop.elementAt(keyIndex)),keyIndex++);
		
		//results = nextCountHash;
		if(nextRound.size() == 0)
			return results;
		else
			return getChildOfTypeCountMulti(nextRound, parentTypes, type, results, nextRoundImmediateParentHash);
	}

	// this is the piece I need to do next
	// that is trying to walk the tree up and then finding out what are the pieces to account for
	// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
	// this is not a working function
	// I am trying to go from X3.. X1 with the ability to say what is the count of X1 in X3
	// Start at X3, if there is a parent
	// I am going to take the count from the parent
	// plot it into the function
	public static  Hashtable<String, Hashtable<String, Integer>> getParentOfTypeCountMulti(Vector <SimpleTreeNode> nodes, String parentTypes, String type, Hashtable <String, Hashtable<String, Integer>> results, Hashtable <String, String> immediateKnownParentHash)
	{
		// I need to first get the parents of this type.. chances are all that we are looking for is parent.. ie the to type or may be not
		// if that is the case.. I can stop the loop and return
		// else I need to see if this is the first time
		// if first time.. I need to get the child count values in the parent
		// and allocate it
		// once that is done
		// then it is a recursive loop as I climb the branch. I am continuously trying to figure out if there is already a value and then input it. 
		//Hashtable <String, Integer> nextCountHash = new Hashtable<String, Integer>();
		Vector <SimpleTreeNode> nextRound = new Vector<SimpleTreeNode>();
		Hashtable <String, String> nextRoundImmediateParentHash = new Hashtable<String, String>();
		Vector <String> keysToDrop = new Vector<String>();
		String curType = null;
		while(nodes.size() > 0)
		{
			SimpleTreeNode targetNode = nodes.remove(0);
			curType = ((ISEMOSSNode)targetNode.leaf).getType();
			
			String mainParentKey = null;
			// do the nulling process
			if(targetNode.parent != null && !curType.equalsIgnoreCase(type)) //  && !((ISEMOSSNode)targetNode.parent.leaf).getType().equalsIgnoreCase(type))
			{
				int count = 0;
				String thisKey = targetNode.leaf.getKey();
				String thisType = ((ISEMOSSNode)targetNode.leaf).getType();
				
				// I need to find my immediate parent
				// and then find which node is this parent a part of
				SimpleTreeNode parent = targetNode.parent;
				
				if(parent != null)
				{
					
					Hashtable <String, Integer> hashToPut = new Hashtable<String, Integer>();
					String immediateParentKey = thisKey;
					
					// get it from the hash
					if(immediateKnownParentHash.containsKey(thisKey))
						immediateParentKey = immediateKnownParentHash.get(thisKey);
					
					int curKeyCount = 0;
					// now get the latest through the immediate parent key					
					if(results.containsKey(immediateParentKey))
					{
						hashToPut = results.get(immediateParentKey);
						curKeyCount = hashToPut.get("temp");
					}
					else
					{
						// get the count from the parent
						curKeyCount = parent.childCount.get(thisKey);// so once I have the count I dont need it again
						// I should probably use temp as the key in hashToPut and finalize it in the end
						hashToPut.put("temp", curKeyCount); // first time set it up
					}
					if(parentTypes.contains(";" + thisType + ";")) // need to check for first time
					{
						if(!immediateParentKey.equals(thisKey))
						{
							mainParentKey = immediateParentKey + "_" + thisKey;
							keysToDrop.add(immediateParentKey);
						}
						else
							mainParentKey = thisKey;
					}
					else
					{
						mainParentKey = immediateParentKey; // revert it back to the previous one
					}

					hashToPut.remove(immediateParentKey); // I may not need this.. let me try
					
					int totalCount = 0;
					
					if(results.containsKey(mainParentKey)) // interestingly this is only valid if the ultimate parent is going to be the same
					{
						//curKeyCount = hashToPut.get("temp"); // + curKeyCount; - not sure I need this either
						hashToPut = results.get(mainParentKey);
					}
					hashToPut.put("temp", curKeyCount); // put the new key back // I need a way to synchronize this to say if there is already one use that
					// add it to the results
					results.remove(immediateParentKey); // remove the previous one
					results.put(mainParentKey, hashToPut); // replace with the new one

					
					nextRoundImmediateParentHash.put(parent.leaf.getKey(), mainParentKey);					
				}
				// now do the logic of adding the child to the next nodes
				// and also do the setting up in terms of the immediateparent Hash
				//if(parent != null && immediateKnownParentHash.containsKey(parent))
				//	immediateKnownParentHash.remove(targetNode.parent);
				
				// now set the new one
				//nextRoundImmediateParentHash.put(targetNode.leaf.getKey(), mainParentKey); // this where I set it for the next round // this should be done only if I did not do it previously
				
				// now set all the child siblings into the nextRoung
				SimpleTreeNode nodeRunner = targetNode.parent;
				nextRound.add(nodeRunner);
			}
			else
			{
				// we have come to the right point
				// woo hoo.. all that need to do at this point is
				// work through the child
				// pick the count and then 
				String key = targetNode.leaf.getKey();
				//int count = targetNode.parent.childCount.get(key);
				
				
				mainParentKey = immediateKnownParentHash.get(targetNode.leaf.getKey());
				// find the results
				Hashtable <String, Integer> curHash = results.get(mainParentKey);
				
				// I dont need to do much here other than just replace the key
				// but what if 2 of them come through what happens then ? hmm.. 
				// seems like I need to keep which child it came from too
				// interesting.. 
				int count = 0; // I have no idea why I need to add this.. anyways
				// possibly they have it already ? who knows
				if(curHash.containsKey("temp"))
					count = curHash.get("temp");
				
				// need to see if the count already exists
				// i.e. if it existed from the previous run
				if(curHash.containsKey(key))
					count = count + curHash.get(key);
				
				// remove the mainParentKey - its job is done
				curHash.remove("temp");
				
				// put it back to curHash
				curHash.put(key, count);
				
				// put it back please
				results.put(mainParentKey, curHash);
			}
		}// end of while I bet.. 
		
		// keys to drop seems to be wrong - in the last run
		for(int keyIndex = 0;keyIndex < keysToDrop.size();results.remove(keysToDrop.elementAt(keyIndex)),keyIndex++);
		
		//results = nextCountHash;
		if(nextRound.size() == 0)
			return results;
		else
			return getParentOfTypeCountMulti(nextRound, parentTypes, type, results, nextRoundImmediateParentHash);
	}

	
	
	
	public static Hashtable<String, Integer> getAllSiblingCount(SimpleTreeNode node, Hashtable<String, Integer> results)
	{
		SimpleTreeNode thisNode = node;
		while(thisNode != null)
		{
			String key = thisNode.leaf.getKey();
			Integer count = thisNode.parent.childCount.get(key);
			Integer curCounter = 0;
			if(results.containsKey(key))
				curCounter = results.get(key);
			curCounter = curCounter + count;
			results.put(key, curCounter);
			thisNode = thisNode.rightSibling;
		}
		return results;
		//results = addAllSiblings(targetNode, results);
		
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
	
	protected void incrementTransitiveFilter() {
		transitivelyFiltered++;
	}
	
	protected void decrementTransitiveFilter() {
		if(transitivelyFiltered > 0) {
			transitivelyFiltered--;
		}
	}
	
	protected void hardFilter() {
		if(hardFiltered) {
			incrementTransitiveFilter();
		} else {
			this.hardFiltered = true;
		}
	}
	
	protected void removeHardFilter() {
		if(this.hardFiltered) {
			this.hardFiltered = false;
		}
	}
	
	protected void resetFilters() {
		this.transitivelyFiltered = 0;
		this.hardFiltered = false;
	}
}

