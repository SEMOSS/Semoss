package prerna.ds;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Vector;


public class TreeNode {
	
	// gets the sibling tree
	TreeNode leftSibling = null;
	TreeNode rightSibling = null;
	TreeNode eldestLeftSibling = null; // some performance to make sure this is fast enough
	TreeNode parent = null;
	TreeNode rightChild = null;
	TreeNode leftChild = null;
	static String numberList = "";
	
	ITreeKeyEvaluatable leaf = null;
	Vector <SimpleTreeNode> instanceNode = new Vector<SimpleTreeNode>();
	
	int fanout = 5;
	
	boolean root = true;
	
	public TreeNode(ITreeKeyEvaluatable leaf)
	{
		this.leaf = leaf;
	}
	
	public void addInstance(SimpleTreeNode node)
	{
		this.instanceNode.add(node);
	}	

	public Vector<SimpleTreeNode> getInstances()
	{
		return this.instanceNode;
	}

	public boolean search(Vector <TreeNode> nodes, TreeNode searchNode, boolean found)
	{
		Vector <TreeNode> childNodes = new Vector<TreeNode>();
		for(int nodeIndex = 0;nodeIndex < nodes.size() && !found;nodeIndex++)
		{
			TreeNode node = nodes.get(nodeIndex);
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
		else
		{
			//System.err.println("found ?  " + found + searchNode.leaf.getKey());
			return found;
		}
	}
	

	public TreeNode getNode(Vector <TreeNode> nodes, TreeNode searchNode, boolean found)
	{
		TreeNode foundNode = null;
		//System.out.println("Finding Node " + searchNode.leaf.getKey());
		Vector <TreeNode> childNodes = new Vector<TreeNode>();
		for(int nodeIndex = 0;nodeIndex < nodes.size() && !found;nodeIndex++)
		{
			TreeNode node = nodes.get(nodeIndex);
			// search all siblings
			do
			{
				found = node.equal(searchNode);
				if(found)
				{
					foundNode = node;
					break;
				}
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
			return getNode(childNodes, searchNode, false);
		else
		{
			//System.err.println("found ?  " + found + searchNode.leaf.getKey());
			return foundNode;
		}
	}

	
	public synchronized TreeNode  insertData(TreeNode node)
	{
		TreeNode root = node;
		if(this.rightChild == null && this.leftChild == null) // basically means it is the base node
		{
			root = embedNode(this, node);
			// once this is done adjust it
			//balance(this);
		}
		else
		{
			// this is the case when the child are already there
			TreeNode mainNode = this;
			while(mainNode != null)
			{
				if(mainNode.equal(node))
					return root(getLeft(mainNode));
				/*else if(mainNode.leftChild == null)
				{
					mainNode.insertData(node);
				}*/
				else if(mainNode.left(node) && mainNode.leftChild != null)
				{
					return mainNode.leftChild.insertData(node);
					//break;
				}
				else if(mainNode.rightSibling == null && mainNode.rightChild != null)
				{
					return mainNode.rightChild.insertData(node);
					//break;
				}
				mainNode = mainNode.rightSibling;
			}
		}
		return root(root);
	}
	
	public TreeNode root(TreeNode node)
	{
		//System.err.println("Finding root for " + node.leaf.getKey());
		if(node.parent != null)
		{
			if(node.parent.equal(node))
			{
				System.err.println("Ok.. this is fucked up..  " + node.leaf.getKey());
				return getLeft(node);
			}
			return root(node.parent);
		}
		else return getLeft(node);
	}
	
	
	public TreeNode embedNode(TreeNode nodes, TreeNode node2Embed)
	{
		if(nodes.left(node2Embed))
		{
			// set it for the left sibling too
			if(nodes.leftSibling != null)
				nodes.leftSibling.rightSibling = node2Embed;
			// see if the current node has a left sibling
			node2Embed.leftSibling = nodes.leftSibling;
			node2Embed.eldestLeftSibling = nodes.eldestLeftSibling;
			// adjust it
			node2Embed.rightSibling = nodes;
			nodes.leftSibling = node2Embed;
			// adjust the left node for the remaining sibling
			node2Embed.parent = nodes.parent;
			

			// Logic to adjust the parents
			// set the left node as child for parent
			// need to check if it was left or right before setting it
			// this is only valid if the node2embed ended up being the first node
			// Need to adjust the right sibling here too
			// or left sibling
			if(nodes.parent != null && nodes.parent.left(node2Embed) && node2Embed.leftSibling == null)
			{
				nodes.parent.leftChild = getLeft(node2Embed);
				//setParent(node2Embed, nodes.parent);
				// there is a good chance that the parent sibling may also be having this. 
				if(nodes.parent.leftSibling != null && nodes.parent.leftSibling.rightChild != null)
				{
					// adjust the parents and child
					TreeNode parentLeftSiblingRightChild = getLeft(nodes.parent.leftSibling.rightChild);
					//setParent(parentLeftSiblingRightChild, nodes.parent);					
					nodes.parent.leftSibling.rightChild = getLeft(nodes.parent.leftSibling.rightChild);
				}
			}
			else if(nodes.parent != null && node2Embed.leftSibling == null)
			{
				nodes.parent.rightChild = getLeft(node2Embed);
				// there is a good chance that the parent sibling may also be having this. 
				if(nodes.parent.rightSibling != null && nodes.parent.rightSibling.leftChild != null)
				{
					nodes.parent.rightSibling.leftChild = getLeft(nodes.parent.rightSibling.leftChild);
					//setParent(nodes.parent.rightSibling.leftChild, nodes.parent.rightSibling);
				}
			}
			// if this replaced the left most node.. set it to the left most node
			// obviously I have ignored if the child is already there
			// if so I have to merge it
			// HUGE TODO
			
			
			// Logic to adjust the childs
			// if node2embed has a left sibling which does nto have a right child - set this case 1
			// If the node2Embed has a left sibling which has a right child
			// if the right child has space for 2 more - then insert the
			// node2embed.left child with the left siblings right child
			// else take each node and insert it back
			// same logic for right sibling			
			// set it to that
			// else set the new childs
			if(node2Embed.rightChild != null && node2Embed.rightSibling != null && node2Embed.rightSibling.leftChild == null) // easy case 1
			{
				node2Embed.rightSibling.leftChild = node2Embed.rightChild;
				setParent(node2Embed.rightChild, node2Embed.rightSibling);
			} // Need to revisit this logic BIG TIME
			
			else if(node2Embed.rightChild != null && node2Embed.rightSibling != null)
			{
				// case 2 - when the rightsibling left child has space insert
				int rightSiblingChilds = countSiblings(node2Embed.rightSibling.leftChild);
				int node2EmbedRightChildSiblings = countSiblings(node2Embed.rightChild);
				TreeNode rightChildNode = getRight(node2Embed.rightChild);
				for(int nodeIndex = rightSiblingChilds;nodeIndex < fanout && rightChildNode != null;nodeIndex++)
				{
					TreeNode thisNode = rightChildNode;
					//System.err.println("Trying to insert " + thisNode.leaf.getKey());
					//System.err.println("With " + thisNode.leftSibling.leaf.getKey());
					//thisNode.leftSibling = null;
					//if(rightChildNode.leftSibling != null)
					//	rightChildNode.leftSibling.rightSibling = null;
					rightChildNode = rightChildNode.leftSibling;					
				}
				//System.err.println("Missing Nodes ");
				//printNodes(node2Embed.rightChild);
				// case 3 - when the right sibling left child does not have space
				// whatever is remaining get the root and insert
				// grab the root and insert
			}
			if(node2Embed.leftChild != null && node2Embed.leftSibling != null && node2Embed.leftSibling.rightChild == null) // easy case 1
			{
				//setParent(node2Embed.leftChild, node2Embed.rightSibling);
				node2Embed.leftSibling.rightChild = node2Embed.leftChild;
			}
			
			else if(node2Embed.leftChild != null && node2Embed.leftSibling != null)
			{
				// case 2 - when the rightsibling left child has space insert
				// need to replace this with a merge
				int leftChildSiblings = countSiblings(node2Embed.leftSibling.rightChild);
				int node2EmbedRightChildSiblings = countSiblings(node2Embed.leftChild);
				TreeNode leftChildNode = getLeft(node2Embed.leftChild);
				for(int nodeIndex = leftChildSiblings;nodeIndex < fanout && leftChildNode != null;nodeIndex++)
				{
					TreeNode thisNode = leftChildNode;
//					/thisNode.rightSibling = null;
					//if(leftChildNode.rightSibling != null)
					//	leftChildNode.rightSibling.leftSibling = null;
					leftChildNode = leftChildNode.rightSibling;
				}
				//System.err.println("Missing Nodes ");
				//printNodes(node2Embed.leftChild);
				//printNodes(node2Embed.rightChild);
				// case 3 - when the right sibling left child does not have space
				// whatever is remaining get the root and insert
				// grab the root and insert
			}	
			balance(nodes);
			return node2Embed;
			/*
			 *TODO - this logic has to be accomodated 			// if the node2Push is left of parent adjust that too

			 */
		}
		else
		{
			if(nodes.rightSibling != null && !nodes.equal(node2Embed))
			{
				return embedNode(nodes.rightSibling, node2Embed);
			}
				
			else if(!nodes.equal(node2Embed))
			{
				nodes.rightSibling = node2Embed;
				node2Embed.leftSibling = nodes;
				node2Embed.parent = nodes.parent;
				if(node2Embed.rightChild != null && node2Embed.rightSibling != null)
					node2Embed.rightSibling.leftChild = node2Embed.rightChild;
				if(node2Embed.leftChild != null && node2Embed.leftSibling != null)
					node2Embed.leftSibling.rightChild = node2Embed.leftChild;
				
				balance(getLeft(node2Embed));
				return getLeft(node2Embed);
			}
			else
			{
				//System.out.println("Ignoring .. " + nodes.leaf.getKey());
				Vector vec = new Vector();
				vec.addElement(nodes);
				//printNode(vec, true, 1);
				return getLeft(nodes);
			}
		}

	}
	
	private void printNodes(TreeNode rightChild2) {
		// TODO Auto-generated method stub
		System.out.println(rightChild2.leaf.getKey());
		if(rightChild2.rightSibling != null)
			printNodes(rightChild2.rightSibling);
		else
			System.out.println("=====");
	}

	public boolean equal(TreeNode node2Embed) {
		// TODO Auto-generated method stub
		return this.leaf.isEqual(node2Embed.leaf);
	}

	public void balance(TreeNode node)
	{
		// travel to the left to get to the first node
		TreeNode leftNode = getLeft(node);
		// count the number of nodes
		int count = countSiblings(leftNode);
		//System.err.println("Count is..  " + count);
		int siblings = countSiblings(leftNode);
		if(siblings > fanout)
		{
			// 2 scenarios here
			// I dont have children
			// I have children
			if(!hasChildren(leftNode)) // fairly simple
			{
				printList(leftNode);
				//System.out.println("Doesn't have children");
				//System.out.println("\n....");
				int node2Pick = (fanout / 2 )+ 1;
				
				TreeNode node2Push = leftNode;
				for(int index = 1;index < node2Pick;node2Push = node2Push.rightSibling, index++);
				pushUp(leftNode, node2Push, node2Push.rightSibling);			
			}
			if(hasChildren(leftNode))
			{
				printList(leftNode);
				//System.out.println("Has children");
				//System.out.println("\n....");
				int node2Pick = (fanout / 2 )+ 1;				
				TreeNode node2Push = leftNode;
				for(int index = 1;index < node2Pick;node2Push = node2Push.rightSibling, index++);
				
				if(node2Push.leftChild != null && node2Push.leftSibling != null && node2Push.leftSibling.rightChild == null)
					node2Push.leftSibling.rightChild = node2Push.leftChild;
				if(node2Push.rightChild != null && node2Push.rightSibling != null && node2Push.rightSibling.leftChild == null)
					node2Push.rightSibling.leftChild = node2Push.rightChild;
				
				pushUp(leftNode, node2Push, node2Push.rightSibling);			
				
			}
		}
	}

	public void pushUp(TreeNode leftList, TreeNode node2Push, TreeNode rightList)
	{
		// pushup tactic
		// if a parent already there then 
		// add to parent
		
		// if parent full again 
		// do the same pushup tactic
		
		// need to accomodate the case when the node2push has a child 
		// need to take the left child and give it to the left sibling
		// need to take the right child and give it over to the right sibling
		TreeNode node2PushLeftChild = node2Push.leftChild;
		TreeNode node2PushRightChild = node2Push.rightChild;		
		
		Vector <TreeNode> nodes2BAdded = new Vector<TreeNode>();
		
		// start with removing the node first from the parent
		if(node2PushLeftChild != null)
		{
			//node2PushLeftChild.rightSibling = null;
			//node2Push.leftSibling.rightSibling = null;
			//
			//node2PushLeftChild.parent = null;
			//System.err.println("Nodes that may be missed ");
			//printNodes(node2PushLeftChild);
			// give the nodes over to the new left
			// it should not be left list it should be the left sibling
			TreeNode rightNode = node2Push.leftSibling;
			if(rightNode.rightChild != null && !rightNode.rightChild.equal(node2PushLeftChild))
			{
				//System.err.println("Core usecase..... ");
				TreeNode nextNode = node2PushLeftChild;
				while(nextNode != null)
				{
					TreeNode thisNode = nextNode;
					nextNode = nextNode.rightSibling;
					thisNode.leftSibling = null;
					thisNode.rightSibling = null;
					thisNode.parent = null;
					//thisNode.leftChild = null;
					//thisNode.rightChild = null;
					if(nextNode != null)
						nextNode.leftSibling = null;
					nodes2BAdded.add(thisNode);
					//System.err.println("Adding node "  + thisNode.leaf.getKey());
					int countLeftChild = countSiblings(leftList.rightChild);
					//if(fanout > countLeftChild)
					//	leftList.rightChild.embedNode(leftList.rightChild, thisNode);
					///else
					//	leftList.insertData(thisNode);
					
				}
				//setParent(node2PushLeftChild, rightNode);
				printNodes(node2PushLeftChild);
				printNodes(rightNode.rightChild);
			}
			else if(rightNode.rightChild.equal(node2PushLeftChild))
			{
				// I need to join the node2Push leftChild and rightchild and then set it with left and right siblings
				setParent(node2PushLeftChild, node2Push.leftSibling);
				//System.err.println("This is where I need to adjust the parents...  " + node2PushLeftChild.leaf.getKey() + " Parent " + node2PushLeftChild.parent.leaf.getKey());
			}
		}
		if(node2PushRightChild != null)
		{
			//node2Push.rightSibling.leftSibling = null;
			//node2PushRightChild.leftSibling = null;
			//node2PushRightChild.parent = null;
			if(rightList.leftChild != null && !rightList.leftChild.equal(node2PushRightChild))
			{
				
				TreeNode nextNode = node2PushRightChild;
				while(nextNode != null)
				{
					TreeNode thisNode = nextNode;
					nextNode = nextNode.rightSibling;
					System.err.println("Adding node "  + thisNode.leaf.getKey());
					thisNode.leftSibling = null;
					thisNode.rightSibling = null;
					//thisNode.parent = null;
					//thisNode.leftChild = null;
					//thisNode.rightChild = null;
					if(nextNode != null)
						nextNode.leftSibling = null;
					nodes2BAdded.add(thisNode);
					
					int countLeftChild = countSiblings(leftList.rightChild);
					//if(fanout > countLeftChild)
					//	rightList.leftChild.embedNode(rightList.leftChild, thisNode);
					//else
					//{
					//	System.out.println("Starting screwup");
					//	rightList.insertData(thisNode);
					//}
					
				}
				//setParent(node2PushRightChild, rightList);
			}
			else if(rightList.leftChild.equal(node2PushRightChild))
			{
				//System.err.println("This is where I need to adjust the parents...  " + node2PushRightChild.leaf.getKey() + " Parent " + node2PushRightChild.parent.leaf.getKey());
			}
			//System.err.println("Nodes that may be missed ");
			//printNodes(node2PushRightChild);
			// give the nodes over to the new left
		}
		
		// try to see if our data node has a parent
		// need to do the same for siblings too here
		TreeNode parent = node2Push.parent;
		if(parent != null && parent.left(leftList))
		{
			parent.leftChild = null;
			//if(parent.leftSibling != null)
			//	parent.leftSibling.rightChild = null;
		}
		else if(parent != null)
		{
			parent.rightChild = null;
			//if(parent.rightSibling != null)
			//	parent.rightSibling.leftChild = null;
		}
		setParent(leftList, node2Push);

		// set the parents
		node2Push.leftChild = leftList;
		node2Push.rightChild = rightList;
		
		// splice the linked list
		if(node2Push.leftSibling != null)
			node2Push.leftSibling.rightSibling = null;
		if(node2Push.rightSibling != null)
			node2Push.rightSibling.leftSibling = null;
		node2Push.leftSibling = null;
		node2Push.rightSibling = null;
		node2Push.parent = parent;
				
		// try to see if at the parent level the number of siblings > child nodes
		// disconnect it from the child		

		if(parent != null) 
		{			
			// first insert it
			// if there is overflow then adjust
			// if the parent is less than the number of child then insert else different strategy
			TreeNode leftMostNode = getLeft(parent);
			embedNode(leftMostNode, node2Push);
			// and we are done
			//balance(node2Push);
		}
		for(int nodeAdded = 0;nodeAdded < nodes2BAdded.size();nodeAdded++)
		{
			System.err.println("Mising  " + nodes2BAdded.elementAt(nodeAdded).leaf.getKey());
			node2Push.insertData(nodes2BAdded.elementAt(nodeAdded));
		}
		//if(nodes2BAdded.size() > 0)
		//	node2Push.insertData(nodes2BAdded.elementAt(0));
	}
	
	public int countSiblings(TreeNode counterNode)
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

	public int countChildren(TreeNode counterNode)
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

	public boolean hasChildren(TreeNode counterNode)
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

	public void printList(TreeNode node)
	{
		//System.out.print(node.leaf.getKey());
		if(node.rightSibling != null)
		{
			//System.out.print("--");
			printList(node.rightSibling);
		}
	}

	public void setParent(TreeNode leftList, TreeNode parent)
	{
		if(leftList != null && !leftList.equal(parent))
		{
			leftList.parent = parent;
			//System.err.println("Setting parent for " + leftList.leaf.getKey() + " ..... " + parent.leaf.getKey());
		}
		if(leftList.rightSibling != null)
			setParent(leftList.rightSibling, parent);
	}
	
	public TreeNode getLeft(TreeNode node)
	{
		if(node!= null && node.leftSibling != null)
			return getLeft(node.leftSibling);
		else
			return node;
	}

	public TreeNode getRight(TreeNode node)
	{
		if(node.rightSibling != null)
			return getRight(node.rightSibling);
		else
			return node;
	}
	
	public void addChild(TreeNode node)
	{
		node.parent = this;
		System.out.println("Leaf is " + node.leaf.getKey());
		if(this.leftChild != null)
		{
			TreeNode rightMost = getRight(this.leftChild);
			rightMost.rightSibling = node;
			node.leftSibling = rightMost;
		}
		else
			this.leftChild = node;
	}


	public boolean left(TreeNode node)
	{
		return this.leaf.isLeft(node.leaf);
	}
		
	
	// main method for printing the node
	public void printNode(Vector <TreeNode> nodes, boolean parent, int level)
	{
		Vector childNodes = new Vector();
		System.out.print("Level " + level);
		System.out.println();
		for(int nodeIndex = 0;nodeIndex < nodes.size();nodeIndex++)
		{
			TreeNode node = nodes.elementAt(nodeIndex);
			do{
				System.out.print(node.leaf.getKey());
				if(node.leftChild != null)
					childNodes.add(node.leftChild);
				//else
				//	childNodes.add(new TreeNode(new IntClass(0)));
				if(node.rightChild != null)
					childNodes.add(node.rightChild);
				//else
				//	childNodes.add(new TreeNode(new IntClass(0)));
				node = node.rightSibling;
				if(node != null)
					System.out.print("<>");
			}while(node != null);
			if(!parent)
				System.out.print("|");
		}
		System.out.println();
		if(childNodes.size() > 0)
			printNode(childNodes, false, level+1);
	}

	// gets all the instances
	// not just for this one
	public Vector <SimpleTreeNode> getInstanceNodes(Vector <TreeNode> nodes, Vector <SimpleTreeNode> curNodes)
	{
		Vector childNodes = new Vector();
		
		for(int nodeIndex = 0;nodeIndex < nodes.size();nodeIndex++)
		{
			TreeNode node = nodes.elementAt(nodeIndex);
			do{
				//System.out.println(">>>" + node.leaf.getValue());
				for(int instanceIndex = 0;instanceIndex < node.instanceNode.size();instanceIndex++)
					curNodes.add(node.instanceNode.elementAt(instanceIndex));
				//System.out.print(node.leaf.getKey());
				if(node.leftChild != null)
					childNodes.add(node.leftChild);
				//else
				//	childNodes.add(new TreeNode(new IntClass(0)));
				if(node.rightChild != null)
					childNodes.add(node.rightChild);
				//else
				//	childNodes.add(new TreeNode(new IntClass(0)));
				node = node.rightSibling;
			}while(node != null);
		}
		//System.out.println();
		if(childNodes.size() > 0)
			return getInstanceNodes(childNodes, curNodes);
		else
			return curNodes;
	}

	// gets all the instances
	// not just for this one
	public Vector <ISEMOSSNode> getInstanceSNodes(Vector <TreeNode> nodes, Vector <ISEMOSSNode> curNodes)
	{
		Vector childNodes = new Vector();
		
		for(int nodeIndex = 0;nodeIndex < nodes.size();nodeIndex++)
		{
			TreeNode node = nodes.elementAt(nodeIndex);
			do{
				//System.out.println(">>>" + node.leaf.getValue());
				for(int instanceIndex = 0;instanceIndex < node.instanceNode.size();instanceIndex++)
					curNodes.add((ISEMOSSNode)node.instanceNode.elementAt(instanceIndex).leaf);
				//System.out.print(node.leaf.getKey());
				if(node.leftChild != null)
					childNodes.add(node.leftChild);
				//else
				//	childNodes.add(new TreeNode(new IntClass(0)));
				if(node.rightChild != null)
					childNodes.add(node.rightChild);
				//else
				//	childNodes.add(new TreeNode(new IntClass(0)));
				node = node.rightSibling;
			}while(node != null);
		}
		//System.out.println();
		if(childNodes.size() > 0)
			return getInstanceSNodes(childNodes, curNodes);
		else
			return curNodes;
	}

	
	public static String serializeTree(String output, Vector <TreeNode> nodes, boolean parent, int level)
	{
		Vector childNodes = new Vector();
		for(int nodeIndex = 0;nodeIndex < nodes.size();nodeIndex++)
		{
			TreeNode node = nodes.elementAt(nodeIndex);
			do{
				output += node.leaf.getKey();
				if(node.leftChild != null)
					childNodes.add(node.leftChild);
				//else
				//	childNodes.add(new TreeNode(new IntClass(0)));
				if(node.rightChild != null)
					childNodes.add(node.rightChild);
				//else
				//	childNodes.add(new TreeNode(new IntClass(0)));
				node = node.rightSibling;
				if(node != null)
				{
					output += "-";
				}
			}while(node != null);
			if(!parent)
				output += "|";
		}
		System.out.println(output);
		output += "/";
		if(childNodes.size() > 0)
			return serializeTree(output, childNodes, false, level+1);
		else
			return output;
	}
	
	public static TreeNode deserializeTree(String output)
	{
		TreeNode rootNode = null;
		boolean parent = true;
		Vector <TreeNode> parentNodes = new Vector();
		// each one of this is a new line
		StringTokenizer mainTokens = new StringTokenizer(output, "/");
		while(mainTokens.hasMoreTokens())
		{
			Vector <TreeNode> nextLevel = new Vector();
			String line = mainTokens.nextToken();
			int count = 0;
			if(!parent)
			{
				// next is to zoom out the pipes
				//TreeNode curParent = parentNodes.elementAt(count);
				TreeNode curParentNode = null;
				System.out.println("Total number of parents " + parentNodes.size());
				StringTokenizer leftRightTokens = new StringTokenizer(line, "|");
				while(leftRightTokens.hasMoreTokens())
				{
					if(curParentNode == null)
					{
						curParentNode = parentNodes.elementAt(count);
						count++;
					}
					System.out.println("[" + curParentNode + "]");
					System.out.println("Cur Parent Node is " + curParentNode.leaf.getKey());
					String leftChildString = leftRightTokens.nextToken();
					String rightChildString = leftRightTokens.nextToken();
					Object [] stringOfNodes = createStringOfNodes(leftChildString, nextLevel);
					TreeNode leftNode = (TreeNode)stringOfNodes[0];
					nextLevel = (Vector<TreeNode>)stringOfNodes[1];
					// set the parent here
					// do the right node only if the parent has no sibling kind of
					stringOfNodes = createStringOfNodes(rightChildString, nextLevel);
					TreeNode rightNode = (TreeNode)stringOfNodes[0];
					nextLevel = (Vector<TreeNode>)stringOfNodes[1];

					curParentNode.leftChild = leftNode;
					leftNode.parent = curParentNode;
					rightNode.parent = curParentNode;
					
					if(curParentNode.leftSibling != null)
						curParentNode.leftSibling.rightChild = curParentNode.leftChild;
					if(curParentNode.rightSibling == null)
						curParentNode.rightChild = rightNode;
					// move on next
					curParentNode = curParentNode.rightSibling;
				}
			}
			else
			{
				System.out.println("Parent.. ");
				Object [] stringOfNodes = createStringOfNodes(line, nextLevel);
				rootNode = (TreeNode)stringOfNodes[0];
				nextLevel = (Vector<TreeNode>)stringOfNodes[1];
				System.out.println("Next level is " + nextLevel.get(0).leaf.getKey());
				parent = false;
			}
			parentNodes = nextLevel;
		}
		return rootNode;
	}
	
	public static Object [] createStringOfNodes(String childString, Vector <TreeNode> inVector)
	{
		// nasty.. I dont have a proper object eeks
		Object [] retObject = new Object[2];
		// final loop is the <> loop
		StringTokenizer leftString = new StringTokenizer(childString, "-");
		TreeNode leftNode = null;
		while(leftString.hasMoreElements())
		{
			String leftNodeKey = leftString.nextToken();
			TreeNode node = new TreeNode(new IntClass(Integer.parseInt(leftNodeKey)));
			if(leftNode == null)
			{
				leftNode = node;
				retObject[0] = node;
				inVector.addElement(node);
				System.out.println("Adding Key " + leftNodeKey);
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
	
	public static TreeNode join(Vector <TreeNode> fromNodes, Vector <TreeNode> toNodeRoot,boolean parent, TreeNode resNodeRoot)
	{
		Vector childNodes = new Vector();
		for(int nodeIndex = 0;nodeIndex < fromNodes.size();nodeIndex++)
		{
			TreeNode node = fromNodes.elementAt(nodeIndex);
			do{
				//output += node.leaf.getKey();
				if(toNodeRoot.get(0).search(toNodeRoot, node, false))
				{
					TreeNode newNode = new TreeNode(node.leaf);
					if(resNodeRoot == null)
						resNodeRoot = newNode;
					else
						resNodeRoot = resNodeRoot.insertData(newNode);
				}
				if(node.leftChild != null)
					childNodes.add(node.leftChild);
				//else
				//	childNodes.add(new TreeNode(new IntClass(0)));
				if(node.rightChild != null)
					childNodes.add(node.rightChild);
				//else
				//	childNodes.add(new TreeNode(new IntClass(0)));
				node = node.rightSibling;
			}while(node != null);
		}
		if(childNodes.size() > 0)
			return join(childNodes, toNodeRoot, false, resNodeRoot);
		else
			return resNodeRoot;
	}
	
	public static void tryFlatten()
	{
		StringClass cap = new StringClass("Lab");
		StringClass bp1 = new StringClass("AnatomicPath");
		StringClass bp2 = new StringClass("ClinicalPath");
		
		TreeNode capt = new TreeNode(cap);
		TreeNode bp1t = new TreeNode(bp1);
		TreeNode bp2t = new TreeNode(bp2);
		TreeNode bp3 = new TreeNode(bp1);
		TreeNode bp4 = new TreeNode(bp1);
		
		bp3.addChild(bp4);
		bp1t.addChild(bp3);
		
		capt.addChild(bp1t);
		capt.addChild(bp2t);

		Vector rootNodes = new Vector();
		rootNodes.add(capt);
		
		

		capt.printNode(rootNodes, true, 1);
		
		capt.flattenRoots(capt, false);
	}
	
	

	public static void main(String [] args) throws Exception
	{
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		
		Vector rootNodes = new Vector();
		IntClass val = new IntClass(20);
		TreeNode root = new TreeNode((ITreeKeyEvaluatable)val);
		
		/*String numbers = "23:41:85:46:64:13:89:55:97:29:18:77:65";
		numbers = "733:401:141:558:578:650:260:436:243:153:312:455:808:470:648:425:44:532:813:620:895:777:37:5:737:366:386:988:213:703:602:454:805:306:942:205:736:121:154:980:239:775:25:136:702:853:847:708:713:260:510:29:563:890:882:273:342:341:601:323:357:153:375:133:256:127:439:302:865:577:847:951:504:6:890:95:199:569:19:630:43:871:333:719:687:103:853:863:805:231:509:153:196:11:41:283:110:861:749:658:254:228:366:578:920:920:148:824:626:56:642:143:692:667:503:55:193:706:162:145:825:220:79:477:289:671:864:77:975:987:516:479:855:703:627:163:714:701:456:376:57:438:421:195:315:207:688:896:229:955:226:674:645:415:898:483:283:789:927:327:433:82:316:624:959:200:625:367:410:329:856:688:548:735:41:82:81:54:537:980:959:343:979:604:125:649:269:999:266:613:16:962:683:692:316:117:118:890:874:754:39:508:999:153:624:324:165:49:442:768:461:557:614:127:437:355:75:542:557:188:383:578:735:757:430:452:180:905:317:559:96:578:585:845:610:439:14:652:686:146:143:147:667:271:790:99:510:525:833:799:777:491:692:137:891:310:31:447:228:864:397:513:155:825:697:285:209:562:633:201:230:326:497:723:438:297:994:616:68:59:95:773:448:829:277:850:115:269:880:839:840:63:730:726:252:500:636:790:83:206:823:695:904:800:895:501:667:723:441:869:249:662:991:266:679:45:608:721:740:810:58:729:116:267:139:239:358:173:485:266:503:297:835:564:297:641:576:959:639:183:105:410:29:462:417:587:522:600:149:785:928:365:216:278:784:702:73:807:905:762:599:749:196:821:563:656:735:634:642:283:61:744:15:926:31:929:616:855:557:524:223:943:42:597:724:73:353:56:27:805:723:579:312:834:63:896:533:198:778:378:326:866:349:35:803:263:592:38:1:560:747:605:117:53:275:630:57:748:525:747:218:371:232:614:890:353:147:97:136:12:971:643:507:50:154:635:501:701:71:682:239:361:525:843:447:262:526:529:442:850:849:423:507:808:876:83:732:310:860:21:113:971:847:967:480:115:149:704:590:479:420:343:578:148:142:141:764:598:526:20:742:958:631:727:571:232:814:209:831:57:442:113:293:712:179:512:966:531:61:870:300:568:991:0:430:347:264:254:418:803:212:475:605:50:86:304:326:475:857:270:471:730:691:736:576:604:477:165:387:441:950:357:152:695:601:298:192:471:810:982:669:419:355:136:854:123:753:58:600:231:660:883:186:923:925:491:166:951:740:892:374:412:743:377:761:620:850:963:333:931:53:620:41:132:435:593:124:60:145:662:57:454:155:548:961:625:207:792:64:461:461:339:542:423:19:757:883:658:677:467:720:88:963:654:940:348:769:512:760:750:661:319:368:15:244:436:689:399:812:681:796:955:873:686:500:112:35:824:711:455:180:941:197:562:799:705:510:42:882:611:953:452:740:766:634:591:735:431:872:808:946:905:400:327:453:748:120:267:15:542:422:11:675:341:413:783:959:725:745:835:140:889:135:417:272:537:62:822:531:417:578:104:720:237:665:761:671:328:55:568:677:892:981:954:12:866:904:803:69:448:847:335:619:106:622:756:250:471:883:709:71:821:642:150:364:43:612:188:993:821:504:585:17:510:182:69:139:619:420:346:332:79:840:725:236:505:500:555:683:469:529:260:14:400:124:606:701:106:231:0:131:64:227:15:345:565:947:66:913:522:504:115:120:247:427:597:122:792:772:247:747:504:52:394:181:285:8:361:864:699:468:13:543:442:817:180:560:899:878:606:164:485:324:351:821:198:104:43:252:367:746:228:200:881:800:88:740:268:161:207:269:903:456:677:185:235:211:766:686:405:52:582:11:817:746:634:932:241:516:988:314:796:680:725:30:742:341:775:651:343:536:776:830:180:4:538:513:227:457:822:658:666:66:633:422:895:670:942:405:447:70:777:841:540:670:916:372:829:629:996:345:495:624:718:435:194:334:951:731:555:736:996:346:835:342:925:30:75:605:954:239:592:795:14:587:937:796:294:656:492:436:300:671:742:158:131:658:882:61:42:296:810:623:33:844:771:668:176:196:857:499:609:329:354:452:651:148:424:507:293:21:944:18:391:81:424:70:246:737:777:431:919:666:395:605:519:0:422:998:926:322:305:818:908:558:762:896:317:843:16:270:147:969:775:41:527:68:744:694:350:786:576:809:134:300:122:849:869:196:457:502:66:414:708:877:974:142:889:683:915:837:35:456:14:273:708:752:172:854:554:219:974:271:966:409:396:913:66:217:575:386:156:863:607:895:495:415:352:224:800:933:450:519:87:485:344:134:607";
		
		numbers="706:536:654:625:479:894:674:465:484:54:712:866:296:361:431:823:804:132:724:761:228:192:366:630:487:507:296:288:299:970:64:475:821:233:760:364:932:329:727:942:260:716:664:235:879:268:322:695:208:724:832:929:0:36:854:261:479:114:174:605:618:457:53:454:278:285:640:850:405:826:16:625:807:613:988:249:41:577:194:841:373:266:786:105:608:446:134:871:888:187:908:932:378:412:366:659:933:784:298:398:163:127:249:311:628:22:691:860:610:265:608:209:327:268:287:728:947:97:992:397:271:787:586:385:490:759:99:277:986:666:706:428:760:747:567:707:703:829:504:460:575:433:564:851:285:60:63:897:754:627:879:376:431:180:363:519:314:481:443:845:144:802:205:733:286:256:477:408:733:319:700:769:520:754:717:129:845:28:892:294:129:434:391:559:277:567:327:154:430:511:901:386:581:55:649:212:520:561:396:281:711:610:515:530:870:427:686:271:215:152:415:335:559:456:173:238:729:644:214:519:22:352:755:402:774:192:462:626:460:184:888:880:111:656:816:560:273:327:246:657:160:684:113:487:146:382:675:594:232:334:939:644:681:351:382:290:582:685:733:39:688:598:537:915:540:104:761:255:523:403:761:363:214:84:518:837:100:118:150:303:522:793:379:985:451:221:809:0:573:773:559:521:975:815:77:988:622:424:706:354:738:329:880:948:126:195:691:920:470:491:903:385:647:642:406:313:126:822:536:960:607:764:640:88:143:223:764:565:141:266:359:944:774:404:468:495:570:677:936:785:130:219:799:308:228:468:167:847:798:178:593:232:659:591:127:901:178:671:4:205:435:86:190:721:909:696:799:902:98:688:771:317:16:118:5:342:980:563:619:636:655:755:858:934:907:616:712:713:586:809:230:39:37:11:86:496:755:450:994:421:341:812:149:811:610:568:0:161:475:917:308:194:31:985:616:900:52:693:976:565:9:671:25:495:565:958:263:957:439:731:708:448:467:242:927:789:673:219:817:692:162:465:405:385:699:43:377:597:902:461:983:730:77:516:501:278:863:0:416:677:20:326:68:563:883:210:586:76:237:958:551:224:631:575:986:554:424:864:934:292:202:914:122:912:528:912:923:595:397:988:133:370:167:892:866:822:189:667:200:117:834:118:107:753:131:797:562:913:758:427:684:464:709:195:207:693:560:75:585:635:891:416:548:404:77:356:991:294:382:214:657:551:982:944:201:592:336:849:866:175:88:271:183:469:525:658:817:225:448:453:938:297:223:541:250:556:652:565:122:852:313:285:126:33:926:299:612:218:363:868:884:436:935:276:967:458:673:171:893:180:400:566:965:162:563:750:439:26:919:187:665:215:387:573:782:301:615:631:681:378:34:896:133:310:501:683:609:540:929:675:583:945:668:91:804:729:770:178:335:688:703:856:943:235:519:714:769:502:945:181:66:438:401:629:198:311:467:369:39:196:900:984:336:69:911:201:629:886:26:577:833:606:554:573:630:440:340:217:426:760:239:447:736:990:504:405:513:491:41:375:101:947:352:846:244:958:800:148:772:638:733:435:414:788:913:360:445:822:645:644:750:2:985:899:231:700:392:452:393:320:255:674:372:267:789:246:643:824:887:105:102:90:656:343:297:199:540:152:88:877:106:43:124:22:821:932:155:414:389:378:314:819:514:204:222:746:285:963:953:876:704:250:647:668:307:612:850:16:184:953:427:896:218:625:280:808:769:89:931:510:610:916:450:947:395:132:128:512:559:375:772:774:135:100:476:599:595:143:246:138:510:230:939:506:507:697:520:111:723:514:802:498:730:914:450:236:691:389:238:887:309:512:227:316:17:106:818:641:713:544:371:946:267:566:398:456:986:843:189:268:181:668:79:875:996:583:476:668:712:437:186:310:786:368:559:394:7:481:444:57:281:500:980:311:712:560:812";
		StringTokenizer tokenz = new StringTokenizer(numbers, ":");
		while(tokenz.hasMoreTokens())
		{
			int inserter = Integer.parseInt(tokenz.nextToken());
			//System.out.println("Processing " + inserter);
			Vector nodes = new Vector();
			//nodes.add(root);
			//boolean found = root.search(nodes, new TreeNode(new IntClass(inserter)), false);
			//if(i%100 == 0)
			//if(!found)
			root = root.insertData(new TreeNode(new IntClass(inserter)));
			//wait(reader,root);
			//wait(reader,root);				
		}
		wait(reader,root);
		//int inserter = Integer.parseInt(tokenz.nextToken());
		int inserter = 577;
		root = root.insertData(new TreeNode(new IntClass(inserter)));
		//System.out.println("Processing 97" + inserter);
		Vector nodes = new Vector();
		nodes.add(root);
		wait(reader,root);
		//numbers = "654";
		tokenz = new StringTokenizer(numbers, ":");
		String numbersNotThere = "";
		while(tokenz.hasMoreElements())
		{
			Vector nodes2 = new Vector();
			nodes2.add(root);
			String num = tokenz.nextToken();
			//System.out.println("Searching " + num);
			TreeNode searchNode = new TreeNode(new IntClass(Integer.parseInt(num)));
			
			boolean found = root.search(nodes2, searchNode, false);
			if(!found)
				numbersNotThere = numbersNotThere + ": " + num;
		}
		
		System.out.println("Numbers not there " + numbersNotThere);
		searchIt(nodes, reader);
		//boolean found = root.search(nodes, new TreeNode(new IntClass(inserter)), false);
		//if(!found)
			root = root.insertData(new TreeNode(new IntClass(inserter)));
		*/				
		
		
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		long time1 = System.nanoTime();
		long mtime1 = System.currentTimeMillis();
		//System.out.println("Start >> Nano " + dateFormat.format(date));
		System.out.println("Start >> Nano " + time1);
		System.out.println("Total memory...  " + Runtime.getRuntime().maxMemory()/ 1000 + "kb");

		System.out.println("Available memory...  " + Runtime.getRuntime().freeMemory());
		//System.out.println(new DateTim)
		int count = 1;
		for (int i =0;i < 15;i++)
		{
			
			//int inserter = (int) ((Math.random() * 1000)%1000);
			int inserter = i;
			numberList = numberList + ":" + inserter;
			//System.out.print(">i:" + i + "<");
			
			Vector nodes = new Vector();
			nodes.add(root);
			boolean found = false;
			found = root.search(nodes, new TreeNode(new IntClass(inserter)), false);
			if(i%1000 == 0)
			{
				System.out.println("Available memory...  " + count + ">> " + Runtime.getRuntime().freeMemory() / 1000 + "kb");
				//wait(reader, root);
			}	
			//if(found)
				root = root.insertData(new TreeNode(new IntClass(inserter)));
				//System.err.println("Numbers " + numberList);
			count++;//else
			//	System.out.println("Search failed for " + inserter);
		}
		date = new Date();
		long mtime2= System.currentTimeMillis();
		long time2 = System.nanoTime();
		//System.out.println("End  " + dateFormat.format(date));
		System.out.println("Available memory...  " + Runtime.getRuntime().freeMemory());
		System.out.println("Start >> Nano " + time2);
		System.out.println("Time Spent... Milliseconds " + (time2 - time1) / 1000000);
		System.out.println("Time Spent... seconds " + (mtime2 - mtime1)/1000);
		//System.out.println("Time Spent... seconds " + (time2 - time1));
		//wait(reader,root);				
		Vector nodes = new Vector();
		nodes.add(root);
		
		// enable search
		//searchIt(nodes, reader);
		TreeNode leaf = root.getLeaf(root, true);
		System.out.println("leaf is " + leaf.leaf.getKey());
		
		
		
		//root.printRecords(root);
		//root.flattenRoots(root, true);
		System.out.println("----");
		String serializedOutput = root.serializeTree("", nodes, true, 1);
		System.out.println("Output of serialized ..  "  + serializedOutput);
		
		// now try to deserialize
		TreeNode rootNode = root.deserializeTree(serializedOutput);
		nodes = new Vector();
		nodes.add(rootNode);
		System.out.println("Sweetness..... ");
		wait(reader, rootNode);
		

		System.out.println("-------");
		tryFlatten();
		
		//wait(reader, root);
		
		
		//root = root.insertData(node);
		//rootNodes.add(root);
		//root.printNode(rootNodes, true);
		
		/*
		Vector rootNodes = new Vector();
		rootNodes.add(root);
		root.printNode(rootNodes, true);

		//System.out.println("Root is " + root.leaf.getKey());
		//System.out.println("Root is Final" + root.printTree(root, ""));
		*/
	}
	
	public static void searchIt(Vector <TreeNode> nodes, BufferedReader reader) throws Exception
	{
		String data = null;
		TreeNode root = nodes.elementAt(0);
		while(!((data = reader.readLine()).equalsIgnoreCase("stop")))
		{
			Vector <TreeNode> vec = new Vector();
			vec.add(root);
			DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			Date date = new Date();
			long time1 = System.nanoTime();
			long mtime1 = System.currentTimeMillis();
			//System.out.println("Start >> Nano " + dateFormat.format(date));
			System.out.println("Start >> Nano " + time1);
			System.out.println(" Finding  " + data + "    result " + root.search(vec, new TreeNode(new IntClass(Integer.parseInt(data))), false));
			date = new Date();
			long mtime2= System.currentTimeMillis();
			long time2 = System.nanoTime();
			//System.out.println("End  " + dateFormat.format(date));
			System.out.println("Available memory...  " + Runtime.getRuntime().freeMemory());
			System.out.println("Start >> Nano " + time2);
			System.out.println("Time Spent... Milliseconds " + (time2 - time1) / 1000000);
			System.out.println("Time Spent... seconds " + (mtime2 - mtime1)/1000);
			System.out.print("Enter : ");
		}
	}
	
	public TreeNode getLeaf(TreeNode node, boolean left)
	{
		if(node.leftChild != null && left)
			return getLeaf(node.leftChild, true);
		if(node.rightSibling == null && node.rightChild != null && !left)
			return getLeaf(node.rightChild, false);
		else return node;
	}

		
	// x
	public void flattenTree(Vector<TreeNode> parentNodeList, TreeNode node)
	{
		if(node.leftChild != null && node.rightChild != null)
		{
			if(node.leftChild != null)
			{
				Vector <TreeNode> newList = getNewVector(parentNodeList);
				//Collections.copy(newList, parentNodeList);
				newList.add(node);
				TreeNode leftChild = node.leftChild;
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
				TreeNode leftChild = node.rightChild;
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
			for(int idx = 0;idx < parentNodeList.size();idx++)
				System.err.print(">>" + parentNodeList.get(idx).leaf.getKey() + ":");
			System.err.print(node.leaf.getKey());
			System.err.println("----");

		}
	}

	public void flattenTree2(List<String> table, TreeNode node)
	{
		if(node.leftChild != null)
		{
			table.add(node.leaf.getKey());
			TreeNode leftChild = node.leftChild;
			while(leftChild != null)
			{
				flattenTree2(table, leftChild);
				leftChild = leftChild.rightSibling;
			}
		}
	}

	public void flattenUnBalancedTree(Vector<TreeNode> parentNodeList, TreeNode node)
	{
			if(node.leftChild != null)
			{
				Vector <TreeNode> newList = getNewVector(parentNodeList);
				//Collections.copy(newList, parentNodeList);
				newList.add(node);
				TreeNode leftChild = node.leftChild;
				while(leftChild != null)
				{
					flattenUnBalancedTree(newList, leftChild);
					leftChild = leftChild.rightSibling;
				}
			}
			else
			{
				//System.out.println("Current Array is " + parentNodeList.size());
				for(int idx = 0;idx < parentNodeList.size();idx++)
					System.err.print(">>" + parentNodeList.get(idx).leaf.getKey() + ":");
				System.err.print(node.leaf.getKey());
				System.err.println("----");
	
			}
	}

	public void flattenRoots(TreeNode node, boolean balanced)
	{
		while(node != null)
		{
			if(balanced)
				flattenTree(new Vector(), node);
			else
				flattenUnBalancedTree(new Vector(), node);
			node = node.rightSibling;
		}
	}
	
	public TreeNode mergeTrees(TreeNode fromNode, TreeNode toNode, boolean inner)
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
	
	public static void wait(BufferedReader reader, TreeNode node) throws Exception
	{
		System.out.println(">>");
		//reader.readLine();
		Vector rootNodes = new Vector();
		rootNodes.add(node);
		node.printNode(rootNodes, true, 1);
		System.out.println("Numbers >>  " + numberList);

	}
}
