package prerna.ds;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Vector;


public class TreeNode {
	
	TreeNode leftSibling = null;
	TreeNode parent = null;
	TreeNode rightChild = null;
	TreeNode leftChild = null;
	TreeNode rightSibling = null;
	
	TreeNode eldestLeftSibling = null; // some performance to make sure this is fast enough
	static String numberList = "";
	
	ITreeKeyEvaluatable leaf = null;
	Vector <SimpleTreeNode> instanceNode = new Vector<SimpleTreeNode>();
	Vector <SimpleTreeNode> filteredInstanceNode = new Vector<SimpleTreeNode>();
	
	private static final int fanout = 5;
	
//	boolean root = true;
	
	/**
	 * 
	 * @param leaf
	 */
	public TreeNode(ITreeKeyEvaluatable leaf) {
		this.leaf = leaf;
	}
	
	/**
	 * 
	 * @param node
	 */
	public void addInstance(SimpleTreeNode node) {
		this.instanceNode.add(node);
	}	
	
	/**
	 * 
	 * @param node
	 */
	public void addFilteredInstance(SimpleTreeNode node) {
		this.filteredInstanceNode.add(node);
	}	

	/**
	 * 
	 * @return
	 */
	public Vector<SimpleTreeNode> getInstances() {
		return this.instanceNode;
	}
	
	public Vector<SimpleTreeNode> getFilteredInstances() {
		return this.filteredInstanceNode;
	}

	/**
	 * 
	 * @param nodes 
	 * @param searchNode
	 * @param found
	 * @return
	 */
	public boolean search(Vector <TreeNode> nodes, TreeNode searchNode, boolean found) {
		
		Vector <TreeNode> childNodes = new Vector<TreeNode>();
		for(int nodeIndex = 0; nodeIndex < nodes.size() && !found; nodeIndex++) {
			TreeNode node = nodes.get(nodeIndex);
			
			// search all siblings
			do {
				
				if(node.equal(searchNode)) {
					if(node.instanceNode.size() + node.filteredInstanceNode.size() > 0) {
						return true;
					} else {
						return false;
					}
				}
				
				node = node.rightSibling;
			} while(node != null);
			
			// if not found then go for the one where it is the first left
			// and then add that child node and move
			node = nodes.get(nodeIndex);
			
			// find where this node lands
			// same logic as embed
			while(node != null)	{
				// see if the node is less than this current node, then add the left child
				if(node.left(searchNode) && node.leftChild != null) {
					childNodes.add(node.leftChild);
					break;
				}
				
				else if(!found && node.rightSibling == null && node.rightChild != null) {
					childNodes.add(node.rightChild );
					break;
				}
				
				node = node.rightSibling;
			}
		}
		if(!found && childNodes.size() > 0) {
			return search(childNodes, searchNode, false);
		} else {
			return found;
		}
	}
	

//	public TreeNode getNode(ISEMOSSNode value) {
//		return null;
//	}
	/**
	 * 
	 * @param nodes
	 * @param searchNode
	 * @param found
	 * @return
	 */
	public TreeNode getNode(Vector <TreeNode> nodes, TreeNode searchNode, boolean found)
	{
		TreeNode foundNode = null;

		Vector <TreeNode> childNodes = new Vector<TreeNode>();
		for(int nodeIndex = 0;nodeIndex < nodes.size() && !found;nodeIndex++) {
			TreeNode node = nodes.get(nodeIndex);
			// search all siblings
			do {
				
				if(node.equal(searchNode)) {
					if(node.instanceNode.size() + node.filteredInstanceNode.size() > 0) {
						return node;
					} else {
						return null;
					}
				}
				
				node = node.rightSibling;
			} while(node != null);
			
			// if not found then go for the one where it is the first left
			// and then add that child node and move
			node = nodes.get(nodeIndex);
			
			// find where this node lands
			// same logic as embed
			while(node != null) {
				
				//System.out.print(node.leaf.getKey());
				// see if the node is less than this current node, then add the left child
				if(node.left(searchNode) && node.leftChild != null) {
					childNodes.add(node.leftChild);
					break;
				}
				
				else if(!found && node.rightSibling == null && node.rightChild != null) {
					childNodes.add(node.rightChild );
					break;
				}
				
				node = node.rightSibling;
			}
		}
		
		if(!found && childNodes.size() > 0) {
			return getNode(childNodes, searchNode, false);
		}
		else {
			return foundNode;
		}
	}


	/**
	 * 
	 * @param node
	 * @return
	 */
	public static TreeNode refresh(TreeNode node, boolean fullRefresh) {
		
		TreeNode rootNode = node.root(node);
		
		if(!fullRefresh && rootNode.instanceNode.size() > 0) {
			return rootNode;
		}
		
		IndexTreeIterator it = new IndexTreeIterator(rootNode);
		
		TreeNode newRoot = new TreeNode(null);
		while(it.hasNext()) {
			TreeNode nextNode = it.next();
			if(nextNode.instanceNode.size() > 0 || nextNode.filteredInstanceNode.size() > 0) {
				if(newRoot.leaf == null) {
					newRoot = nextNode;
				} else {
					newRoot = newRoot.insertData(nextNode);
				}
			}
		}
		
		return newRoot;
	}
	
	public boolean equal(TreeNode node2Embed) {
		return this.leaf.isEqual(node2Embed.leaf);
	}
	
	/**
	 * 
	 * @param node
	 * @return
	 */
	public TreeNode root(TreeNode node) {	
		while(node.parent != null) {
			node = node.parent;
		}
		node = getLeft(node);
		return node;
	}
	
	public void addChild(TreeNode node)
	{
		node.parent = this;
		if(this.leftChild != null)
		{
			TreeNode rightMost = getRight(this.leftChild);
			rightMost.rightSibling = node;
			node.leftSibling = rightMost;
		}
		else
			this.leftChild = node;
	}
	
	public synchronized TreeNode insertData(TreeNode node)
	{
		if(node.instanceNode.size()+node.filteredInstanceNode.size() == 0) {
			throw new IllegalArgumentException("TreeNode has no SimpleTreeNodes");
		}
		
		TreeNode root = node;
		if(this.rightChild == null && this.leftChild == null) // basically means it is the base node
		{
			root = embedNode(this, node);
			// once this is done adjust it
			//balance(this);
		}
		else {
			// this is the case when the child are already there
			TreeNode mainNode = this;
			while(mainNode != null) {
				
				if(mainNode.equal(node)) {
					return root(getLeft(mainNode));
				}
				
				else if(mainNode.left(node) && mainNode.leftChild != null) {
					return mainNode.leftChild.insertData(node);
				}
				
				else if(mainNode.rightSibling == null && mainNode.rightChild != null) {
					return mainNode.rightChild.insertData(node);
				}
				
				mainNode = mainNode.rightSibling;
			}
		}
//		root = root(root);
//		if(root.instanceNode.size() == 0 && root.filteredInstanceNode.size() ==0) {
//			root = refresh(root);
//		}
		return root(root);
	}
	
	private TreeNode embedNode(TreeNode nodes, TreeNode node2Embed)
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

	private void balance(TreeNode node)
	{
		// travel to the left to get to the first node
		TreeNode leftNode = getLeft(node);
		// count the number of nodes
		//int count = countSiblings(leftNode);
		//System.err.println("Count is..  " + count);
		int siblings = countSiblings(leftNode);
		if(siblings > fanout)
		{
			// 2 scenarios here
			// I dont have children
			// I have children
			if(!hasChildren(leftNode)) // fairly simple
			{
				//printList(leftNode);
				//System.out.println("Doesn't have children");
				//System.out.println("\n....");
				int node2Pick = (fanout / 2 )+ 1;
				
				TreeNode node2Push = leftNode;
				for(int index = 1;index < node2Pick;node2Push = node2Push.rightSibling, index++);
				pushUp(leftNode, node2Push, node2Push.rightSibling);			
			}
			if(hasChildren(leftNode))
			{
				//printList(leftNode);
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

	private void pushUp(TreeNode leftList, TreeNode node2Push, TreeNode rightList)
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
					if(nextNode != null)
						nextNode.leftSibling = null;
					nodes2BAdded.add(thisNode);

					int countLeftChild = countSiblings(leftList.rightChild);

					
				}

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

					if(nextNode != null)
						nextNode.leftSibling = null;
					nodes2BAdded.add(thisNode);
					
					int countLeftChild = countSiblings(leftList.rightChild);

					
				}

			}
			else if(rightList.leftChild.equal(node2PushRightChild))
			{
				//System.err.println("This is where I need to adjust the parents...  " + node2PushRightChild.leaf.getKey() + " Parent " + node2PushRightChild.parent.leaf.getKey());
			}
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

	}

	protected boolean left(TreeNode node)	{
		return this.leaf.isLeft(node.leaf);
	}


	/**
	 * Currently unuseable because Index Tree will lose all references to Value Tree when deserialized
	 * 
	 * @param root
	 * @return
	 */
	public static String serializeTree(TreeNode root) {
		Vector<TreeNode> nodes = new Vector<TreeNode>();
		nodes.add(root);
		return TreeNode.serializeTree(new StringBuilder(""), nodes, true, 0);
	}
	

	/**
	 * Currently unuseable because Index Tree will lose all references to Value Tree when deserialized
	 * 
	 * @param output
	 * @param nodes
	 * @param parent
	 * @param level
	 * @return
	 */
	private static String serializeTree(StringBuilder output, Vector<TreeNode> nodes, boolean parent, int level) {
		Vector<TreeNode> childNodes = new Vector<>();
		for(int nodeIndex = 0; nodeIndex < nodes.size(); nodeIndex++) {
			TreeNode node = nodes.elementAt(nodeIndex);
			
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
		
		if(childNodes.size() > 0) {
			return serializeTree(output, childNodes, false, level + 1);
		}
		else {
			return output.toString();
		}
	}
	
	/**
	 * Deserializes Index Tree serialization. Currently unuseable because Index Tree will lose all references to Value Tree when deserialized
	 * 
	 * @param serializedTree
	 * @return
	 */
	public static TreeNode deserializeTree(String serializedTree) {
		TreeNode rootNode = null;
		boolean parent = true;
		Vector<TreeNode> parentNodes = new Vector<TreeNode>();
		// each one of this is a new line
		String[] mainTokens = serializedTree.split("/{3}(?!/)");
		for (String line : mainTokens) {
			Vector<TreeNode> nextLevel = new Vector<TreeNode>();
			int count = 0;
			if (!parent) {
				TreeNode curParentNode = null;
				String[] leftRightTokens = line.split("\\|{3}(?!\\|)");
				for (int i = 0; i < leftRightTokens.length; i = i + 2) {
					// for (String leftChildString : leftRightTokens) {
					if(curParentNode == null)
					{
						curParentNode = parentNodes.elementAt(count);
						count++;
					}
					Object[] leftStringOfNodes = createStringOfNodes(leftRightTokens[i], nextLevel);
					Object[] rightStringOfNodes = createStringOfNodes(leftRightTokens[i + 1], nextLevel);
					TreeNode leftNode = (TreeNode)leftStringOfNodes[0];
					TreeNode rightNode = (TreeNode) rightStringOfNodes[0];
					nextLevel = (Vector<TreeNode>) leftStringOfNodes[1];

					// set the parent here
					// do the right node only if the parent has no sibling kind of
					curParentNode.leftChild = leftNode;
					curParentNode.rightChild = rightNode;
					leftNode.parent = curParentNode;

					// move on next
					curParentNode = curParentNode.rightSibling;
				}
			}
			else
			{
				Object [] stringOfNodes = createStringOfNodes(line, nextLevel);
				rootNode = (TreeNode)stringOfNodes[0];
				nextLevel = (Vector<TreeNode>)stringOfNodes[1];
				parent = false;
			}
			parentNodes = nextLevel;
		}
		// StringTokenizer mainTokens = new StringTokenizer(serializedTree, "/");
		// while(mainTokens.hasMoreTokens())
		// {
		// Vector <TreeNode> nextLevel = new Vector();
		// String line = mainTokens.nextToken();
		// int count = 0;
		// if(!parent)
		// {
		// // next is to zoom out the pipes
		// //TreeNode curParent = parentNodes.elementAt(count);
		// TreeNode curParentNode = null;
		// //System.out.println("Total number of parents " + parentNodes.size());
		// StringTokenizer leftRightTokens = new StringTokenizer(line, "|");
		// while(leftRightTokens.hasMoreTokens())
		// {
		// if(curParentNode == null)
		// {
		// curParentNode = parentNodes.elementAt(count);
		// count++;
		// }
		// //System.out.println("[" + curParentNode + "]");
		// //System.out.println("Cur Parent Node is " + curParentNode.leaf.getKey());
		// String leftChildString = leftRightTokens.nextToken();
		// String rightChildString = leftRightTokens.nextToken();
		// Object [] stringOfNodes = createStringOfNodes(leftChildString, nextLevel);
		// TreeNode leftNode = (TreeNode)stringOfNodes[0];
		// nextLevel = (Vector<TreeNode>)stringOfNodes[1];
		// // set the parent here
		// // do the right node only if the parent has no sibling kind of
		// stringOfNodes = createStringOfNodes(rightChildString, nextLevel);
		// TreeNode rightNode = (TreeNode)stringOfNodes[0];
		// nextLevel = (Vector<TreeNode>)stringOfNodes[1];
		//
		// curParentNode.leftChild = leftNode;
		// leftNode.parent = curParentNode;
		// rightNode.parent = curParentNode;
		//
		// if(curParentNode.leftSibling != null)
		// curParentNode.leftSibling.rightChild = curParentNode.leftChild;
		// if(curParentNode.rightSibling == null)
		// curParentNode.rightChild = rightNode;
		// // move on next
		// curParentNode = curParentNode.rightSibling;
		// }
		// }
		// else
		// {
		// //System.out.println("Parent.. ");
		// Object [] stringOfNodes = createStringOfNodes(line, nextLevel);
		// rootNode = (TreeNode)stringOfNodes[0];
		// nextLevel = (Vector<TreeNode>)stringOfNodes[1];
		// //System.out.println("Next level is " + nextLevel.get(0).leaf.getKey());
		// parent = false;
		// }
		// parentNodes = nextLevel;
		// }
		return rootNode;
	}

	

	//**********************UTILITY METHODS*******************//
	
	private static Object [] createStringOfNodes(String childString, Vector <TreeNode> inVector)
	{
		// nasty.. I dont have a proper object eeks
		Object[] retObject = new Object[2];
		// final loop is the <> loop
		String[] leftString = childString.split("@{3}(?!@)"); //split on the LAST 3 @ signs
		TreeNode leftNode = null;
		for (String leftNodeKey : leftString) {
			String[] classString = leftNodeKey.split("#{3}(?!#)");
			ISEMOSSNode sNode = null;
			try {
				sNode = (ISEMOSSNode) Class.forName(classString[0]).getConstructor(String.class, Boolean.class).newInstance(classString[1], true);
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				e.printStackTrace();
			} catch (NoSuchMethodException e) {
				e.printStackTrace();
			} catch (SecurityException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			TreeNode node = new TreeNode(sNode);
			
			if (leftNode == null) {
				leftNode = node;
				retObject[0] = node;
				inVector.addElement(node);
				// also need to set the parent here
			} else {
				leftNode.rightSibling = node;
				node.leftSibling = leftNode;
				leftNode = node;
			}
		}
		retObject[1] = inVector;
		// // nasty.. I dont have a proper object eeks
		// Object [] retObject = new Object[2];
		// // final loop is the <> loop
		// StringTokenizer leftString = new StringTokenizer(childString, "-");
		// TreeNode leftNode = null;
		// while(leftString.hasMoreElements())
		// {
		// String leftNodeKey = leftString.nextToken();
		// TreeNode node = new TreeNode(new IntClass(Integer.parseInt(leftNodeKey)));
		// if(leftNode == null)
		// {
		// leftNode = node;
		// retObject[0] = node;
		// inVector.addElement(node);
		// }
		// else
		// {
		// leftNode.rightSibling = node;
		// node.leftSibling = leftNode;
		// leftNode = node;
		// }
		// }
		// retObject[1] = inVector;
		return retObject;
	}
	
	/**
	 * 
	 * @param leftList
	 * @param parent
	 */
	private void setParent(TreeNode leftList, TreeNode parent)	{
		while(leftList != null) {
			leftList.parent = parent;
			leftList = leftList.rightSibling;
		}
	}
	
	/**
	 * 
	 * @param node
	 * @return
	 */
	static TreeNode getLeft(TreeNode node) {
		if(node != null && node.leftSibling != null) {
			return getLeft(node.leftSibling);
		}
		else {
			return node;
		}
	}

	static TreeNode getRight(TreeNode node) {
		if(node.rightSibling != null)
			return getRight(node.rightSibling);
		else
			return node;
	}
	
	private int countSiblings(TreeNode counterNode) {
		int count = 1;
		counterNode = getLeft(counterNode);
		// expects you to give the left most node
		while(counterNode != null && counterNode.rightSibling != null) {
			counterNode = counterNode.rightSibling;
			count++;
		}
		return count;
	}
	
	private boolean hasChildren(TreeNode counterNode) {
		boolean has = false;
		// expects you to give the left most node
		while(counterNode.rightSibling != null && !has)	{
			if(counterNode.leftChild != null)
				has = true;
			if(counterNode.rightChild != null)
				has = true;
			counterNode = counterNode.rightSibling;
		}
		return has;
	}
	
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

	
	//TODO: Either delete this method because this would be too complex and inefficient to code to account for empty instance node
	//or just keep it private for internal structural uses
	//Also get right logic would not work since it does not go to right sibling first
//	private TreeNode2 getLeaf(TreeNode2 node, boolean left) {
//		if(node.leftChild != null && left)
//			return getLeaf(node.leftChild, true);
//		if(node.rightSibling == null && node.rightChild != null && !left)
//			return getLeaf(node.rightChild, false);
//		else return node;
//	}
	//****************************END UTILITY METHODS************************//
		

	//*****************************FLATTEN METHODS****************************//

//	public void flattenRoots(TreeNode2 node, boolean balanced)
//	{
//		while(node != null)
//		{
//			if(balanced)
//				flattenTree(new Vector(), node);
//			else
//				flattenUnBalancedTree(new Vector(), node);
//			node = node.rightSibling;
//		}
//	}
	
	//TODO: is flattenTree Needed?
//	/**
//	 * 
//	 * @param parentNodeList
//	 * @param node
//	 */
//	public void flattenTree(Vector<TreeNode2> parentNodeList, TreeNode2 node) {
//		if(node.leftChild != null && node.rightChild != null) {
//			if(node.leftChild != null) {
//				Vector <TreeNode2> newList = getNewVector(parentNodeList);
//				
//				if(node.instanceNode.size() > 0) {
//					newList.add(node);
//				}
//				
//				TreeNode2 leftChild = node.leftChild;
//				while(leftChild != null) {
//					flattenTree(newList, leftChild);
//					leftChild = leftChild.rightSibling;
//				}
//			}
//			if(node.rightChild != null && node.rightSibling == null)
//			{
//				Vector<TreeNode2> newList = getNewVector(parentNodeList);
//
//				if(node.instanceNode.size() > 0) {
//					newList.add(node);
//				}
//				
//				TreeNode2 leftChild = node.rightChild;
//				while(leftChild != null) {
//					flattenTree(newList, leftChild);
//					leftChild = leftChild.rightSibling;
//				}
//			}
//		}
//	}
//	
//	/**
//	 * 
//	 * 
//	 * @param parentNodeList
//	 * @param node
//	 */
//	public void flattenUnBalancedTree(Vector<TreeNode2> parentNodeList, TreeNode2 node)	{
//		if(node.leftChild != null) {
//			Vector <TreeNode2> newList = getNewVector(parentNodeList);
//
//			if(node.instanceNode.size() > 0) {
//				newList.add(node);
//			}
//			
//			TreeNode2 leftChild = node.leftChild;
//			while(leftChild != null) {
//				flattenUnBalancedTree(newList, leftChild);
//				leftChild = leftChild.rightSibling;
//			}
//		}
//	}

	public List<Object> flattenToArray(TreeNode node, boolean balanced)
	{
		List<Object> list = new Vector<Object>();
		while(node != null)
		{
			if(balanced)
				flattenTree2Array(list, node);
			else
				flattenUnBalancedTree2Array(list, node);
			node = node.rightSibling;
		}
		return list;
	}
	
	/**
	 * 
	 * @param table
	 * @param node
	 */
	private void flattenTree2Array(List<Object> table, TreeNode node) {
		
		if(node.instanceNode.size() > 0) {
			table.add(node.leaf.getValue());
		}
		
		if(node.leftChild != null && node.rightChild != null) {
			if(node.leftChild != null) {
				TreeNode leftChild = node.leftChild;
				while(leftChild != null) {
					flattenTree2Array(table, leftChild);
					leftChild = leftChild.rightSibling;
				}
			}
			if(node.rightChild != null && node.rightSibling == null) {
				TreeNode leftChild = node.rightChild;
				while(leftChild != null) {
					flattenTree2Array(table, leftChild);
					leftChild = leftChild.rightSibling;
				}
			}
		}
	}
	
	/**
	 * 
	 * @param table
	 * @param node
	 */
	private void flattenUnBalancedTree2Array(List<Object> table, TreeNode node)	{
		if(node.leftChild != null) {
			table.add(node.leaf.getValue());
			TreeNode leftChild = node.leftChild;
			while(leftChild != null) {
				flattenUnBalancedTree2Array(table, leftChild);
				leftChild = leftChild.rightSibling;
			}
		}
	}
	
	public List<Object> flattenRawToArray(TreeNode node, boolean balanced)
	{
		List<Object> list = new Vector<Object>();
		while(node != null)
		{
			if(balanced)
				flattenRawTree2Array(list, node);
			else
				flattenRawUnBalancedTree2Array(list, node);
			node = node.rightSibling;
		}
		return list;
	}
	
	/**
	 * 
	 * @param table
	 * @param node
	 */
	private void flattenRawTree2Array(List<Object> table, TreeNode node)
	{
		if(node.instanceNode.size() > 0) {
			table.add(node.leaf.getRawValue());
		}
		if(node.leftChild != null && node.rightChild != null)
		{
			if(node.leftChild != null)
			{
				TreeNode leftChild = node.leftChild;
				while(leftChild != null)
				{
					flattenRawTree2Array(table, leftChild);
					leftChild = leftChild.rightSibling;
				}
			}
			if(node.rightChild != null && node.rightSibling == null)
			{
				TreeNode leftChild = node.rightChild;
				while(leftChild != null)
				{
					flattenRawTree2Array(table, leftChild);
					leftChild = leftChild.rightSibling;
				}
			}
		}
	}
	
	/**
	 * 
	 * @param table
	 * @param node
	 */
	private void flattenRawUnBalancedTree2Array(List<Object> table, TreeNode node) {
		if(node.leftChild != null) {
			table.add(node.leaf.getRawValue());
			TreeNode leftChild = node.leftChild;
			while(leftChild != null) {
				flattenRawUnBalancedTree2Array(table, leftChild);
				leftChild = leftChild.rightSibling;
			}
		}
	}
	
	//*****************************END FLATTEN METHODS****************************//
	
	
	//***********METHODS NOT USED***********//
	//TODO: this would be useful and more efficient than insert data
	//Need to figure how to do this
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
	
	public void cleanNode() {
		this.parent = null;
		this.leftChild = null;
		this.rightChild = null;
		this.rightSibling = null;
		this.leftSibling = null;
	}


}
