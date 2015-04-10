package prerna.ds;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.StringTokenizer;
import java.util.Vector;


public class SimpleTreeNode {
	
	// gets the sibling tree
	SimpleTreeNode leftSibling = null;
	SimpleTreeNode rightSibling = null;
	SimpleTreeNode eldestLeftSibling = null; // some performance to make sure this is fast enough
	SimpleTreeNode parent = null;
	SimpleTreeNode rightChild = null;
	SimpleTreeNode leftChild = null;
	static String numberList = "";
	public static final String EMPTY = "_____";
	
	ITreeKeyEvaluatable leaf = null;
	
	int fanout = 4;
	
	boolean root = true;
	
	public SimpleTreeNode(ITreeKeyEvaluatable leaf)
	{
		this.leaf = leaf;
	}
	
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
		else
		{
			//System.err.println("found ?  " + found + searchNode.leaf.getKey());
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
	
	
	
	public SimpleTreeNode root(SimpleTreeNode node)
	{
		//System.err.println("Finding root for " + node.leaf.getKey());
		if(node.parent != null)
		{
			if(node.parent.equal(node))
			{
				System.err.println("Fishy..  " + node.leaf.getKey());
				return getLeft(node);
			}
			return root(node.parent);
		}
		else return getLeft(node);
	}
	
	
	
	private void printNodes(SimpleTreeNode rightChild2) {
		// TODO Auto-generated method stub
		System.out.println(rightChild2.leaf.getKey());
		if(rightChild2.rightSibling != null)
			printNodes(rightChild2.rightSibling);
		else
			System.out.println("=====");
	}

	public boolean equal(SimpleTreeNode node2Embed) {
		// TODO Auto-generated method stub
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

	public void printList(SimpleTreeNode node)
	{
		//System.out.print(node.leaf.getKey());
		if(node.rightSibling != null)
		{
			//System.out.print("--");
			printList(node.rightSibling);
		}
	}

	public void setParent(SimpleTreeNode leftList, SimpleTreeNode parent)
	{
		if(leftList != null && !leftList.equal(parent))
		{
			leftList.parent = parent;
			//System.err.println("Setting parent for " + leftList.leaf.getKey() + " ..... " + parent.leaf.getKey());
		}
		if(leftList.rightSibling != null)
			setParent(leftList.rightSibling, parent);
	}
	
	public SimpleTreeNode getLeft(SimpleTreeNode node)
	{
		while(node.leftSibling != null)
		{
			node = node.leftSibling;
		}
		return node;
	}

	public SimpleTreeNode getRight(SimpleTreeNode node)
	{
		while(node.rightSibling != null)
		{
			node = node.rightSibling;
		}
		return node;
	}
	

	public boolean left(SimpleTreeNode node)
	{
		return this.leaf.isLeft(node.leaf);
	}
		
	
	public void printNode(Vector <SimpleTreeNode> nodes, boolean parent, int level)
	{
		Vector childNodes = new Vector();
		System.out.print("Level " + level);
		System.out.println();
		for(int nodeIndex = 0;nodeIndex < nodes.size();nodeIndex++)
		{
			SimpleTreeNode node = nodes.elementAt(nodeIndex);
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
	
	public static String serializeTree(String output, Vector <SimpleTreeNode> nodes, boolean parent, int level)
	{
		Vector childNodes = new Vector();
		for(int nodeIndex = 0;nodeIndex < nodes.size();nodeIndex++)
		{
			SimpleTreeNode node = nodes.elementAt(nodeIndex);
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
		output += "/";
		if(childNodes.size() > 0)
			return serializeTree(output, childNodes, false, level+1);
		else
			return output;
	}
	
	public static SimpleTreeNode deserializeTree(String output)
	{
		SimpleTreeNode rootNode = null;
		boolean parent = true;
		Vector <SimpleTreeNode> parentNodes = new Vector();
		// each one of this is a new line
		StringTokenizer mainTokens = new StringTokenizer(output, "/");
		while(mainTokens.hasMoreTokens())
		{
			Vector <SimpleTreeNode> nextLevel = new Vector();
			String line = mainTokens.nextToken();
			int count = 0;
			if(!parent)
			{
				// next is to zoom out the pipes
				//TreeNode curParent = parentNodes.elementAt(count);
				SimpleTreeNode curParentNode = null;
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
					SimpleTreeNode leftNode = (SimpleTreeNode)stringOfNodes[0];
					nextLevel = (Vector<SimpleTreeNode>)stringOfNodes[1];
					// set the parent here
					// do the right node only if the parent has no sibling kind of
					stringOfNodes = createStringOfNodes(rightChildString, nextLevel);
					SimpleTreeNode rightNode = (SimpleTreeNode)stringOfNodes[0];
					nextLevel = (Vector<SimpleTreeNode>)stringOfNodes[1];

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
				rootNode = (SimpleTreeNode)stringOfNodes[0];
				nextLevel = (Vector<SimpleTreeNode>)stringOfNodes[1];
				System.out.println("Next level is " + nextLevel.get(0).leaf.getKey());
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
		StringTokenizer leftString = new StringTokenizer(childString, "-");
		SimpleTreeNode leftNode = null;
		while(leftString.hasMoreElements())
		{
			String leftNodeKey = leftString.nextToken();
			SimpleTreeNode node = new SimpleTreeNode(new IntClass(Integer.parseInt(leftNodeKey)));
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
	
	
	public static SimpleTreeNode join(Vector <SimpleTreeNode> fromNodes, Vector <SimpleTreeNode> toNodeRoot,boolean parent, SimpleTreeNode resNodeRoot)
	{
		Vector childNodes = new Vector();
		for(int nodeIndex = 0;nodeIndex < fromNodes.size();nodeIndex++)
		{
			SimpleTreeNode node = fromNodes.elementAt(nodeIndex);
			do{
				//output += node.leaf.getKey();
				if(toNodeRoot.get(0).search(toNodeRoot, node, false))
				{
					SimpleTreeNode newNode = new SimpleTreeNode(node.leaf);
					if(resNodeRoot == null)
						resNodeRoot = newNode;
					//else
					//	resNodeRoot = resNodeRoot.insertData(newNode);
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
	
	public void addChild(SimpleTreeNode node)
	{
		node.parent = this;
		//System.out.println("Leaf is " + node.leaf.getKey());
		if(this.leftChild != null && !this.leftChild.leaf.getKey().equalsIgnoreCase(SimpleTreeNode.EMPTY))
		{
			SimpleTreeNode rightMost = getRight(this.leftChild);
			rightMost.rightSibling = node;
			node.leftSibling = rightMost;
		}
		else
		{
			// what kind of cookie logic is this
			/*SimpleTreeNode prevLeftChild = null;
			if(this.leftChild != null)
				prevLeftChild = this.leftChild.leftChild; // what is this doing here
			node.leftChild = prevLeftChild;*/
			this.leftChild = node;			
		}
	}

	public static void addLeafChild(SimpleTreeNode parentNode, SimpleTreeNode node)
	{
		// the idea here is it will go to the ultimate child of this node and add it there
		// this is useful when there is a non-linear join that will happen
		// I will get to this later
		// use type comparison for siblings vs. ultimate child

		if(parentNode.leftChild != null && !parentNode.leftChild.leaf.getKey().equalsIgnoreCase(SimpleTreeNode.EMPTY))
		{
			//System.err.println("The value of the node is " +parentNode.leftChild.leaf.getKey() );

			if(((ISEMOSSNode)parentNode.leftChild.leaf).getType().equalsIgnoreCase(((ISEMOSSNode)node.leaf).getType())) // if it is the same type dont bother
				parentNode.addChild(node);
			else
			{
				SimpleTreeNode targetNode = parentNode.leftChild;
				do
				{
					addLeafChild(targetNode, node); // move on to this node
					targetNode = targetNode.rightSibling;// move to the next node on the sibling list
				}while(targetNode != null);
			}
		}
		else
			parentNode.addChild(node);
	}
		
	public static void deleteNode(SimpleTreeNode node)
	{
		// realign parents
		
		// if the node the the left child, catch the parent and set the 
		// first possibility - this has no right sibling - possibly the best scenario - delete
		if(node.rightSibling == null && node.leftSibling == null)
		{
			node.parent.leftChild = null;
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
			node.parent.leftChild = node.rightSibling;
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
	public static void tryFlatten()
	{
		StringClass cap = new StringClass("Lab");
		StringClass bp1 = new StringClass("AnatomicPath");
		StringClass bp2 = new StringClass("ClinicalPath");
		
		SimpleTreeNode capt = new SimpleTreeNode(cap);
		SimpleTreeNode bp1t = new SimpleTreeNode(bp1);
		SimpleTreeNode bp2t = new SimpleTreeNode(bp2);
		SimpleTreeNode bp3 = new SimpleTreeNode(bp1);
		SimpleTreeNode bp4 = new SimpleTreeNode(bp1);
		
		bp3.addChild(bp4);
		bp1t.addChild(bp3);
		
		capt.addChild(bp1t);
		capt.addChild(bp2t);

		Vector rootNodes = new Vector();
		rootNodes.add(capt);
		
		

		capt.printNode(rootNodes, true, 1);
		
		capt.flattenRoots(capt, new Vector());
	}
	
	

	public static void main(String [] args) throws Exception
	{
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		
		Vector rootNodes = new Vector();
		IntClass val = new IntClass(20);
		SimpleTreeNode root = new SimpleTreeNode((ITreeKeyEvaluatable)val);
		
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
	
	public static void searchIt(Vector <SimpleTreeNode> nodes, BufferedReader reader) throws Exception
	{
		String data = null;
		SimpleTreeNode root = nodes.elementAt(0);
		while(!((data = reader.readLine()).equalsIgnoreCase("stop")))
		{
			Vector <SimpleTreeNode> vec = new Vector();
			vec.add(root);
			DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			Date date = new Date();
			long time1 = System.nanoTime();
			long mtime1 = System.currentTimeMillis();
			//System.out.println("Start >> Nano " + dateFormat.format(date));
			System.out.println("Start >> Nano " + time1);
			System.out.println(" Finding  " + data + "    result " + root.search(vec, new SimpleTreeNode(new IntClass(Integer.parseInt(data))), false));
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
	
	public SimpleTreeNode getLeaf(SimpleTreeNode node, boolean left)
	{
		if(node.leftChild != null && left)
			return getLeaf(node.leftChild, true);
		if(node.rightSibling == null && node.rightChild != null && !left)
			return getLeaf(node.rightChild, false);
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
	
	public static void wait(BufferedReader reader, SimpleTreeNode node) throws Exception
	{
		System.out.println(">>");
		//reader.readLine();
		Vector rootNodes = new Vector();
		rootNodes.add(node);
		node.printNode(rootNodes, true, 1);
		System.out.println("Numbers >>  " + numberList);

	}
}
