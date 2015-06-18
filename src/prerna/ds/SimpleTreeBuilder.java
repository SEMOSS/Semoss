package prerna.ds;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
	SimpleTreeNode lastAddedNode = null;
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
	ISEMOSSNode [] seeds = null;
	ILinkeableEngine [] engines = null;
	String [] childs = null;
	TreePhaser phaser = null;
	ArrayList threadList = null;
	int runLevel = 0;
	ExecutorService service = null;
	String finalChildType = null;
	


	// this is for adding more than one pair of nodes at a time
	// the node array can be thought of as a row in a table--one node for each type
	// this assumes that the node array passed in spans the whole width of the table--every type gets a node
	public synchronized void addNodeArray(ISEMOSSNode... nodeArray)
	{
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
			retNode.addInstance(childInstanceNode);
			
			SimpleTreeNode.addLeafChild(parentInstanceNode, childInstanceNode);
			addToNodeIndex(childSEMOSSNode.getType(), retNode);

			lastAddedNode = parentInstanceNode;
			parentInstanceNode = childInstanceNode;
		}
		
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
		
	public int getNodeTypeCount(String nodeType)
	{
		return getSInstanceNodes(nodeType).size();
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
			for(int instanceIndex = 0;instanceIndex < typeInstances.size() && (child == null || (child != null && child.leaf.getKey().equalsIgnoreCase(SimpleTreeNode.EMPTY)));instanceIndex++)
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
		
	
	public TreeNode getNode(ISEMOSSNode node)
	{
		TreeNode typeIndexRoot = nodeIndexHash.get(node.getType());
		TreeNode retNode = null;
		if(typeIndexRoot != null)
		{
			Vector <TreeNode> rootNodeVector = new Vector<TreeNode>();
			rootNodeVector.add(typeIndexRoot);
			// find the node which has
			retNode = typeIndexRoot.getNode(rootNodeVector, new TreeNode(node), false);
		}
		return retNode;
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
				rightMostNode = rightMostNode.getRight(rightMostNode);
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
	
	public void addToNodeIndex(String nodeType, TreeNode node)
	{
		TreeNode root = node;
		//System.err.println("Adding type " + nodeType + " <> " + node.leaf.getKey());
		if(nodeIndexHash.containsKey(nodeType))
		{
			// need to search first to make sure this node is not there
			root = nodeIndexHash.get(nodeType);
			//System.err.println("Old root is " + root.leaf.getKey());
			Vector<TreeNode> searcher = new Vector<TreeNode>();
			searcher.add(root);
			if(!root.search(searcher, node, false))
			{// if not found insert it
				//System.err.println("In the not found loop");
				root = nodeIndexHash.get(nodeType).insertData(node);
			}
			//System.err.println("New root is " + root.leaf.getKey());
		}
		nodeIndexHash.put(nodeType, root);
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

	// get all instances of this node
	public Vector<SimpleTreeNode> getInstances(String type)
	{
		// pick the type root from the index
		// Run through every node and add the instances to a vector
		// push to flatten roots on the SimpleTreeNode and flatten it
		TreeNode typeRoot = nodeIndexHash.get(type);
		SimpleTreeNode instanceNode = typeRoot.getInstances().elementAt(0);
		instanceNode = instanceNode.getLeft(instanceNode);
		Vector <TreeNode> rootVector = new Vector<TreeNode>();
		rootVector.add(typeRoot);
		Vector <SimpleTreeNode> instanceVector = new Vector<SimpleTreeNode>();
		instanceVector.add(instanceNode);
		//Vector output = typeRoot.getInstanceNodes(rootVector, new Vector());
		Vector <SimpleTreeNode> output = instanceNode.getChildsOfType(instanceVector, type, new Vector<SimpleTreeNode>());
		
		// need to remove the duplicates
		System.out.println("Total Number of instances are " + output.size() + output.elementAt(0).leaf.getKey());
		//System.out.println("Total Number of instances are " + output.size() + output.elementAt(5).leaf.getKey());
		
		return output;
	}
	
	public Hashtable<SimpleTreeNode, Vector<SimpleTreeNode>> getPath(String fromType, String toType)
	{
		TreeNode typeRoot = nodeIndexHash.get(fromType);
		Vector <TreeNode> searchVector = new Vector<TreeNode>();
		searchVector.add(typeRoot);
		// I need to write the logic to walk the tree and get all instances
		// gets all the instances
		Vector <SimpleTreeNode> allInstanceVector = typeRoot.getInstanceNodes(searchVector, new Vector<SimpleTreeNode>());
		
		Hashtable<SimpleTreeNode, Vector<SimpleTreeNode>> values = new Hashtable<SimpleTreeNode, Vector<SimpleTreeNode>>();
		for(int instanceIndex = 0;instanceIndex < allInstanceVector.size();instanceIndex++)
		{
			
			Vector <SimpleTreeNode> instanceVector = new Vector<SimpleTreeNode>();
			SimpleTreeNode fromNode = allInstanceVector.elementAt(instanceIndex);
			instanceVector.addElement(fromNode);
			//SimpleTreeNode instanceNode = typeRoot.getInstances().elementAt(0);
			//instanceNode = instanceNode.getLeft(instanceNode);
			//instanceVector.add(instanceNode);
			//Vector output = typeRoot.getInstanceNodes(rootVector, new Vector());
			Vector <SimpleTreeNode> output = SimpleTreeNode.getChildsOfType(instanceVector, toType, new Vector<SimpleTreeNode>());
			values.put(fromNode, output);
			
			// need to remove the duplicates
			System.out.println("Total Number of instances are " + output.size() + output.elementAt(0).leaf.getKey());
		}
		System.out.println("Output values is " + values);
		return values;
	}

	// gets all the semoss nodes i.e. the leafs within the simple tree node
	public Vector<ISEMOSSNode> getSInstanceNodes(String type)
	{
		TreeNode typeRoot = nodeIndexHash.get(type);
		Vector <TreeNode> rootVector = new Vector<TreeNode>();
		rootVector.add(typeRoot);
		Vector <ISEMOSSNode> output = typeRoot.getInstanceSNodes(rootVector, new Vector<ISEMOSSNode>()); // gives me simple tree nodes
		return output;
	}
	
	public void addStress()
	{
		int [] nodeNumberArray = {100,1000,10000,100000};
		
		int total = 3000000;
		int levels = 10;
		int eachLevel = total / levels;
		int levelCounter = 1;
		int secCounter = 1;
		for(int counter = 0;counter < total;counter++)
		{
			String nodeName = "Sample" + secCounter;
			String type = "Type" + levelCounter;
			String type2 = "Type" + (levelCounter + 1);
			//System.err.println("Adding " + nodeName + "   Parent Type" + type + "child Name " + nodeName  +  "   Parent Type 2" + type2);
			addNode(new StringClass(nodeName, type), new StringClass(nodeName, type2));
			secCounter++;
			if(secCounter % eachLevel == 0)
			{
				levelCounter++;
				secCounter = 1;
				//System.err.println("Moved to next level" + counter);
				//System.err.println("Level Counter " + levelCounter);
			}
		}
		System.out.println("Held up ");
		//flattenFromType("Type1");
	}

	public void addStress2()
	{
		// 50 capabilities - 20 activity each - 6 data objects - 200 systems for each object
		//int [] nodeNumberArray = {50,20,6,20};//,100000}; // number of nodes / children at every level
		int []  nodeNumberArray = {10,200,300}; // number of nodes / children at every level
		
		// create the first piece where 
		// the number of nodes on level 1 is 100
		// number of nodes on level 2 is 1000
		// at the next level add 10,000 nodes to each of the children
		// at the level next to that add 100,000 to each node again
		
		long startTime = printTime();
		
		int numLevels = nodeNumberArray.length;
		String nodeName = "NODE";
		String type = "TYPE";
		int numRecords = nodeNumberArray[0];
		
		for(int levelIndex = 1;levelIndex < numLevels;levelIndex++)
		{
			int numNodesAtThisLevel = nodeNumberArray[levelIndex - 1];
			int numNodesAtNextLevel = nodeNumberArray[levelIndex];
			numRecords = numRecords * numNodesAtNextLevel;
			
			String parentType = type + levelIndex;
			String childType = type + (levelIndex + 1);
			
			System.out.println("Completing Type " + parentType + " To  " + childType);
			
			for (int parentIndex = 0;parentIndex < numNodesAtThisLevel;parentIndex++)
			{
				String parentName = nodeName + parentIndex;
				//System.out.println("Running Parent " + parentName);
				for(int childIndex = 0;childIndex < numNodesAtNextLevel;childIndex++)
				{
					//System.out.println("Completing Type " + parentType + " To  " + childType);
					
					String childName = nodeName + childIndex;
					addNode(new StringClass(parentName, parentType), new StringClass(childName, childType));
				}
			}
		}		
		long endTime = printTime();
		long timeToCreateTree = (endTime - startTime)/1000;
		System.out.println("Time Spent... seconds " + timeToCreateTree);
		System.out.println("Total Records " + numRecords);

		System.out.println("Held up ");
		startTime = printTime();
		flattenFromType("TYPE1");
		endTime = printTime();
		long flattenTime = (endTime - startTime)/1000;
		System.out.println("Time Spent... to create  " + timeToCreateTree );
		System.out.println("Time Spent... to flatten  " + flattenTime);

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

	public void addSimple()
	{
		addNode(new StringClass("AnatomicPath", "BP"), new StringClass("Modify Referrals", "Activity"));
		addNode(new StringClass("AnatomicPath", "BP"), new StringClass("Modify Referrals", "Activity"));
		addNode(new StringClass("AnatomicPath", "BP"), new StringClass("Modify Referrals", "Activity"));
		addNode(new StringClass("AnatomicPath", "BP"), new StringClass("Modify Orders", "Activity"));
		addNode(new StringClass("AnatomicPath", "BP"), new StringClass("Procurement", "Activity"));
		
		addNode(new StringClass("Lab", "Cap"), new StringClass("AnatomicPath", "BP"));
		addNode(new StringClass("Lab", "Cap"), new StringClass("ClinicalPath", "BP"));
		//flattenFromType("Cap");
		
		//System.out.println("Number of nodes " + getSInstanceNodes("Activity").size());
		
		//System.err.println("-----");
		//adjustType("BP", false);
		//flattenFromType("Cap");
		
		//System.err.println("-----");
		addNode(new StringClass("ClinicalPath", "BP"), new StringClass("Procurement", "Activity"));
		//flattenFromType("Cap");
		
		//System.err.println("-----");
		
		addNode(new StringClass("Modify Referrals", "Activity"), new StringClass("Orders", "DO"));
		addNode(new StringClass("Modify Referrals", "Activity"), new StringClass("Referrals", "DO"));
		addNode(new StringClass("Modify Referrals", "Activity"), new StringClass("Patients", "DO"));
		addNode(new StringClass("ClinicalPath", "BP"), new StringClass("Procurement2", "Activity"));
		
		//adjustType("Activity", false);
		
		//addNode(new StringClass("Procurement", "Activity"), new StringClass("Eligibility", "BLU"));
		//addNode(new StringClass("Modify Referrals", "Activity"), new StringClass("Eligibility", "BLU"));
		addNode(new StringClass("Patients", "DO"), new StringClass("Eligibility32", "BLUMEAWAY"));
		adjustType("DO", false);
		//addNode(new StringClass("Eligibility", "BLU"), new StringClass("Eligibility33", "TRY"));
		addNode(new StringClass("Lab", "Cap"), new StringClass("HOLYCow", "TRY2"));
		
		//adjustType("DO", false);
		
		//flattenFromType("Cap");
		//getInstances("Cap");
		//getPath("BP", "TRY2");
		addNode(new StringClass("Lab", "Cap"), new StringClass("PK", "BP"));
		//adjustType("BP", true);
		//System.err.println("After Adjustments.... for a single node");
		//flattenFromType("Cap");
		//removeNode(new StringClass("Referrals", "DO"));
		//removeType("DO"); // not removing the last one
		//addNode(new StringClass("Modify Referrals", "Activity"), new StringClass("Eligibility", "BLU"));
		//System.out.println("Flattening ");
		//flattenFromType("Cap");
		//System.err.println("-----");
		
		//flattenFromType("Cap", "Activity");

	}
	
	public Vector <String> findLevels()
	{
		
		if(finalChildType == null) return null;
		//{
			//Set<String> keys = nodeIndexHash.keySet();
			//if(keys !)
			//return null;
		//}
		Vector <String> retVector = new Vector<String>();
		
		TreeNode aNode = nodeIndexHash.get(finalChildType);
		SimpleTreeNode sNode = aNode.instanceNode.elementAt(0);
		while(sNode != null)
		{
			retVector.add(((ISEMOSSNode)sNode.leaf).getType());
			sNode = sNode.parent;
		}
		Collections.reverse(retVector);
		return retVector;
	}

	public static void main(String [] args)
	{
		SimpleTreeBuilder builder = new SimpleTreeBuilder();
		SimpleTreeBuilder builder2 = new SimpleTreeBuilder();
		
		//System.out.println(Runtime.getRuntime().availableProcessors());
		
		
		
		builder.addSimple();
		builder.append(builder.getRoot(), builder2.getRoot());
		
		//builder.addStress2();
		//builder.multiEngineAdd();
	}
	
	public void removeNode(ISEMOSSNode node)
	{
		String type = node.getType();
		if(nodeIndexHash.containsKey(type))
		{
			TreeNode rootNode = nodeIndexHash.get(type);
			TreeNode searchNode = new TreeNode(node);
			Vector searchVector = new Vector();
			searchVector.addElement(rootNode);
			TreeNode foundNode = rootNode.getNode(searchVector, searchNode, false);
			
			if(foundNode != null)
			{
				// get all the instances and delete
				Vector <SimpleTreeNode> instances = foundNode.getInstances();
				for(int instanceIndex = 0;instanceIndex < instances.size();instanceIndex++)
					SimpleTreeNode.deleteNode(instances.get(instanceIndex));
				foundNode.instanceNode = null;
			}
		}
	}
	
	public void multiEngineAdd()
	{
		// need to create some structures
		// I need an array of linkeable engines
		// I need an array of childs 
		// I need a seed set of data to start of the process
		// Optionally, I need some filters
		// filters are typically of the form of
		int numProc = Runtime.getRuntime().availableProcessors();
		threadList = new ArrayList();
		numProc = 8;
		
		// this is the core method that would test the tree add
		// I need set of levels
		// number of nodes at every level
		// I need to set up the engines so that
		// for every node it gives me multiple child nodes
		int []  nodeNumberArray = {10,600,800}; //,3,4,10,2}; //,10};//,10,10}; //,20}; //,20,20}; // number of nodes / children at every level
		service = Executors.newFixedThreadPool(numProc);
		phaser = new TreePhaser(this, service);
		
		engines = new ILinkeableEngine[nodeNumberArray.length - 1];
		for(int engineIndex = 1;engineIndex < nodeNumberArray.length;engineIndex++)
			engines[(engineIndex-1)] = new SampleHashEngine(0, nodeNumberArray[engineIndex], engineIndex, engineIndex+1);

		seeds = new ISEMOSSNode[nodeNumberArray[0]];
		String type = "TYPE0";
		for(int nodeIndex = 0;nodeIndex < nodeNumberArray[0];nodeIndex++)
			seeds[nodeIndex] = new StringClass("NODE"+nodeIndex, type); 

		// now create the threads
		for(int threadIndex = 0;threadIndex < numProc;threadIndex++)
			threadList.add(new TreeThreader(this, phaser));		
		runLevel = 0;
		execEngine();
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

	// equivalent of removing a column
	// this needs to be revisited
	// i.e. it needs to be remastered / reparented
	// I cant just drop the entire tree and below for this
	public void removeType(String type)
	{
		// find the node of this type
		// get the instance node
		// go to the parent node and find the type for parent
		// gather all the instances of parent
		// 0. for each parent
		// assimilate all the nodes below it
		// 1. i.e. pick the left child
		// 2. get the instances
		// 3. parent the instances to the grand parent in step 0.
		// 4. Add these instance to the parent in step 0.
		// need to handle when the first columns is blown out
		
		if(!nodeIndexHash.containsKey(type))
			return;
		
		TreeNode typeRoot = nodeIndexHash.get(type);
		SimpleTreeNode typeInstanceNode = typeRoot.instanceNode.firstElement();
		SimpleTreeNode parent = typeInstanceNode.parent;
		if(parent != null)
		{
			String parentType = ((ISEMOSSNode)parent.leaf).getType();
			
			Vector <TreeNode> searchVector = new Vector<TreeNode>();
			TreeNode parentRoot = nodeIndexHash.get(parentType); 
			searchVector.add(parentRoot);
			// I need to write the logic to walk the tree and get all instances
			// gets all the instances
			// the complexity of this routine is really bad.. three freaking loops..
			// need to revisit
			Vector <SimpleTreeNode> allInstanceVector = parentRoot.getInstanceNodes(searchVector, new Vector<SimpleTreeNode>());
			for(int instanceIndex = 0;instanceIndex < allInstanceVector.size();instanceIndex++)
			{
				SimpleTreeNode daParentNode = allInstanceVector.get(instanceIndex);
				SimpleTreeNode deletedNode = daParentNode.leftChild;
				daParentNode.leftChild = null; // nullify so that the type wont interfere
				while(deletedNode != null)
				{
					deletedNode.parent = null;
					// assimilate the child
					if(deletedNode.leftChild != null)
					{
						SimpleTreeNode targetNode = deletedNode.leftChild;
						while(targetNode != null)
						{
							SimpleTreeNode.addLeafChild(daParentNode, targetNode);
							targetNode = targetNode.rightSibling;
						}
					}
					// get to the next deleted node
					deletedNode = deletedNode.rightSibling;
				}
			}
		}
		else // this is the case when the type is the root
		{
			typeInstanceNode = typeInstanceNode.getLeft(typeInstanceNode);
			if(typeInstanceNode != null) {
				
				SimpleTreeNode leftMostNode = typeInstanceNode.leftChild;
				SimpleTreeNode rightMostSibling = leftMostNode.getRight(leftMostNode);
				
				while(typeInstanceNode != null) {
					// need to make sure new root are all siblings
					
					SimpleTreeNode targetNode = typeInstanceNode.leftChild;
					if(targetNode != leftMostNode) { // this occurs for the first iteration
						rightMostSibling.rightSibling = targetNode;
						targetNode.leftSibling = rightMostSibling;
					}
					rightMostSibling = targetNode.getRight(targetNode); // update the new most right sibling

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
	
	/**
	 * 
	 * @param type
	 */
	public void removeDuplicates(String type) {
		//Basic Idea
		//Consider all nodes to be individual trees, consolidate them together to remove duplicates
		//reattach new children to proper parents
		//Need to figure out how to  properly update the index tree
			
		if(!nodeIndexHash.containsKey(type)) return;
		
		//Iterate through the nodes on the level above the level we are removing duplicates
		//this is done so we don't append nodes from different branches on the same level
		SimpleTreeNode simpleTreeNode = nodeIndexHash.get(type).instanceNode.get(0);
		ISEMOSSNode parentNode;
		
		if(simpleTreeNode.parent != null) {
			parentNode = (ISEMOSSNode)simpleTreeNode.parent.leaf;
			type = parentNode.getType();
		}
		
		ValueTreeColumnIterator iterator = new ValueTreeColumnIterator(nodeIndexHash.get(type));
		
		while(iterator.hasNext()) {
			
			//TODO: fix the sibling relationships
			SimpleTreeNode currParent = iterator.next();
			SimpleTreeNode firstNode = currParent.leftChild;
			SimpleTreeNode node = firstNode.rightSibling;
			firstNode.rightSibling = null;
			
			while(node!=null) {
				consolidate(firstNode, node);
				node = node.rightSibling;
			}
		}
	}
	
	/**
	 * Calls removeDuplicates for every level in the BTree, in order from leaf to root
	 */
	public void removeAllDuplicates() {
		Vector<String> levels = this.findLevels();
		Collections.reverse(levels);
		//call remove duplicates in order from leaf to root
		for(String level: levels) {
			this.removeDuplicates(level);
		}
	}
	
	/**
	 * Combines subtrees within the BTree to remove duplicates.  The logic is identical to append, except
	 * consolidate removes from the index trees as opposed to adding to them
	 * @param node
	 * @param node2consolidate
	 */
	private void consolidate(SimpleTreeNode node, SimpleTreeNode node2consolidate) {
		
		SimpleTreeNode rightNode = node.getRight(node);
		SimpleTreeNode leftNode2Merge = node2consolidate.getLeft(node2consolidate);
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
				removeFromIndexTree(leftNode2Merge);
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
						consolidate(equivalentInstance.leftChild, leftNode2Merge.leftChild);
					} 
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
					removeFromIndexTree(leftNode2Merge); 
				}
				//append(equivalentInstance.leftChild, leftNode2Merge.leftChild);
			}
			leftNode2Merge = rightMergeSibling;
		}
	}
	
	/**
	 * Finds the TreeNode in the index tree with the value of @param n, 
	 * and removes the instance of n from the index tree
	 * 
	 * @param n		The instance to be removed from the index tree
	 */
	private void removeFromIndexTree(SimpleTreeNode n) {
		//get the appropriate index tree
		ISEMOSSNode isn = (ISEMOSSNode)n.leaf;
		TreeNode treeNode = nodeIndexHash.get(isn.getType());
		
		//find the proper corresponding TreeNode in the index tree
		TreeNode searchNode = new TreeNode(n.leaf);
		TreeNode foundNode = treeNode.getLeaf(searchNode, true);
		
		//remove n from the instance list
		Vector<SimpleTreeNode> instanceList = foundNode.getInstances();
		if(instanceList.size()==0) {
			//TODO: figure out if I need to rebalance the tree after removing a node
			this.removeNode(isn);
		} else {
			instanceList.remove(n);
		}
	}
	
	
	// add a filter so that it is not kept when flattening is done
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
	
	//TODO: figure out how to filter roots
	public void filterTree(String column, List<ITreeKeyEvaluatable> objectsToFilter) {
		for(ITreeKeyEvaluatable i: objectsToFilter) {
			filterTree(column, i);
		}
	}
	
	public void filterTree(String column, ITreeKeyEvaluatable objectToFilter) {
		TreeNode filterIndexTree = nodeIndexHash.get(column);
		TreeNode treeNode = new TreeNode(objectToFilter);
		TreeNode foundNode = filterIndexTree.getLeaf(treeNode, true);
		for(SimpleTreeNode n: foundNode.instanceNode) {
			filterSimpleTreeNode(n);
			filterTreeNode(n);
			//foundNode.instanceNode.remove(n);
			//foundNode.filteredInstanceNode.add(n);
		}
	}

	private void filterSimpleTreeNode(SimpleTreeNode node2filter) {
		SimpleTreeNode parentNode = node2filter.parent;
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
			parentNode.leftChild = nodeRightSibling;
			nodeRightSibling.leftSibling = null;
		}
		
		//put the node2filter on the right side
		if(parentNode.rightChild == null) {
			parentNode.rightChild = node2filter;
		} else {
			SimpleTreeNode rightFilteredChild = parentNode.rightChild;
			rightFilteredChild = rightFilteredChild.getRight(rightFilteredChild);
			rightFilteredChild.rightSibling = node2filter;
			node2filter.leftSibling = rightFilteredChild;
		}
	}
	
	private void filterTreeNode(SimpleTreeNode instance2filter) {
		String type = ((ISEMOSSNode)instance2filter.leaf).getType();
		TreeNode filterIndexTree = nodeIndexHash.get(type);
		TreeNode treeNode = new TreeNode(instance2filter.leaf);
		TreeNode foundNode = filterIndexTree.getLeaf(treeNode, true);
		
		foundNode.instanceNode.remove(instance2filter);
		foundNode.filteredInstanceNode.add(instance2filter);
		
		if(instance2filter.rightSibling!=null) {
			filterTreeNode(instance2filter.rightChild);
		}
		if(instance2filter.leftChild!=null) {
			filterTreeNode(instance2filter.leftChild);
		}
		/*
		if(treeNode.instanceNode.contains(instance2filter)) {
			treeNode.instanceNode.remove(instance2filter);
			treeNode.filteredInstanceNode.add(instance2filter);
		}
		*/
	}
	
	public void unfilterSimpleTree(String column) {
		//TODO
	}
	public void unfilterColumn(String column) {
		TreeNode unfilterIndexTree = nodeIndexHash.get(column);
		ITreeKeyEvaluatable l = unfilterIndexTree.instanceNode.get(0).parent.leaf;
		ISEMOSSNode parent = (ISEMOSSNode)l;
		String parentType = parent.getType();
		
		TreeNode parentIndexTree = nodeIndexHash.get(parentType);
		ValueTreeColumnIterator iterator = new ValueTreeColumnIterator(parentIndexTree);
		
		while(iterator.hasNext()) {
			SimpleTreeNode simpleTree = iterator.next();
			if(simpleTree.rightChild!=null) {
				SimpleTreeNode leftChild = simpleTree.leftChild;
				SimpleTreeNode rightChild = simpleTree.rightChild;
				
				if(leftChild==null) {
					simpleTree.leftChild = rightChild;
				} else {
					SimpleTreeNode rightMostLeftChild = leftChild.getRight(leftChild);
					rightMostLeftChild.rightSibling = rightChild;
					rightChild.leftSibling = rightMostLeftChild;
				}
				simpleTree.rightChild = null;
			}
		}
		//unfilter the TreeNode as well
		//transfer all right children of parent column back to the left child
	}
	
	public void unfilterTreeNode(TreeNode treeNode, SimpleTreeNode instance) {
		if(treeNode.filteredInstanceNode.contains(instance)) {
			treeNode.filteredInstanceNode.remove(instance);
			treeNode.instanceNode.add(instance);
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
		
		SimpleTreeNode rightNode = node.getRight(node);
		SimpleTreeNode leftNode2Merge = node2merge.getLeft(node2merge);
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
	 * This method recursively adds a node and it's children to the appropriate index trees
	 * 
	 * 
	 * 
	 * */
	private void appendToIndexTree(SimpleTreeNode node) {
		if(node == null) {
			return;
		}
		
		ISEMOSSNode n = (ISEMOSSNode) node.leaf;
		TreeNode rootIndexNode = nodeIndexHash.get(n.getType());

		while(node != null) {
			TreeNode newNode = getNode( (ISEMOSSNode)node.leaf);
			// search first
			if(newNode == null) {
				// if not found 
				// create new node and set instances vector to the new value node
				newNode = new TreeNode(node.leaf);
				TreeNode newRoot = rootIndexNode.insertData(newNode);
				nodeIndexHash.put(n.getType(), newRoot);
				newNode.addInstance(node);
			} else {
				// if found
				// add instance to existing TreeNode
				newNode.getInstances().add(node);
			}
			
			if(node.leftChild != null) {
				appendToIndexTree(node.leftChild);
			}
			node = node.rightSibling;
		}
	}
	
	//TODO: make this better, not sure if lastAddedNode is reliable
	public SimpleTreeNode getRoot() {
		SimpleTreeNode root = lastAddedNode;
		while(root.parent!=null) root = root.parent;
		while(root.leftSibling!=null) root = root.leftSibling;
		return root;
	}
}
