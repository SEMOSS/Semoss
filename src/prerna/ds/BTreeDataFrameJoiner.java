package prerna.ds;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.util.ArrayUtilityMethods;

public class BTreeDataFrameJoiner {
	
	/**
	 * 
	 * @param table1
	 * @param table2
	 * @param table1ColHeaders
	 * @param table2ColHeaders
	 * 
	 * Assumptions made:
	 * 
	 */
	public static void join(BTreeDataFrame table1, BTreeDataFrame table2, String[] table1ColHeaders, String[] table2ColHeaders) {
		
		Map<String, ArrayList<SimpleTreeNode>> bucketMap = new HashMap<String, ArrayList<SimpleTreeNode>>();
		List<String> usedKeys = new ArrayList<String>();
		
		String lowestMatchLevel = table1ColHeaders[table1ColHeaders.length-1];
		TreeNode root = table1.getBuilder().nodeIndexHash.get(lowestMatchLevel);
		ValueTreeColumnIterator treeLevelIterator = new ValueTreeColumnIterator(root);
		while(treeLevelIterator.hasNext()) {
			
			SimpleTreeNode nextNode = treeLevelIterator.next();
			String key = nextNode.leaf.getValue().toString();
			
			SimpleTreeNode parentNode = nextNode.parent;
			int index = table1ColHeaders.length - 2;
			
			while(parentNode != null) {
				String type = ((ISEMOSSNode)parentNode.leaf).getType();
				if(type.equals(table1ColHeaders[index])) {
					key = key+":::"+parentNode.leaf.getValue().toString();
					index--;
				}
				
				if(index < 0) {
					break;
				}
				
				parentNode = parentNode.parent;
			}
			
			if(bucketMap.containsKey(key)) {
				bucketMap.get(key).add(nextNode);
			} else {
				ArrayList<SimpleTreeNode> newList = new ArrayList<SimpleTreeNode>();
				newList.add(nextNode);
				bucketMap.put(key, newList);
			}
		}
		
		lowestMatchLevel = table2ColHeaders[table2ColHeaders.length-1];
		root = table2.getBuilder().nodeIndexHash.get(lowestMatchLevel);
		treeLevelIterator = new ValueTreeColumnIterator(root);
		while(treeLevelIterator.hasNext()) {
			
			SimpleTreeNode nextNode = treeLevelIterator.next();
			String key = nextNode.leaf.getValue().toString();
			
			SimpleTreeNode parentNode = nextNode.parent;
			int index = table1ColHeaders.length - 2;
			
			while(parentNode != null) {
				String type = ((ISEMOSSNode)parentNode.leaf).getType();
				if(type.equals(table1ColHeaders[index])) {
					key = key+":::"+parentNode.leaf.getValue().toString();
					index--;
				}
				
				if(index < 0) {
					break;
				}
				
				parentNode = parentNode.parent;
			}
			
			if(bucketMap.containsKey(key)) {
				ArrayList<SimpleTreeNode> instanceList = bucketMap.get(key);
				
				Vector<SimpleTreeNode> vec = new Vector<SimpleTreeNode>();
				vec.add(nextNode.leftChild);
				String serialized = SimpleTreeNode.serializeTree("", vec, true, 0);
				
				for(SimpleTreeNode instance : instanceList) {
					SimpleTreeNode hookUp = SimpleTreeNode.deserializeTree(serialized);
					SimpleTreeNode.addLeafChild(instance, hookUp);
				}
				
				usedKeys.add(key);
			}
		}
		
		//add more code later for optimization
		//use the leftover simpletreenodes in the bucket map after removing used ones to either remove from the tree or adjust
		//this will prevent having to iterate through the whole value tree, at least in the case of inner joins
		
		//Also need to not create a new ITable for the purpose of simply iterating through---not worth the effort to create the index trees, just create
		//an array list to begin with to iterate through, this means not using an IAnalyticRoutine as parameter for joins
//		for(String usedKey : usedKeys) {
//			bucketMap.remove(usedKey);
//		}
		
		
		
		
	}
	
	public static void innerJoin(BTreeDataFrame table1, BTreeDataFrame table2, String table1ColHeader, String table2ColHeader) {
		
		Map<String, ArrayList<SimpleTreeNode>> bucketMap = new HashMap<String, ArrayList<SimpleTreeNode>>();
		List<String> usedKeys = new ArrayList<String>();
		
		TreeNode root = table1.getBuilder().nodeIndexHash.get(table1ColHeader);
//		ValueTreeColumnIterator treeLevelIterator = new ValueTreeColumnIterator(root);
		
		//bucket all the values in joining tree into a map
		IndexTreeIterator treeLevelIterator = new IndexTreeIterator(root);
		while(treeLevelIterator.hasNext()) {
			
			TreeNode nextNode = treeLevelIterator.next();
			String key = nextNode.leaf.getValue().toString();
			
			ArrayList<SimpleTreeNode> newList = new ArrayList<SimpleTreeNode>();
			newList.addAll(nextNode.instanceNode);
			bucketMap.put(key, newList);
			
		}
		
		//iterate through level of the second tree, attach matching nodes to all the nodes in the map for that value
		root = table2.getBuilder().nodeIndexHash.get(table2ColHeader);
		ValueTreeColumnIterator treeLevelIterator2 = new ValueTreeColumnIterator(root);
		while(treeLevelIterator2.hasNext()) {
			
			SimpleTreeNode nextNode = treeLevelIterator2.next();
			String key = nextNode.leaf.getValue().toString();
			
			if(bucketMap.containsKey(key)) {
				ArrayList<SimpleTreeNode> instanceList = bucketMap.get(key);
				
				Vector<SimpleTreeNode> vec = new Vector<SimpleTreeNode>();
				vec.add(nextNode.leftChild);
				String serialized = SimpleTreeNode.serializeTree("", vec, true, 0);
				
				//hookup new branch
				for(SimpleTreeNode instance : instanceList) {
					SimpleTreeNode hookUp = SimpleTreeNode.deserializeTree(serialized);
					SimpleTreeNode.addLeafChild(instance, hookUp);
				}
				
				usedKeys.add(key);
			}
		}
		
		//add more code later for optimization
		//use the leftover simpletreenodes in the bucket map after removing used ones to either remove from the tree or adjust
		//this will prevent having to iterate through the whole value tree, at least in the case of inner joins
		
		//Also need to not create a new ITable for the purpose of simply iterating through---not worth the effort to create the index trees, just create
		//an array list to begin with to iterate through, this means not using an IAnalyticRoutine as parameter for joins
		for(String usedKey : usedKeys) {
			bucketMap.remove(usedKey);
		}
		
		//for all 'unused' keys, delete the branches associated with those keys
		SimpleTreeBuilder builder = table1.getBuilder();
		for(String key : bucketMap.keySet()) {
			List<SimpleTreeNode> removeNodes = bucketMap.get(key);
			for(SimpleTreeNode n : removeNodes) {
				while(n!=null) {
					SimpleTreeNode parentNode = n.parent;
					builder.removeFromIndexTree(n);
					SimpleTreeNode.deleteNode(n);
					n = parentNode;
					if(n != null && n.leftChild != null) {
                        break;
					}
				}
			}
		}
	}
}
