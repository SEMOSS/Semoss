package prerna.ds;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.openrdf.model.Literal;

import prerna.algorithm.api.ITableDataFrame;

/**
 * This class is responsible for implementing Excel-style filtering on the BTreeDataFrame 
 * 		manipulating SimpleTreeNodes and TreeNodes within the BTreeDataFrame's SimpleTreeBuilder 
 */
public class BTreeExcelFilterer implements IBtreeFilterer {

	private BTreeDataFrame table; //the table on which to filter
	private SimpleTreeBuilder builder; //the builder which belongs to the table
	
	public BTreeExcelFilterer(BTreeDataFrame table) {
		this.table = table;
		this.builder = table.getBuilder();
	}
	
	
	/**
	 * @param column - the column on which to filter on
	 * @param filterValues - the list of values to filter OUT of the tree
	 * 
	 * this method filters out the filterValues from the tree, and unfilters all other values in the column
	 */
	@Override
	public void filter(String column, List<Object> filterValues) {
		
		IndexTreeIterator iterator = new IndexTreeIterator(builder.nodeIndexHash.get(column));
		
		//convert to Set for efficient 'contains' checks
		Set<String> Values = new HashSet<String>();
		for(Object value : filterValues) {
			Values.add(value.toString());
		}
		
		
		//for each unique value in the column 
		while(iterator.hasNext()) {

			//get the value to conduct filtering on
			TreeNode node = iterator.next();
			Object nodeValue = node.getValue();
//			if(nodeValue.toString().equalsIgnoreCase("Beastly") || nodeValue.toString().equalsIgnoreCase("Seven_Psychopaths") || nodeValue.toString().equalsIgnoreCase("Salmon_Fishing_in_the_Yemen") ||nodeValue.toString().equalsIgnoreCase("Last_Vegas")) {
//				System.out.println("Here");
//			}
			ITreeKeyEvaluatable nodeObject = this.createNodeObject(nodeValue, nodeValue, column);
			
			//if the value is contained with in the set of values of the argument filterValues, unfilter that value
			if(Values.contains(nodeValue.toString())) {
				this.unfilterTree(nodeObject);
			} 
			
			//otherwise filter the value
			else {
				this.filterTree(nodeObject);
			}
		}
		
		this.adjustRoots();
	}
	
	/**
	 * 
	 * @param column - column to unfilter
	 * 
	 * calls unfilterTree method for each value in the index tree
	 */
	@Override
	public void unfilter(String column) {
		IndexTreeIterator iterator = new IndexTreeIterator(builder.nodeIndexHash.get(column));
		
		//for each unique value in the column 
		while(iterator.hasNext()) {

			//get the value to conduct filtering on
			TreeNode node = iterator.next();
			Object nodeValue = node.getValue();
			ITreeKeyEvaluatable nodeObject = this.createNodeObject(nodeValue, nodeValue, column);
			
			//unfilter the tree
			this.unfilterTree(nodeObject);
		}
		
		this.adjustRoots();
	}
	
	/**
	 * 
	 * @param objectToFilter - object to filter OUT of the tree
	 * 
	 * This method filters the appropriate SimpleTreeNodes associated with objectToFilter
	 * 		filtering involves:
	 * 			1. moving the SimpleTreeNode to be a right child (or attached to filteredRoot for root level nodes)
	 * 			2. filtering the subtree associated with the SimpleTreeNode 
	 */
	public void filterTree(ITreeKeyEvaluatable objectToFilter) {
		//find the TreeNode associated with the object's value
		TreeNode foundNode = builder.getNode((ISEMOSSNode)objectToFilter);
		if(foundNode != null) {
			
			//copy references to new list to prevent concurrent modification exceptions
			//only operate on visible values and transitively filtered values
			List<SimpleTreeNode> nodeList = new ArrayList<SimpleTreeNode>();
			for(SimpleTreeNode n: foundNode.getInstances()) {
				nodeList.add(n);
			}
			
			for(SimpleTreeNode n: foundNode.getTransFilteredInstances()) {
				nodeList.add(n);
			}
			
			//for each SimpleTreeNode with the value of objectToFilter, filter only if n is not already hard filtered
			for(SimpleTreeNode n: nodeList) {
				if(!n.hardFiltered) {

					//parentCheck determines whether we should filter the parents on the tree node
					//if root no need to filter parents
					boolean parentCheck = false;
					if(n.parent == null) {
						parentCheck = false;
					}
					//check if parent has a left child
					else {
						parentCheck = n.parent.leftChild != null;
					}
					
					//move node to the right because it will be hard filtered
					moveToRightChild(n);
				
					//determine if parent had a left child, now no longer has a left child
					if(parentCheck) {
						parentCheck = n.parent.leftChild == null;
					}
					
					//place node in the right bucket in the index tree, i.e. unfiltered, hard filtered, transitively filtered
					//this will also transitively filter the children and parents(when necessary)
					filterTreeNode(n, true);
					
					//filter parents if need be
					if(parentCheck) {
						filterTreeNodeParents(n);
					}
				}
			}
		}
	}

	/**
	 * 
	 * @param node2filter
	 * 
	 * This method filters node2filter from the value tree
	 * In the case where node2filter's parent has one child (which is node2filter), node2filter's parent is filtered, etc
	 */
	private void moveToRightChild(SimpleTreeNode node2filter) {
		
		SimpleTreeNode parentNode = node2filter.parent;
		boolean root = (parentNode == null);
		
		//if not root and is already a right child just return
		if((!root && parentNode.rightChild == node2filter) || builder.getFilteredRoot() == node2filter) return;
		
		//Grab the siblings
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
				if(parentNode.leftChild == node2filter) {
					parentNode.leftChild = nodeRightSibling;
				}
			}
			nodeRightSibling.leftSibling = null;
		} 
		
		else {
			//only child
			if(!root && parentNode.leftChild == node2filter) {
				parentNode.leftChild = null;
			}
		}
		
		//Isolate the node from the siblings
		node2filter.rightSibling = null;
		node2filter.leftSibling = null;
		
		//If node is root level, attach to filteredRoot 
		if(root) {
			
			SimpleTreeNode filteredRoot = builder.getFilteredRoot();
			if(filteredRoot == null) {
				filteredRoot = node2filter;
			} else { //need a check here?
				filteredRoot.leftSibling = node2filter;
				node2filter.rightSibling = filteredRoot;
				filteredRoot = node2filter;
			}
		} 
		
		//otherwise put node2filter on the right side of the parent
		else {
			if(parentNode.rightChild == null) {
				parentNode.rightChild = node2filter;
			} else if(parentNode.rightChild != node2filter) {
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
		
		//if parent node exists and parent node has no left children, filter that parent too but just that parent
		if(parentNode != null && SimpleTreeNode.countNodeChildren(parentNode)==0) {
			moveToRightChild(parentNode);
//			filterTreeNode(parentNode);
////			if(!node2filter.hardFiltered) {
//			parentNode.incrementTransitiveFilter();
////			}
//			parentNode = parentNode.parent;
		}
	}
	
	/**
	 * 
	 * @param node2filter
	 */
	private void moveSingleNodeToRightChild(SimpleTreeNode node2filter) {
		SimpleTreeNode parentNode = node2filter.parent;
		boolean root = (parentNode == null);
		
		//if not root and is already a right child just return
		if((!root && parentNode.rightChild == node2filter) || builder.getFilteredRoot() == node2filter) return;
				
		//Grab the siblings
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
				
				if(parentNode.leftChild == node2filter) {
					parentNode.leftChild = nodeRightSibling;
				}
			}
			nodeRightSibling.leftSibling = null;
		} else {
			//only child
			if(!root && parentNode.leftChild == node2filter) {
				parentNode.leftChild = null;
			}
		}
		
		//Isolate the node from the siblings
		node2filter.rightSibling = null;
		node2filter.leftSibling = null;
		
		//If node is root level, attach to filteredRoot
		if(root) {
			
			SimpleTreeNode filteredRoot = builder.getFilteredRoot();
			if(filteredRoot == null) {
				filteredRoot = node2filter;
			} else { //need a check here?
				filteredRoot.leftSibling = node2filter;
				node2filter.rightSibling = filteredRoot;
				filteredRoot = node2filter;
			}
		} 
		
		//otherwise put node2filter on the right side of the parent
		else {
			if(parentNode.rightChild == null) {
				parentNode.rightChild = node2filter;
			} else if(parentNode.rightChild != node2filter) {
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
	}
	
	/**
	 * 
	 * @param instance2filter - the root of the sub tree to filter
	 * @param firstLevel - indicates whether the call refers to the first level call
	 * 
	 * filters a subtree from the value tree with root 'instance2filter' from the index trees
	 * will not filter the siblings of instance2filter
	 */
	private void filterTreeNode(SimpleTreeNode instance2filter, boolean firstLevel) {
		
		if(firstLevel && instance2filter.hardFiltered) return;
		
		TreeNode foundNode = builder.getNode((ISEMOSSNode)instance2filter.leaf);
		
		//if we find the node in the index tree and the root instance is hard not filtered
		if(foundNode != null) {
		
			//set node to be hard filtered
			//move to hard filtered bucket in tree node if need be
			//transitively filter the parents if need be
			if(firstLevel) {
//				boolean isFiltered = isFiltered(instance2filter);
//				boolean transFilterCheck = instance2filter.transitivelyFiltered > 0;
//				boolean hardFilterCheck = instance2filter.hardFiltered;
				
//				boolean wasHardFiltered = instance2filter.hardFiltered;
				instance2filter.hardFiltered = true;
				foundNode.hardFilter(instance2filter);
				
				//only filter parents if node was not filtered and became filtered
//				if(transFilterCheck && !hardFilterCheck) {
//					filterTreeNodeParents(instance2filter);
//				}
				
//				if(!wasHardFiltered) {
//					filterTreeNodeParents(instance2filter);
//				}
			} 
			
			//otherwise increment the transitive filter count and place in proper bucket
			else {
				instance2filter.incrementTransitiveFilter();
				foundNode.transitivelyFilter(instance2filter);
			}
			
			//filter the sibling if not in the first level
			if(!firstLevel && instance2filter.rightSibling!=null) {
				filterTreeNode(instance2filter.rightSibling, false);
			}
			
			//trans filter the left child
			if(instance2filter.leftChild!=null) {
				filterTreeNode(instance2filter.leftChild, false);
			}
			
			//trans filter the right child
			if(instance2filter.rightChild != null) {
				filterTreeNode(instance2filter.rightChild, false);
			}
		}
	}
	
	/**
	 * 
	 * @param instance2filter
	 * 
	 * Filters the parents of node 
	 */
	private void filterTreeNodeParents(SimpleTreeNode node) {
		SimpleTreeNode parentNode = node.parent;
		//if parent is not null and has no left children transitively filter the parents, repeat for parent's parent
		if(parentNode != null) {
			if(SimpleTreeNode.countNodeChildren(parentNode)==0) {
				TreeNode treeParentNode = this.getTreeNode(parentNode);
				treeParentNode.transitivelyFilter(parentNode);
				parentNode.incrementTransitiveFilter();
				
				filterTreeNodeParents(parentNode);
			}
		}
	}
	
	//---------------------------------------------------------------------------------------------------------------------------
	
	
	/**
	 * 
	 * @param objectToFilter - object to unfilter, or bring back visibility into the table
	 */
	public void unfilterTree(ITreeKeyEvaluatable objectToFilter) {
		//find the TreeNode associated with the object's value
		TreeNode foundNode = builder.getNode((ISEMOSSNode)objectToFilter);
		if(foundNode != null) {
			
			//copy references to new list to prevent concurrent modification exceptions			
			//Grab the hard filtered values
			List<SimpleTreeNode> filteredNodeList = new ArrayList<SimpleTreeNode>();
			for(SimpleTreeNode n: foundNode.getHardFilteredInstances()) {
				filteredNodeList.add(n);
			}
			
			//Grab the transitively filtered values
			for(SimpleTreeNode n: foundNode.getTransFilteredInstances()) {
				filteredNodeList.add(n);
			}
			
			for(SimpleTreeNode n: filteredNodeList) {
				
				//only adjust if n is hard filtered
				if(n.hardFiltered) {

					//parentCheck determines whether we should filter the parents on the tree node
					//if root no need to filter parents
//					boolean parentCheck = false;
//					if(n.parent == null) {
//						parentCheck = false;
//					}
//					//check if parent has a left child
//					else {
//						parentCheck = n.parent.leftChild == null;
//					}
					
					//remove the hard filters and reduce transitive filter on children/parents
					unfilterTreeNode(n, true);
					adjustRightChildren(n);

					
					//if n is root and is not filtered
					if(n.parent == null && !isFiltered(n)) {
						moveToLeftChild(n);
						adjustLeftChildren(n);
					}
					
					//if n is not root and parent is filtered move to Left
					else if(n.parent != null && isFiltered(n.parent)) {
						moveToLeftChild(n);
						adjustLeftChildren(n);
					}
					
					//if n is not root and parent is not filtered and this is not filtered
					else if(n.parent != null && !isFiltered(n.parent) && !isFiltered(n)) {
						moveToLeftChild(n);
						adjustLeftChildren(n);
					}
					
//					if(parentCheck) {
//						parentCheck = n.parent.leftChild != null;
//					}
					//filter parents if need be
//					if(parentCheck) {
//						if(n.parent.leaf.getValue().toString().equalsIgnoreCase("Summit")) {
//							System.out.println("");
//						}
//						adjustLeftChildren(n.parent);
						adjustRightChildren(n.parent);
						unfilterTreeNodeParents(n);
						adjustLeftChildren(n.parent);
//					}
				}
			}
		}
	}
	
	/**
	 * 
	 * @param n
	 * 
	 * move children to right from left that should be right
	 */
	private void adjustRightChildren(SimpleTreeNode n) {
		if(n != null) {
			Set<SimpleTreeNode> nodes = collectNodesForRightAdjustment(n, true, new HashSet<SimpleTreeNode>());
			for(SimpleTreeNode node : nodes) {
				moveSingleNodeToLeftChild(node);
			}
		}
	}
	
	/**
	 * 
	 * @param instance
	 * @param firstLevel
	 * @param nodeList
	 * @return
	 */
	private Set<SimpleTreeNode> collectNodesForRightAdjustment(SimpleTreeNode instance, boolean firstLevel, Set<SimpleTreeNode> nodeList){

		if(!firstLevel && instance.rightSibling != null && instance.rightSibling.transitivelyFiltered == 0 && !instance.rightSibling.hardFiltered) {
			nodeList.addAll(collectNodesForRightAdjustment(instance.rightSibling, false, nodeList));
			nodeList.add(instance.rightSibling);
		}
		
		if(instance.leftChild!=null) {
			nodeList.addAll(collectNodesForRightAdjustment(instance.leftChild, false, nodeList));
		}
		

		if(instance.rightChild != null && instance.rightChild.transitivelyFiltered == 0 && !instance.rightChild.hardFiltered) {
			nodeList.addAll(collectNodesForRightAdjustment(instance.rightChild, false, nodeList));
			nodeList.add(instance.rightChild);
		}
		
		return nodeList;
	}
	
	/**
	 * 
	 * @param n
	 * 
	 * move children from right to left that should be left
	 */
	private void adjustLeftChildren(SimpleTreeNode n) {
		if(n!=null) {
			Set<SimpleTreeNode> nodes = collectNodesForLeftAdjustment(n, true, new HashSet<SimpleTreeNode>());
			for(SimpleTreeNode node : nodes) {
				moveSingleNodeToRightChild(node);
			}
		}
	}
	
	private Set<SimpleTreeNode> collectNodesForLeftAdjustment(SimpleTreeNode instance, boolean firstLevel, Set<SimpleTreeNode> nodeList){

		if(!firstLevel && instance.rightSibling != null && (instance.rightSibling.transitivelyFiltered > 0 || instance.rightSibling.hardFiltered)) {
			nodeList.addAll(collectNodesForLeftAdjustment(instance.rightSibling, false, nodeList));
			nodeList.add(instance.rightSibling);
		}

		if(instance.leftChild != null && (instance.leftChild.transitivelyFiltered > 0 || instance.leftChild.hardFiltered)) {
			nodeList.add(instance.leftChild);
		}
		
		if(instance.leftChild != null) {
			nodeList.addAll(collectNodesForLeftAdjustment(instance.leftChild, false, nodeList));
		}
		
		return nodeList;
	}
	
	private void adjustRoots() {
		SimpleTreeNode root = builder.getRoot();
		List<SimpleTreeNode> moveToFilteredRoot = new ArrayList<SimpleTreeNode>();
		while(root != null) {
			if(isFiltered(root)) {
				moveToFilteredRoot.add(root);
			}
			root = root.rightSibling;
		}
		
		for(SimpleTreeNode node : moveToFilteredRoot) {
			this.moveToRightChild(node);
		}
		
		root = builder.getFilteredRoot();
		List<SimpleTreeNode> moveToRoot = new ArrayList<SimpleTreeNode>();
		while(root != null) {
			if(!isFiltered(root)) {
				moveToRoot.add(root);
			}
			root = root.rightSibling;
		}
		
		for(SimpleTreeNode node : moveToRoot) {
			this.moveToLeftChild(node);
		}
	}
	
	/**
	 * 
	 * @param node2filter
	 * @return
	 */
	private void moveToLeftChild(SimpleTreeNode node2filter) {
		SimpleTreeNode rootNode = builder.getRoot();
		SimpleTreeNode parentNode = node2filter.parent;
		boolean root = (parentNode == null);
		if((!root && parentNode.leftChild == node2filter) || rootNode == node2filter) return;
		
		//Grab the siblings

		if( (root && isFiltered(node2filter)) || (!root && parentNode.leftChild != node2filter)) {
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
					if(parentNode.rightChild == node2filter) {
						parentNode.rightChild = nodeRightSibling;
					}
				}
				nodeRightSibling.leftSibling = null;
			} else {
				//only child
				if(!root && parentNode.rightChild == node2filter) {
					parentNode.rightChild = null;
				}
			}
			
			//Isolate the node from the siblings
			node2filter.rightSibling = null;
			node2filter.leftSibling = null;
			
			//If node is root level, attach to root from filteredRoot
			if(root) {
				
				SimpleTreeNode filteredRoot = builder.getFilteredRoot();
				if(filteredRoot == node2filter) {
					filteredRoot = nodeRightSibling;
				}

//				SimpleTreeNode rootNode = builder.getRoot();
				if(rootNode != null) {
					rootNode.leftSibling = node2filter;
					node2filter.rightSibling = rootNode;
				}
			} 
			
			//otherwise put node2filter on the left side of the parent
			else {
				if(parentNode.leftChild == null) {
					parentNode.leftChild = node2filter;
				} else if(parentNode.leftChild != node2filter) {
					SimpleTreeNode leftChild = parentNode.leftChild;
					node2filter.leftSibling = leftChild;
					SimpleTreeNode nextRight = leftChild.rightSibling;
					leftChild.rightSibling = node2filter;
					if(nextRight != null) {
						nextRight.leftSibling = node2filter;
						node2filter.rightSibling = nextRight;
					}
				}
			}
		}
		
		//move parent to the left if parent is not hard filtered
		while(!root && SimpleTreeNode.countNodeChildren(parentNode)==1) {
//			parentNode.decrementTransitiveFilter();
			moveSingleNodeToLeftChild(parentNode);
//			unfilterTreeNode(parentNode);
//			if(node2filter.hardFiltered) {
//			parentNode.decrementTransitiveFilter();
//			}
			parentNode = parentNode.parent;
			root = parentNode == null;
		}
		
	}
	
	/**
	 * 
	 * @param node2filter
	 */
	private void moveSingleNodeToLeftChild(SimpleTreeNode node2filter) {
		SimpleTreeNode rootNode = builder.getRoot();
		
		SimpleTreeNode parentNode = node2filter.parent;
		boolean root = (parentNode == null);
		if((!root && parentNode.leftChild == node2filter) || rootNode == node2filter) return;
		
		//Grab the siblings

		//root && isFiltered(node2filter)
		if( (root && rootNode != node2filter) || (!root && parentNode.leftChild != node2filter)) {
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
					if(parentNode.rightChild == node2filter) {
						parentNode.rightChild = nodeRightSibling;
					}
				}
				nodeRightSibling.leftSibling = null;
			} else {
				//only child
				if(!root && parentNode.rightChild == node2filter) {
					parentNode.rightChild = null;
				}
			}
			
			//Isolate the node from the siblings
			node2filter.rightSibling = null;
			node2filter.leftSibling = null;
			
			//If node is root level, attach to filteredRoot
			if(root) {
				SimpleTreeNode filteredRoot = builder.getFilteredRoot();
				if(filteredRoot == node2filter) {
					filteredRoot = nodeRightSibling;
					nodeRightSibling.leftSibling = null;
				}
//				System.out.println("Before Root");
//				System.out.println("After Root");
				if(rootNode != null) {
					rootNode.leftSibling = node2filter;
					node2filter.rightSibling = rootNode;
				}
			} 
			
			//otherwise put node2filter on the left side of the parent
			else {
				if(parentNode.leftChild == null) {
					parentNode.leftChild = node2filter;
				} else if(parentNode.leftChild != node2filter)  {
					SimpleTreeNode leftChild = parentNode.leftChild;
					node2filter.leftSibling = leftChild;
					SimpleTreeNode nextRight = leftChild.rightSibling;
					leftChild.rightSibling = node2filter;
					if(nextRight != null) {
						nextRight.leftSibling = node2filter;
						node2filter.rightSibling = nextRight;
					}
				}
			}
		}
	}

	/**
	 * 
	 * @param instance
	 * @param firstLevel
	 */
	private void unfilterTreeNode(SimpleTreeNode instance, boolean firstLevel) {
		
		TreeNode foundNode = builder.getNode((ISEMOSSNode)instance.leaf);
		
		if(foundNode != null) {
				
			if(firstLevel) {
				instance.hardFiltered = false;
//				unfilterTreeNodeParents(instance);
			} else {
				instance.decrementTransitiveFilter();
			}
			foundNode.unfilter(instance);
			
			if(!firstLevel && instance.rightSibling!=null) {
				unfilterTreeNode(instance.rightSibling, false);
			}
			
			if(instance.leftChild!=null) {
				unfilterTreeNode(instance.leftChild, false);
			}
			
			if(instance.rightChild!=null) {
				unfilterTreeNode(instance.rightChild, false);
			}
		}
	}
	
	/**
	 * @param instance2filter - the single node to filter
	 * 
	 * unfilters a single node from its corresponding index tree
	 */
	private void unfilterTreeNode(SimpleTreeNode instance) {
		
		TreeNode foundNode = builder.getNode((ISEMOSSNode)instance.leaf);
		
		if(foundNode != null) {
			instance.decrementTransitiveFilter();
			foundNode.unfilter(instance);
		}
	}
	
	/**
	 * 
	 * @param instance
	 */
	private void unfilterTreeNodeParents(SimpleTreeNode instance) {
		SimpleTreeNode parentNode = instance.parent;
		if(parentNode != null) {
			if(parentNode.leftChild == null) return;
		
			SimpleTreeNode leftChild = parentNode.leftChild;
			while(leftChild != null) {
				if(!isFiltered(parentNode.leftChild)) {
					unfilterTreeNode(parentNode);
					unfilterTreeNodeParents(parentNode);
					break;
				}
				leftChild = leftChild.rightSibling;
			}
		}
	}
	
	/**
	 * 
	 * @param node
	 * @return true if node is filtered, false otherwise
	 */
	public boolean isFiltered(SimpleTreeNode node) {
		//find the node in the index tree
		TreeNode tnode = builder.getNode(((ISEMOSSNode)node.leaf));
		
		//Value does not exist
		if(tnode == null) {
			throw new IllegalArgumentException("Node with value "+node.leaf.getValue()+" does not exist");
		}
		
		return tnode.isFiltered(node);
	}
	
	@Override
	/**
	 * 
	 */
	public Object getFilterModel() {
		HashMap<String, Object[]> visibleValues = new HashMap<String, Object[]>();
		HashMap<String, Object[]> invisibleValues = new HashMap<String, Object[]>();
		
		String[] levelNames = table.getColumnHeaders();
		
		for(String column : levelNames) {
			IndexTreeIterator iterator = new IndexTreeIterator(builder.nodeIndexHash.get(column));
			ArrayList<Object> checkedValues = new ArrayList<>();
			ArrayList<Object> uncheckedValues = new ArrayList<>();
			while(iterator.hasNext()) {
				TreeNode node = iterator.next();
				if(node.instanceNode.size() > 0) {
					checkedValues.add(node.getValue());
				} else if(node.getFilteredInstances().size()> 0) {
					uncheckedValues.add(node.getValue());
				}
			}
			visibleValues.put(column, checkedValues.toArray());
			invisibleValues.put(column, uncheckedValues.toArray());
		}
		
		return new Object[]{visibleValues, invisibleValues};
	}
	
	/**
	 * 
	 * @return the filter model for the drop down tabs on the explore table
	 */
	public Object[] getRawFilterModel() {
		
		HashMap<String, Object[]> visibleValues = new HashMap<String, Object[]>();
		HashMap<String, Object[]> invisibleValues = new HashMap<String, Object[]>();
		
		String[] levelNames = table.getColumnHeaders();
		
		for(String column : levelNames) {
			IndexTreeIterator iterator = new IndexTreeIterator(builder.nodeIndexHash.get(column));
			ArrayList<Object> checkedValues = new ArrayList<>();
			ArrayList<Object> uncheckedValues = new ArrayList<>();
			while(iterator.hasNext()) {
				TreeNode node = iterator.next();
				if(node.instanceNode.size() > 0) {
					checkedValues.add(node.leaf.getRawValue());
				} else if(node.getHardFilteredInstances().size() > 0) {
					uncheckedValues.add(node.leaf.getRawValue());
				}
			}
			visibleValues.put(column, checkedValues.toArray());
			invisibleValues.put(column, uncheckedValues.toArray());
		}
		
		return new Object[]{visibleValues, invisibleValues};
	}

	/**
	 * get the TreeNode associated with the SimpleTreeNode
	 */
	private TreeNode getTreeNode(SimpleTreeNode node) {
		TreeNode treenode = builder.getNode((ISEMOSSNode)node.leaf);
		return treenode;
	}
	
	private ISEMOSSNode createNodeObject(Object value, Object rawValue, String level) {
		ISEMOSSNode node;

		if(value == null) {
			node = new StringClass(null, level); // TODO: fix this
		} else if(value instanceof Integer) {
			node = new IntClass((int)value, (int)value, level);
		} else if(value instanceof Number) {
			node = new DoubleClass(((Number) value).doubleValue(), ((Number) value).doubleValue(), level);
		} else if(value instanceof String) {
			if(rawValue instanceof Literal) {
				node = new StringClass((String)value, (String) value, level);
			} else {
				node = new StringClass((String)value, (String) rawValue.toString(), level);
			}
		}
//		else if(value instanceof Boolean) {
//			node = new BooleanClass((boolean)value, level);
//		}
		else {
			node = new StringClass(value.toString(), level);
		}
		return node;
	}

	@Override
	public void reset() {
		//for all columns, change all nodes to unfiltered
		//make all nodes left children
	}
	
	public Map<String, Object[]> getFilterTransformationsValues() {
		String[] columnHeaders = table.getColumnHeaders();
		Map<String, Object[]> retMap = new HashMap<String, Object[]>(columnHeaders.length);
		for(String column : columnHeaders) {
			Object[] filteredValues = table.getFilteredUniqueRawValues(column);
			if(filteredValues.length > 0) {
				retMap.put(column, table.getUniqueRawValues(column));
			}
		}
		return retMap;
	}
}
