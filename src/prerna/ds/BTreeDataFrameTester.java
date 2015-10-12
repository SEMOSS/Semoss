package prerna.ds;


public class BTreeDataFrameTester {
	

	
	/**
	 * 
	 * @param btree
	 * @param height
	 * @return returns true if every branch is of the height passed in the parameter
	 */
	public static boolean isBalanced(BTreeDataFrame btree, int height) {
		SimpleTreeNode root = btree.getBuilder().getRoot();
		return checkBranch(root, 1, height);
	}
	
	private static boolean checkBranch(SimpleTreeNode node, int level, int height) {
		
		//branch just right size
		if(level == height && node.leftChild == null) {
			return true;
		}
		
		//branch too long
		if(level == height && node.leftChild != null) {
			return false;
		}
		
		//branch not long enough
		if(level != height && node.leftChild == null) {
			return false;
		}
		
		if(node.rightSibling != null) {
			return checkBranch(node.leftChild, level+1, height) && checkBranch(node.rightSibling, level, height);
		} else {
			return checkBranch(node.leftChild, level+1, height);
		}
	}
	
	/**
	 * 
	 * @param btree
	 * @return
	 */
	public static boolean isProperlyStructured(BTreeDataFrame btree) {
		
		//for each SimpleTreeNode in the value tree, check sibling relationships, check parent/child relationship
		SimpleTreeNode root = btree.getBuilder().getRoot();
		return checkTreeRelationships(root);
	}
	
	private static boolean checkTreeRelationships(SimpleTreeNode root) {
		//does not check filtered structure
		if(root == null) return true;
		return checkNodeRelationships(root) && checkTreeRelationships(root.rightSibling) && checkTreeRelationships(root.leftChild);
	}
	
	private static boolean checkNodeRelationships(SimpleTreeNode node) {
		
		if(node == null) return true;
		
		SimpleTreeNode leftSibling = node.leftSibling;
		SimpleTreeNode rightSibling = node.rightSibling;
		
		if(leftSibling != null && leftSibling.rightSibling != node) {
			System.err.println("left sibling relationship error");
			return false;
		}
		
		if(rightSibling != null && rightSibling.leftSibling != node) {
			System.err.println("right sibling relationship error");
			return false;
		}
		
		SimpleTreeNode parent = node.parent;
		if(parent != null && leftSibling == null) {
			if(parent.leftChild != node) {
				System.err.println("parent child relationship error");
				return false;
			}
		} else if(parent != null){
			if(parent.leftChild != SimpleTreeNode.getLeft(node)) {
				System.err.println("parent child relationship error");
			}
		}
		
		return true;
	}
}
