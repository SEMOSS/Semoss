package prerna.ds;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;

public class ExactStringPartialOuterJoinMatcher extends ExactStringMatcher {

	private static final Logger LOGGER = LogManager.getLogger(ExactStringPartialOuterJoinMatcher.class.getName());

	/**
	 * Assumes the partial outer join is keeping all values in table1Col for performMatch method
	 */
	public ExactStringPartialOuterJoinMatcher() {
		super();
	}

	
	protected List<TreeNode[]> performMatch(IndexTreeIterator it1, IndexTreeIterator it2) {
		
		List<TreeNode[]> results = new ArrayList<TreeNode[]>();
		
		boolean thereAreNext = it1.hasNext() && it2.hasNext();
		TreeNode nextNode1 = null;
		TreeNode nextNode2 = null;
		//get initial value 1
		if(thereAreNext) {
			nextNode1 = it1.next();
			nextNode2 = it2.next();
		}
		
		while(thereAreNext){
			
			
			int comp = compare(nextNode1, nextNode2);
			// -1 means key1 is ahead
			// 0 means they match
			// 1 means key 2 is ahead
			
			// if key1 is ahead, get next for key2
			// if no next for key2, we are done
			if(comp == -1){
				// because its matching to the right, we do not add a row in this case
				if(it2.hasNext()){
					nextNode2 = it2.next();
//					key2 = nextNode2.leaf.getValue().toString();
					continue;
				}
				else {
					thereAreNext = false;
				}
			}
			// same logic for key2 ahead
			else if(comp == 1){
				// add row with empty
				TreeNode[] row = new TreeNode[2];
				row[0] = nextNode1;
				row[1] = new TreeNode(new StringClass(SimpleTreeNode.EMPTY));
				results.add(row);
				
				if(it1.hasNext()){
					nextNode1 = it1.next();
//					key1 = nextNode1.leaf.getValue().toString();
					continue;
				}
			}
			// if we have a match, add row to the return
			else if (comp == 0){
//				addMatches(results, nextNode1, nextNode2);

				TreeNode[] row = new TreeNode[2];
				row[0] = nextNode1;
				row[1] = nextNode2;
				results.add(row);
				
				if(it1.hasNext() && it2.hasNext()){
					nextNode1 = it1.next();
					nextNode2 = it2.next();
					continue;
				}
			}
			// this means one iterator has run out but not both. Need to figure out which one is still good to go
			if(it1.hasNext()){
				nextNode1 = it1.next();
				continue;
			}
			else if (it2.hasNext()){
				nextNode2 = it2.next();
				continue;
			}
			else { // we are truly out of next
				thereAreNext = false;
				continue;
			}
		}
		
		return results;
	}

}

