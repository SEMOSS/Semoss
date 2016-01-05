package prerna.ds;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.IMatcher.MATCHER_ACTION;

public class InstanceOuterJoinMatcher extends InstanceMatcher {

	private static final Logger LOGGER = LogManager.getLogger(InstanceOuterJoinMatcher.class.getName());

	public InstanceOuterJoinMatcher() {
		super();
	}
	
	protected List<TreeNode[]> performMatch(IndexTreeIterator it1, IndexTreeIterator it2) {
		
		List<TreeNode[]> results = new ArrayList<TreeNode[]>();
		
		boolean thereAreNext = it1.hasNext() && it2.hasNext();
		//get initial value 1
		TreeNode nextNode1 = it1.next();
		TreeNode nextNode2 = it2.next();
		
		while(thereAreNext){
			
			
			int comp = compare(nextNode1, nextNode2);
			// -1 means key1 is ahead
			// 0 means they match
			// 1 means key 2 is ahead
			
			// if key1 is ahead, get next for key2//
			// if no next for key2, we are done
			if(comp == -1){
				LOGGER.info("comp is -1..... adding blank to second col and advancing second column");
				// add row with empty
				TreeNode[] row = new TreeNode[2];
				row[0] = new TreeNode(new StringClass(SimpleTreeNode.EMPTY));
				row[1] = nextNode2;
				results.add(row);
				
				if(it2.hasNext()){
					nextNode2 = it2.next();
//					key2 = nextNode2.leaf.getValue().toString();
					continue;
				}
			}
			// same logic for key2 ahead
			else if(comp == 1){
				LOGGER.info("comp is 1..... adding first col to blank and advancing first column");
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
				break;
			}
		}
		
		return results;
	}

	@Override
	public MATCHER_ACTION getQueryModType() {
		LOGGER.info("Getting query mod type");
		return MATCHER_ACTION.NONE;
	}
}

