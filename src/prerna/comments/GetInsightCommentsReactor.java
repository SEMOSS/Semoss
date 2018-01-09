package prerna.comments;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetInsightCommentsReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		List<String> comments = new Vector<String>();
		Map<String, Integer> commentToIndex = new HashMap<String, Integer>(); 
		List<Integer> indicesToRemove = new ArrayList<Integer>();

		LinkedList<InsightComment> commentObjs = this.insight.getInsightComments();
		int size = commentObjs.size();
		for(int commentIndex = 0; commentIndex < size; commentIndex++) {
			InsightComment c = commentObjs.get(commentIndex);
			String comment = c.getComment();
			String id = c.getId();
			String action = c.getAction();
			
			if(action.equals(InsightComment.ADD_ACTION)) {
				// just add it to the list
				comments.add(comment);
				String subId = InsightComment.getIdMinusTimestamp(id);
				commentToIndex.put(subId, comments.size()-1);
			} else if(action.equals(InsightComment.EDIT_ACTION)) {
				// find the original index
				// and modify the value
				String subId = InsightComment.getIdMinusTimestamp(id);
				int indexToModify = commentToIndex.get(subId);				
				comments.remove(indexToModify);
				comments.add(indexToModify, comment);
			} else if(action.equals(InsightComment.DELETE_ACTION)) {
				// store the index and then remove at the end
				// so we do not need to all the indices in commentToIndex map
				String subId = InsightComment.getIdMinusTimestamp(id);
				int indexToRemove = commentToIndex.get(subId);
				indicesToRemove.add(indexToRemove);
			}
		}
		
		// now delete using the largest index and moving back
		int numRemove = indicesToRemove.size();
		if(numRemove > 0) {
			Collections.sort(indicesToRemove);
			for(int removeIndex = 0; removeIndex < numRemove; removeIndex++) {
				comments.remove(indicesToRemove.get(numRemove-removeIndex).intValue());
			}
		}
		
		return new NounMetadata(comments, PixelDataType.VECTOR);
	}
	
}
