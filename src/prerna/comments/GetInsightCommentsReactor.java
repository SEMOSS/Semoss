package prerna.comments;

import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetInsightCommentsReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		LinkedList<InsightComment> commentObjs = this.insight.getInsightComments();
		
		List<String> comments = new Vector<String>();
		for(InsightComment c : commentObjs) {
			comments.add(c.getComment());
		}
		return new NounMetadata(comments, PixelDataType.VECTOR);
	}
	
}
