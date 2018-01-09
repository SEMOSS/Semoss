package prerna.comments;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.AbstractReactor;

public class AddInsightCommentReactor extends AbstractReactor {

	public AddInsightCommentReactor() {
		this.keysToGet = new String[]{
//				ReactorKeysEnum.ENGINE.getKey(),
//				ReactorKeysEnum.INSIGHT_ID.getKey(),
				ReactorKeysEnum.COMMENT_KEY.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engine = this.insight.getEngineName();
		String rdbmsId = this.insight.getRdbmsId();
//		if(engine == null) {
//			throw new IllegalArgumentException("Need to define which engine this insight belongs to");
//		}
//		String rdbmsId = this.keyValue.get(this.keysToGet[1]);
//		if(rdbmsId == null) {
//			throw new IllegalArgumentException("Need to define which insight this comment belongs to");
//		}
		String comment = this.keyValue.get(this.keysToGet[0]);
		if(comment == null || comment.trim().isEmpty()) {
			throw new IllegalArgumentException("Need a comment to save");
		}
		
		// after grabbing the input, write it to a file		
		InsightComment iComment = new InsightComment(engine, rdbmsId);
		iComment.setComment(comment);
		iComment.setAction(InsightComment.ADD_ACTION);
		// add the comment to the chain
		this.insight.addInsightComment(iComment);
		return new NounMetadata(iComment.getId(), PixelDataType.CONST_STRING);
	}
	
}
