package prerna.comments;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.AbstractReactor;

public class ModifyInsightCommentReactor extends AbstractReactor {

	public ModifyInsightCommentReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey(),
				ReactorKeysEnum.INSIGHT_ID.getKey(),
				ReactorKeysEnum.COMMENT_ID_KEY.getKey(),
				ReactorKeysEnum.COMMENT_KEY.getKey()};
	};

	@Override
	public NounMetadata execute() {
//		organizeKeys();
//		String engine = this.keyValue.get(this.keysToGet[0]);
//		if(engine == null) {
//			throw new IllegalArgumentException("Need to define which engine this insight belongs to");
//		}
//		String rdbmsId = this.keyValue.get(this.keysToGet[1]);
//		if(rdbmsId == null) {
//			throw new IllegalArgumentException("Need to define which insight this comment belongs to");
//		}
//		String commentId = this.keyValue.get(this.keysToGet[2]);
//		if(commentId == null || commentId.trim().isEmpty()) {
//			throw new IllegalArgumentException("Need to define the comment id");
//		}
//		String newComment = this.keyValue.get(this.keysToGet[3]);
//		if(newComment == null || newComment.trim().isEmpty()) {
//			throw new IllegalArgumentException("Need a comment to save");
//		}
//		
//		// after grabbing the input, write it to a file		
//		InsightComment iComment = new InsightComment();
//		iComment.loadFromFile(engine, rdbmsId, commentId);
//		iComment.setComment(newComment);
//		iComment.setLastModifedTimeStamp(InsightComment.getCurrentDate());
//		iComment.writeToFile(engine, rdbmsId, newComment);
//		return new NounMetadata(iComment.getCommentId(), PixelDataType.CONST_STRING);
		return null;
	}
	
}
