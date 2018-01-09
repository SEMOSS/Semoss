package prerna.comments;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.AbstractReactor;

public class AddInsightCommentReactor extends AbstractReactor {

	public AddInsightCommentReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey(),
				ReactorKeysEnum.INSIGHT_ID.getKey(),
				ReactorKeysEnum.COMMENT_KEY.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engine = this.keyValue.get(this.keysToGet[0]);
		if(engine == null) {
			throw new IllegalArgumentException("Need to know which engine this insight belongs to");
		}
		String rdbmsId = this.keyValue.get(this.keysToGet[1]);
		if(rdbmsId == null) {
			throw new IllegalArgumentException("Need to know which insight this comment belongs to");
		}
		String comment = this.keyValue.get(this.keysToGet[2]);
		if(comment == null || comment.trim().isEmpty()) {
			throw new IllegalArgumentException("Need a comment to save");
		}
		
		// after grabbing the input, write it to a file		
		InsightComment iComment = new InsightComment();
		iComment.setComment(comment);
		iComment.setCreatedTimeStamp(InsightComment.getCurrentDate());
		iComment.writeToFile(engine, rdbmsId, comment);
		return new NounMetadata(iComment.getCommentId(), PixelDataType.CONST_STRING);
	}
	
}
