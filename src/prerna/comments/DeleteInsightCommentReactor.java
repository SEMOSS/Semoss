package prerna.comments;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class DeleteInsightCommentReactor extends AbstractReactor {

	public DeleteInsightCommentReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COMMENT_ID_KEY.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.insight.getEngineId();
		String engineName = this.insight.getEngineName();
		String rdbmsId = this.insight.getRdbmsId();
		
		String commentId = this.keyValue.get(this.keysToGet[0]);
		if(commentId == null || commentId.trim().isEmpty()) {
			throw new IllegalArgumentException("Need to define the comment id");
		}
		
		// after grabbing the input, write it to a file		
		InsightComment iComment = new InsightComment(engineId, engineName, rdbmsId);
		iComment.setId(commentId);
		iComment.setAction(InsightComment.DELETE_ACTION);
		// add the comment to the chain
		this.insight.addInsightComment(iComment);
		return new NounMetadata(iComment.getId(), PixelDataType.CONST_STRING);
	}
	
}