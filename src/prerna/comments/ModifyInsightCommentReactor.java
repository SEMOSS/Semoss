package prerna.comments;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ModifyInsightCommentReactor extends AbstractReactor {

	public ModifyInsightCommentReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COMMENT_ID_KEY.getKey(),
				ReactorKeysEnum.COMMENT_KEY.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.insight.getProjectId();
		String projectName = this.insight.getProjectName();
		String rdbmsId = this.insight.getRdbmsId();
		
		String commentId = this.keyValue.get(this.keysToGet[0]);
		if(commentId == null || commentId.trim().isEmpty()) {
			throw new IllegalArgumentException("Need to define the comment id");
		}
		String newComment = this.keyValue.get(this.keysToGet[1]);
		if(newComment == null || newComment.trim().isEmpty()) {
			throw new IllegalArgumentException("Need a comment to save");
		}
		
		// after grabbing the input, write it to a file		
		InsightComment iComment = new InsightComment(projectId, projectName, rdbmsId);
		iComment.setId(commentId);
		// after we set the id
		// we need to modify the previous id with the new timestamp of this created object
		iComment.modifyExistingIdWithDate();
		iComment.setComment(newComment);
		iComment.setAction(InsightComment.EDIT_ACTION);
		// add the comment to the chain
		this.insight.addInsightComment(iComment);
		return new NounMetadata(iComment.getId(), PixelDataType.CONST_STRING);
	}
	
}
