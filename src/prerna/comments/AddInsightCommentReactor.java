package prerna.comments;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AddInsightCommentReactor extends AbstractReactor {

	public AddInsightCommentReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COMMENT_KEY.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.insight.getProjectId();
		String projectName = this.insight.getProjectName();
		String rdbmsId = this.insight.getRdbmsId();
		String comment = this.keyValue.get(this.keysToGet[0]);
		if(comment == null || comment.trim().isEmpty()) {
			throw new IllegalArgumentException("Need a comment to save");
		}
		
		// after grabbing the input, write it to a file		
		InsightComment iComment = new InsightComment(projectId, projectName, rdbmsId);
		iComment.setComment(comment);
		iComment.setAction(InsightComment.ADD_ACTION);
		// add the comment to the chain
		this.insight.addInsightComment(iComment);
		return new NounMetadata(iComment.getId(), PixelDataType.CONST_STRING);
	}
}
