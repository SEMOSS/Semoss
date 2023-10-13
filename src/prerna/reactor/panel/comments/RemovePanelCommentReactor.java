package prerna.reactor.panel.comments;

import java.util.List;

import prerna.om.InsightPanel;
import prerna.reactor.panel.AbstractInsightPanelReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RemovePanelCommentReactor extends AbstractInsightPanelReactor {
	
	public RemovePanelCommentReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.COMMENT_KEY.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// get the insight panel
		InsightPanel insightPanel = getInsightPanel();
		// get the comment id
		String commentId = getCommentInputs();
		if(commentId == null) {
			insightPanel.resetComments();
		} else {
			insightPanel.removeComment(commentId);
		}
		return new NounMetadata(insightPanel, PixelDataType.PANEL, PixelOperationType.PANEL_COMMENT);
	}
	
	private String getCommentInputs() {
		// see if it was passed directly in with the lower case key comment
		GenRowStruct genericReactorGrs = this.store.getNoun(keysToGet[1]);
		if(genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
			return genericReactorGrs.get(0).toString();
		}
		
		// see if it is in the curRow
		// if it was passed directly in as a variable
		List<NounMetadata> panelNouns = this.curRow.getNounsOfType(PixelDataType.CONST_STRING);
		if(panelNouns != null && !panelNouns.isEmpty()) {
			return panelNouns.get(0).getValue().toString();
		}
		
		// well, you are out of luck
		return null;
	}

}
