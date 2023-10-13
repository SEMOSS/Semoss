package prerna.reactor.panel.comments;

import java.util.List;
import java.util.Map;

import prerna.om.InsightPanel;
import prerna.reactor.panel.AbstractInsightPanelReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class UpdatePanelCommentReactor extends AbstractInsightPanelReactor {
	
	public UpdatePanelCommentReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.COMMENT_KEY.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// get the insight panel
		InsightPanel insightPanel = getInsightPanel();
		// get the ornaments that come as a map
		Map<String, Object> comment = getCommentInputs();
		// merge the map options
		insightPanel.updateComment(comment);
		return new NounMetadata(insightPanel, PixelDataType.PANEL, PixelOperationType.PANEL_COMMENT);
	}

	private Map<String, Object> getCommentInputs() {
		// see if it was passed directly in with the lower case key ornaments
		GenRowStruct genericReactorGrs = this.store.getNoun(keysToGet[1]);
		if(genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
			return (Map<String, Object>) genericReactorGrs.get(0);
		}
		
		// see if it is in the curRow
		// if it was passed directly in as a variable
		List<NounMetadata> panelNouns = this.curRow.getNounsOfType(PixelDataType.MAP);
		if(panelNouns != null && !panelNouns.isEmpty()) {
			return (Map<String, Object>) panelNouns.get(0).getValue();
		}
		
		// well, you are out of luck
		return null;
	}
}
