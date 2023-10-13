package prerna.reactor.panel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.om.InsightPanel;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.insight.InsightUtility;

public class RefreshPanelViewReactor extends AbstractReactor {

	public RefreshPanelViewReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.PANEL.getKey()};
	}

	@Override
	public NounMetadata execute() {
		// get the filters if any
		List<String> panelIds = getIds();

		List<NounMetadata> nounList = new Vector<>();
		
		Map<String, InsightPanel> insightPanelsMap = this.insight.getInsightPanels();
		for(String panelId : insightPanelsMap.keySet()) {
			if(panelIds == null || panelIds.contains(panelId)) {
				InsightPanel insightPanel = insightPanelsMap.get(panelId);
				if(!insightPanel.getPanelView().equalsIgnoreCase("text-editor")) {
					continue;
				}
				
				Map<String, String> returnMap = new HashMap<String, String>();
				returnMap.put("panelId", insightPanel.getPanelId());
				returnMap.put("view", insightPanel.getPanelView());
				// grab the options for this view
				returnMap.put("options", insightPanel.getPanelActiveViewOptions());
				String renderedViewOptions = InsightUtility.recalculateHtmlViews(this.insight, insightPanel);
				returnMap.put("renderedOptions", renderedViewOptions);
				
				NounMetadata noun = new NounMetadata(returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.PANEL_VIEW);
				nounList.add(noun);
			}
		}

		return new NounMetadata(nounList, PixelDataType.VECTOR);
	}
	
	private List<String> getIds() {
		List<String> panelIds = null;
		// try the key
		GenRowStruct panelGrs = store.getNoun(keysToGet[0]);
		if(panelGrs != null && !panelGrs.isEmpty()) {
			int size = panelGrs.size();
			panelIds = new Vector<String>(size);
			for(int i = 0; i < size; i++) {
				NounMetadata noun = panelGrs.getNoun(i);
				if(noun.getNounType() == PixelDataType.PANEL) {
					panelIds.add( ((InsightPanel) noun.getValue()).getPanelId() ); 
				} else {
					panelIds.add(noun.getValue().toString());
				}
			}
			return panelIds;
		}

		// direct values
		if(!this.curRow.isEmpty()) {
			int size = curRow.size();
			panelIds = new Vector<String>(size);
			for(int i = 0; i < size; i++) {
				NounMetadata noun = curRow.getNoun(i);
				if(noun.getNounType() == PixelDataType.PANEL) {
					panelIds.add( ((InsightPanel) noun.getValue()).getPanelId() ); 
				} else {
					panelIds.add(noun.getValue().toString());
				}
			}
			return panelIds;
		}
		
		return null;
	}

}
