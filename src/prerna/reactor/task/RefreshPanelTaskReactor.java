package prerna.reactor.task;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.Logger;

import prerna.om.InsightPanel;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.insight.InsightUtility;

public class RefreshPanelTaskReactor extends AbstractReactor {

	private static final String CLASS_NAME = RefreshPanelTaskReactor.class.getName();

	public RefreshPanelTaskReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.LIMIT.getKey()};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		// store the tasks to reset
		List<NounMetadata> taskOutput = new ArrayList<>();
		// get the filters if any
		List<String> panelIds = getIds();
		// get the limit for the new tasks
		Integer limit = getTotalToCollect();
		// store additional messages to send to the user
		List<NounMetadata> additionalMessages = new ArrayList<>();
		
		Map<String, InsightPanel> insightPanelsMap = this.insight.getInsightPanels();
		for(String panelId : insightPanelsMap.keySet()) {
			if(panelIds == null || panelIds.contains(panelId)) {
				InsightPanel panel = insightPanelsMap.get(panelId);
				if(!panel.getPanelView().equalsIgnoreCase("visualization")) {
					continue;
				}
				Integer panelCollect = limit;
				if(panelCollect == null) {
					panelCollect = panel.getNumCollect();
				}
				// need to account for layers
				// so will loop through the layer maps
				// that we are storing
				InsightUtility.refreshPanelTasks(this.insight, panel, panelCollect, taskOutput, additionalMessages, logger);
			}
		}
		
		NounMetadata noun = new NounMetadata(taskOutput, PixelDataType.TASK_LIST, PixelOperationType.RESET_PANEL_TASKS);
		if(!additionalMessages.isEmpty()) {
			noun.addAllAdditionalReturn(additionalMessages);
		}
		return noun;
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


	//returns how much do we need to collect
	private Integer getTotalToCollect() {
		// try the key
		GenRowStruct numGrs = store.getNoun(keysToGet[1]);
		if(numGrs != null && !numGrs.isEmpty()) {
			return ((Number) numGrs.get(0)).intValue();
		}

		return null;
	}
}
