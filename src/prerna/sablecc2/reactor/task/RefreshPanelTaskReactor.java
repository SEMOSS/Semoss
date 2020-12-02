package prerna.sablecc2.reactor.task;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.logging.log4j.Logger;

import prerna.om.InsightPanel;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.options.TaskOptions;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;
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
		List<NounMetadata> taskOutput = new Vector<NounMetadata>();
		// get the filters if any
		List<String> panelIds = getIds();
		// get the limit for the new tasks
		int limit = getTotalToCollect();

		Map<String, InsightPanel> insightPanelsMap = this.insight.getInsightPanels();
		for(String panelId : insightPanelsMap.keySet()) {
			if(panelIds == null || panelIds.contains(panelId)) {
				InsightPanel panel = insightPanelsMap.get(panelId);
				if(!panel.getPanelView().equalsIgnoreCase("visualization")) {
					continue;
				}
				// need to account for layers
				// so will loop through the layer maps
				// that we are storing
				Map<String, SelectQueryStruct> lQs = panel.getLayerQueryStruct();
				Map<String, TaskOptions> lTaskOption = panel.getLayerTaskOption();

				if(lQs != null && lTaskOption != null) {
					Set<String> layers = lQs.keySet();
					for(String layerId : layers) {
						SelectQueryStruct qs = lQs.get(layerId);
						TaskOptions taskOptions = lTaskOption.get(layerId);
						
						if(qs != null && taskOptions != null) {
							logger.info("Found task for panel = " + Utility.cleanLogString(panelId));
							// this will ensure we are using the latest panel and frame filters on refresh
							BasicIteratorTask task = InsightUtility.constructTaskFromQs(this.insight, qs);
							task.setLogger(logger);
							task.toOptimize(true);
							task.setLogger(logger);
							task.setTaskOptions(taskOptions);
							// we store the formatter in the task
							// so we can ensure we are properly painting
							// the visualization (graph visuals)
							if(taskOptions.getFormatter() != null) {
								task.setFormat(taskOptions.getFormatter());
							}
							try {
								task.setNumCollect(limit);
								task.optimizeQuery(limit);
							} catch (Exception e) {
								e.printStackTrace();
								throw new SemossPixelException(e.getMessage());
							}
							taskOutput.add(new NounMetadata(task, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA));
						}
					}
				}
			}
		}
		
		return new NounMetadata(taskOutput, PixelDataType.TASK_LIST, PixelOperationType.RESET_PANEL_TASKS);
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
	private int getTotalToCollect() {
		// try the key
		GenRowStruct numGrs = store.getNoun(keysToGet[1]);
		if(numGrs != null && !numGrs.isEmpty()) {
			return ((Number) numGrs.get(0)).intValue();
		}

		// default to 500
		return 500;
	}
}
