package prerna.sablecc2.reactor.task;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.om.InsightPanel;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.options.TaskOptions;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.export.CollectPivotReactor;
import prerna.sablecc2.reactor.export.IFormatter;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.insight.InsightUtility;

public class RefreshAllPanelTasksReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(RefreshAllPanelTasksReactor.class);
	private static final String CLASS_NAME = RefreshAllPanelTasksReactor.class.getName();

	public RefreshAllPanelTasksReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.LIMIT.getKey()};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		// store the tasks to reset
		List<NounMetadata> taskOutput = new Vector<NounMetadata>();
		// get the limit for the new tasks
		Integer limit = getTotalToCollect();
				
		List<NounMetadata> additionalMessages = new Vector<>();

		Map<String, InsightPanel> insightPanelsMap = this.insight.getInsightPanels();
		for(String panelId : insightPanelsMap.keySet()) {
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
			Map<String, SelectQueryStruct> lQs = panel.getLayerQueryStruct();
			Map<String, TaskOptions> lTaskOption = panel.getLayerTaskOption();
			Map<String, IFormatter> lFormatter = panel.getLayerFormatter();

			if(lQs != null && lTaskOption != null) {
				Set<String> layers = lQs.keySet();
				LAYER_LOOP : for(String layerId : layers) {
					SelectQueryStruct qs = lQs.get(layerId);
					// reset the panel specific objects so we can pick up the latest state
					qs.setPanelList(new Vector<InsightPanel>());
					qs.setPanelIdList(new Vector<String>());
					qs.setPanelOrderBy(new Vector<IQuerySort>());
					// add the panel
					qs.addPanel(panel);
					qs.resetPanelState();
					TaskOptions taskOptions = lTaskOption.get(layerId);
					IFormatter formatter = lFormatter.get(layerId);

					if(qs != null && taskOptions != null) {
						logger.info("Found task for panel = " + Utility.cleanLogString(panelId));
						// this will ensure we are using the latest panel and frame filters on refresh
						BasicIteratorTask task = InsightUtility.constructTaskFromQs(this.insight, qs);
						task.setFormat(formatter);
						try {
							executeTask(task, taskOptions, panelCollect, logger);	
						} catch(Exception e) {
							logger.info("Previous query on panel " + panelId + " does not work");
							classLogger.error(Constants.STACKTRACE, e);
							// see if the frame at least exists
							ITableDataFrame queryFrame = qs.getFrame();
							if(queryFrame == null || queryFrame.isClosed()) {
								additionalMessages.add(getError("Attempting to refresh panel id " + panelId 
										+ " but the frame creating the visualization no longer exists"));
								continue LAYER_LOOP;
							} 
							
							NounMetadata warning = getWarning("Attempting to refresh panel id " + panelId 
									+ " but the underlying data creating the visualization no longer exists "
									+ "or is now incompatible with the view. Displaying a grid of the data.");
							
							SelectQueryStruct allQs = queryFrame.getMetaData().getFlatTableQs(true);
							allQs.setFrame(queryFrame);
							allQs.setQsType(QUERY_STRUCT_TYPE.FRAME);
							allQs.setQueryAll(true);
							task = new BasicIteratorTask(allQs);
							taskOptions = new TaskOptions(AutoTaskOptionsHelper.generateGridTaskOptions(allQs, panelId));
							try {
								executeTask(task, taskOptions, panelCollect, logger);
								additionalMessages.add(warning);
							} catch (Exception e1) {
								// at this point - no luck :/
								classLogger.error(Constants.STACKTRACE, e);
								additionalMessages.add(getError("Attempingt to refresh panel id " + panelId 
											+ " but the underlying data creating the visualization no longer exists "
											+ " or is now incompatible with the view. Displaying a grid of the data "
											+ " errors with the following message: " + e.getMessage()));
								continue LAYER_LOOP;
							}
						}
						
						// is this a pivot?
						Set<String> taskPanelIds = taskOptions.getPanelIds();
						String layout = taskOptions.getLayout(taskPanelIds.iterator().next());
						if(layout.equals("PivotTable")) {
							CollectPivotReactor pivot = new CollectPivotReactor();
							pivot.In();
							pivot.setInsight(this.insight);
							pivot.setNounStore(taskOptions.getCollectStore());
							GenRowStruct grs = taskOptions.getCollectStore().makeNoun(PixelDataType.TASK.getKey());
							grs.clear();
							grs.add(new NounMetadata(task, PixelDataType.TASK));
							taskOutput.add(pivot.execute());
						} else {
							taskOutput.add(new NounMetadata(task, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA));
						}
					}
				}
			}
		}

		NounMetadata noun = new NounMetadata(taskOutput, PixelDataType.TASK_LIST, PixelOperationType.RESET_PANEL_TASKS);
		if(!additionalMessages.isEmpty()) {
			noun.addAllAdditionalReturn(additionalMessages);
		}
		return noun;
	}
	
	/**
	 * Use this to actually build and execute the task
	 * @param task
	 * @param taskOptions
	 * @param limit
	 * @param logger
	 * @throws Exception
	 */
	private void executeTask(BasicIteratorTask task, TaskOptions taskOptions, int limit, Logger logger) throws Exception {
		task.setLogger(logger);
		task.toOptimize(true);
		task.setTaskOptions(taskOptions);
		task.setNumCollect(limit);
		task.optimizeQuery(limit);
	}

	//returns how much do we need to collect
	private Integer getTotalToCollect() {
		// try the key
		GenRowStruct numGrs = store.getNoun(keysToGet[0]);
		if(numGrs != null && !numGrs.isEmpty()) {
			return ((Number) numGrs.get(0)).intValue();
		}

		return null;
	}
}
