package prerna.solr.reactor;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.reactor.AbstractReactor;
import prerna.reactor.frame.FrameFactory;
import prerna.reactor.imports.IImporter;
import prerna.reactor.imports.ImportFactory;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class InsightUsageStatisticsReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(InsightUsageStatisticsReactor.class);

	private static List<String> META_KEYS_LIST = new Vector<String>();
	static {
		META_KEYS_LIST.add("description");
		META_KEYS_LIST.add("tag");
	}
	
	public InsightUsageStatisticsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.FILTER_WORD.getKey(), 
				ReactorKeysEnum.TAGS.getKey(), ReactorKeysEnum.PANEL.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		GenRowStruct projectGrsFilters = this.store.getNoun(this.keysToGet[0]);
		List<NounMetadata> warningNouns = new Vector<>();
		// get list of engineIds if user has access
		List<String> pFilters = null;
		if (projectGrsFilters != null && !projectGrsFilters.isEmpty()) {
			pFilters = new Vector<String>();
			for (int i = 0; i < projectGrsFilters.size(); i++) {
				String engineFilter = projectGrsFilters.get(i).toString();
				engineFilter = SecurityProjectUtils.testUserProjectIdForAlias(this.insight.getUser(), engineFilter);
				if (SecurityProjectUtils.userCanViewProject(this.insight.getUser(), engineFilter)) {
					pFilters.add(engineFilter);
				} else {
					// store warnings
					warningNouns.add(NounMetadata.getWarningNounMessage(engineFilter + " does not exist or user does not have access to project."));
				}
			}
		}
		String searchTerm = this.keyValue.get(this.keysToGet[1]);
		List<String> tagFilters = getTags();
		
		// create new frame to store the data
		ITableDataFrame newFrame = null;
		try {
			newFrame = FrameFactory.getFrame(this.insight, "DEFAULT", null);
		} catch (Exception e) {
			throw new IllegalArgumentException("Error occurred trying to create frame of the default type", e);
		}
		// set as default frame if none available
		if(this.insight.getDataMaker() == null) {
			this.insight.setDataMaker(newFrame);
		}
		
		// get results
		SelectQueryStruct qs = SecurityInsightUtils.searchUserInsightsUsage(this.insight.getUser(), pFilters, searchTerm, tagFilters);;
		IDatabaseEngine securityDb = Utility.getDatabase(Constants.SECURITY_DB);
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			IImporter importer = ImportFactory.getImporter(newFrame, qs, wrapper);
			importer.insertData();
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("There was an error in executing the retrieving and loading the insight query statistics", e);
		} finally {
			if(wrapper!=null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return new NounMetadata(newFrame, PixelDataType.FRAME, 
				PixelOperationType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE, PixelOperationType.FRAME_DATA_CHANGE);
//
//		List<NounMetadata> retNouns = new Vector<>();
//		retNouns.add(new NounMetadata(newFrame, PixelDataType.FRAME, PixelOperationType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE, PixelOperationType.FRAME_DATA_CHANGE));
//		
//		String panelId = getPanelId();
//		
//		SelectQueryStruct loadedDataQs = newFrame.getMetaData().getFlatTableQs(true);
//		loadedDataQs.setFrame(newFrame);
//		IRawSelectWrapper loadedDataIterator;
//		try {
//			loadedDataIterator = newFrame.query(loadedDataQs);
//			BasicIteratorTask task = new BasicIteratorTask(loadedDataQs, loadedDataIterator);
//			
//			if(panelId != null) {
//				Map<String, Object> optMap = task.getFormatter().getOptionsMap();
//				TaskOptions tOptions = AudoTaskOptionsHelper.getAutoOptions(qs, panelId, "GRID", optMap);
//				if(tOptions != null) {
//					task.setTaskOptions(tOptions);
//					// if we use task options on a panel
//					// we automatically set the panel view to be visualization
//					InsightUtility.setPanelForVisualization(this.insight, panelId);
//				}
//			}
//			// add to the task store
//			this.insight.getTaskStore().addTask(task);
//			
//			retNouns.add(new NounMetadata(task, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA));
//		} catch (Exception e) {
//			classLogger.error(Constants.STACKTRACE, e);
//			throw new IllegalArgumentException("There was an error in querying the data frame with the loaded insight query statistics", e);
//		}
//		
//		NounMetadata noun = new NounMetadata(retNouns, PixelDataType.VECTOR, PixelOperationType.VECTOR);
//		noun.addAdditionalReturn(getSuccess("Successfully generated new frame with insight usage statistics"));
//		return noun;
	}
	
	/**
	 * Get the tags to set for the insight
	 * @return
	 */
	private List<String> getTags() {
		List<String> tags = new Vector<String>();
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.TAGS.getKey());
		if(grs != null && !grs.isEmpty()) {
			for(int i = 0; i < grs.size(); i++) {
				tags.add(grs.get(i).toString());
			}
		}
		
		return tags;
	}
	
	private String getPanelId() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getNoun(keysToGet[3]);
		if(columnGrs != null && columnGrs.size() > 0) {
			return columnGrs.get(0).toString();
		}
		return null;
	}
}
