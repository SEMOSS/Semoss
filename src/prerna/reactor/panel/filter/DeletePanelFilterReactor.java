package prerna.reactor.panel.filter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.Logger;

import prerna.om.InsightPanel;
import prerna.query.querystruct.filters.BooleanValMetadata;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.reactor.frame.filter.AbstractFilterReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.insight.InsightUtility;

public class DeletePanelFilterReactor extends AbstractFilterReactor {

	public DeletePanelFilterReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.INDEX.getKey(), TASK_REFRESH_KEY };
	}

	@Override
	public NounMetadata execute() {
		InsightPanel panel = getInsightPanel();
		if(panel == null) {
			throw new NullPointerException("Cannot find the input panel for delete panel filter");
		}
		// get the filters to add
		GenRowFilters filters = panel.getPanelFilters();
		if (filters.isEmpty()) {
			throw new IllegalArgumentException("No panel filters exist to delete");
		}

		// first try to delete based on index
		List<Integer> indexList = getOrderedIndexes();
		// first we need to delete the highest index in order to not change the index of what we are deleting
		for (int i = indexList.size(); i > 0; i--) {
			// remove the filter at the index specified by the index list
			filters.removeFilter(indexList.get(i - 1).intValue());
		}

		BooleanValMetadata pFilterVal = BooleanValMetadata.getPanelVal();
		pFilterVal.setName(panel.getPanelId());
		pFilterVal.setFilterVal(true);
		NounMetadata noun = new NounMetadata(pFilterVal, PixelDataType.BOOLEAN_METADATA, PixelOperationType.PANEL_FILTER_CHANGE);
		if(isRefreshTasks()) {
			Logger logger = getLogger(DeletePanelFilterReactor.class.getName());
			InsightUtility.addInsightPanelRefreshFromPanelFilter(insight, panel, noun, logger);
		}
		return noun;
	}

	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////

	/**
	 * return an ordered list of the filter indexes that we want to delete
	 * 
	 * @return
	 */
	private List<Integer> getOrderedIndexes() {
		List<Integer> indexList = new ArrayList<Integer>();
		// this grs will contain all indexes, each as a separate noun
		GenRowStruct formatGRS = this.store.getNoun(keysToGet[1]);
		if (formatGRS != null && formatGRS.size() > 0) {
			for (int i = 0; i < formatGRS.size(); i++) {
				indexList.add(((Number) formatGRS.getNoun(i).getValue()).intValue());
			}
		} else {
			List<Object> numericInputs = this.curRow.getAllNumericColumns();
			for (int i = 0; i < numericInputs.size(); i++) {
				indexList.add(((Number) numericInputs.get(i)).intValue());
			}
		}
		// sort so that we can later remove based on the highest first
		if (indexList.isEmpty()) {
			throw new IllegalArgumentException("No indices are defined");
		}
		Collections.sort(indexList);
		return indexList;
	}

}
