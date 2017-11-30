package prerna.sablecc2.reactor.frame.filter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import prerna.algorithm.api.ITableDataFrame;
import prerna.query.querystruct.GenRowFilters;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.QueryFilter;

public class RemoveFrameFilterReactor extends AbstractFilterReactor {

	// this constant used when we specify the indexes for the filters that we
	// want to remove
	private static final String INDEX = "index";

	@Override
	public NounMetadata execute() {

		ITableDataFrame frame = (ITableDataFrame) this.insight.getDataMaker();
		GenRowFilters filters = new GenRowFilters();
		// get the existing filters
		if (frame != null) {
			filters = frame.getFrameFilters();
		}

		// first try to delete based on index
		List indexList = getOrderedIndexes();
		// if no indexes are entered, the index list will be null
		if (indexList != null) {
			// if we are given an index to delete
			// first we need to delete the highest index in order to not change
			// the index of what we are deleting
			for (int i = indexList.size() - 1; i >= 0; i--) {
				// remove the filter at the index specified by the index list
				filters.removeFilter((int) indexList.get(i));
			}
		} else {
			// if not given an index, get the filters that were inputted
			// get the filter that was inputted
			QueryFilter deleteFilter = getDeleteFilter();

			// compare the filter with existing filters to only delete the
			// correct one
			// assuming it does exist
			for (int i = 0; i < filters.size(); i++) {
				if (filters.getFilters().get(i).equals(deleteFilter)) {
					filters.removeFilter(i);
				}
			}
		}
		return new NounMetadata(filters, PixelDataType.FILTER, PixelOperationType.FRAME_FILTER);
	}

	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////

	/**
	 * 
	 * @return the filter to be deleted
	 */
	private QueryFilter getDeleteFilter() {
		// retrieve filter input
		GenRowFilters grf = getFilters();
		QueryFilter qf = grf.getFilters().get(0);
		return qf;
	}

	/**
	 * 
	 * @return an ordered list of the filter indexes that we want to delete
	 */
	private List getOrderedIndexes() {
		List indexList = new ArrayList<>();
		// this grs will contain all indexes, each as a separate noun
		GenRowStruct formatGRS = this.store.getNoun(INDEX);
		if (formatGRS != null) {
			if (formatGRS.size() > 0) {
				for (int i = 0; i < formatGRS.size(); i++) {
					indexList.add((int) formatGRS.getNoun(i).getValue());
				}
				// sort so that we can later remove based on the highest first
				Collections.sort(indexList);
				return indexList;
			}
		}
		return null;
	}

}
