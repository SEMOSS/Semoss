package prerna.sablecc2.reactor.frame.filter;

import java.util.Collections;
import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;

public class ReplaceFrameFilterReactor extends AbstractFilterReactor {

	@Override
	public NounMetadata execute() {
		ITableDataFrame frame = (ITableDataFrame) this.insight.getDataMaker();
		GenRowFilters filters = null;
		
		// get the existing filters
		if (frame != null) {
			filters = frame.getFrameFilters();
		} else {
			throw new IllegalArgumentException("No frame is defined in the insight to remove the filters from");
		}

		// get the filters to replace
		List<SimpleQueryFilter> replaceFilters = getReplaceFilters();
		
		// keep track of filters indices we are going to replace
		List<Integer> indicesToRemove = new Vector<Integer>();
		
		//for each qf...
		for (SimpleQueryFilter replaceFilter : replaceFilters) {
			// compare the filter with existing filters to only delete the correct one, assuming it does exist
			List<SimpleQueryFilter> currentFilters = filters.getFilters();
			for (int filterIndex = 0; filterIndex < currentFilters.size(); filterIndex++) {
				SimpleQueryFilter curFilter = currentFilters.get(filterIndex);
				if (curFilter.equivalentColumnModifcation(replaceFilter)) {
					// we have a match
					// we will remove this instance
					indicesToRemove.add(filterIndex);
				}
			}
		}
		
		// do we have things to remove?
		if(!indicesToRemove.isEmpty()) {
			Collections.sort(indicesToRemove);
			// first we need to delete the highest index in order to not change the index of what we are deleting
			for(int i = indicesToRemove.size(); i > 0 ; i--) {
				// remove the filter at the index specified by the index list
				filters.removeFilter(indicesToRemove.get(i - 1).intValue());
			}
		}
		
		// now we add the new filters
		for (SimpleQueryFilter replaceFilter : replaceFilters) {
			filters.addFilters(replaceFilter);
		}

		return new NounMetadata(filters, PixelDataType.FILTER, PixelOperationType.FRAME_FILTER);
	}

	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////

	/**
	 * get the filters to be deleted
	 * @return
	 */
	private List<SimpleQueryFilter> getReplaceFilters() {
		//retrieve filter input
		GenRowFilters grf = getFilters();
		List<SimpleQueryFilter> qfList = grf.getFilters();
		return qfList;
	}
}
