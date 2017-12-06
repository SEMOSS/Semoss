package prerna.sablecc2.reactor.frame.filter;

import java.util.Collections;
import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.QueryFilter;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;

public class RemoveFrameFilterReactor extends AbstractFilterReactor {

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

		// get the filters that were inputted
		List<QueryFilter> deleteFilters = getDeleteFilters();
		
		// keep track of empty filters to remove the index if we need to
		List<Integer> indicesToRemove = new Vector<Integer>();
		
		//for each qf...
		for (QueryFilter deleteFilter : deleteFilters) {
			// compare the filter with existing filters to only delete the correct one, assuming it does exist
			List<QueryFilter> currentFilters = filters.getFilters();
			for (int filterIndex = 0; filterIndex < currentFilters.size(); filterIndex++) {
				QueryFilter curFilter = currentFilters.get(filterIndex);
				if (QueryFilter.comparatorNotNumeric(curFilter.getComparator()) && 
						QueryFilter.comparatorNotNumeric(deleteFilter.getComparator()) &&
						curFilter.equivalentColumnModifcation(deleteFilter)) 
				{
					// if comparator is not numeric in both
					// and they are equivalent
					curFilter.subtractInstanceFilters(deleteFilter);
					// is the filter now gone?
					if(curFilter.isEmptyFilterValues()) {
						// grab the index
						indicesToRemove.add(filterIndex);
					}
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
	private List<QueryFilter> getDeleteFilters() {
		//retrieve filter input
		GenRowFilters grf = getFilters();
		List<QueryFilter> qfList = grf.getFilters();
		return qfList;
	}
}
