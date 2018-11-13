package prerna.sablecc2.reactor.frame.filter;

import java.util.Collections;
import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RemoveFrameFilterReactor extends AbstractFilterReactor {

	public RemoveFrameFilterReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILTERS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		ITableDataFrame frame = getFrame();
		GenRowFilters filters = null;

		// get the existing filters
		if (frame != null) {
			filters = frame.getFrameFilters();
		} else {
			throw new IllegalArgumentException("No frame is defined in the insight to remove the filters from");
		}

		// get the filters that were inputted
		List<IQueryFilter> allDeleteFilters = getDeleteFilters();

		// keep track of empty filters to remove the index if we need to
		List<Integer> indicesToRemove = new Vector<Integer>();

		// for each qf...
		for (IQueryFilter deleteFilters : allDeleteFilters) {
			// only consider simple filters
			if (deleteFilters.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
				SimpleQueryFilter deleteFilter = (SimpleQueryFilter) deleteFilters;
				// compare the filter with existing filters to only delete the correct one, assuming it does exist
				List<IQueryFilter> allCurrentFilters = filters.getFilters();
				for (int filterIndex = 0; filterIndex < allCurrentFilters.size(); filterIndex++) {
					IQueryFilter currentFilter = allCurrentFilters.get(filterIndex);
					// only consider simple filters
					if (currentFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
						SimpleQueryFilter curFilter = (SimpleQueryFilter) currentFilter;
						if (IQueryFilter.comparatorNotNumeric(curFilter.getComparator())
								&& IQueryFilter.comparatorNotNumeric(deleteFilter.getComparator())
								&& curFilter.equivalentColumnModifcation(deleteFilter)) {
							// if comparator is not numeric in both
							// and they are equivalent
							curFilter.subtractInstanceFilters(deleteFilter);
							// is the filter now gone?
							if (curFilter.isEmptyFilterValues()) {
								// grab the index
								indicesToRemove.add(filterIndex);
							}
						}
					}
				}
			}
		}

		// do we have things to remove?
		if (!indicesToRemove.isEmpty()) {
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
	 * 
	 * @return
	 */
	private List<IQueryFilter> getDeleteFilters() {
		// retrieve filter input
		GenRowFilters grf = getFilters();
		List<IQueryFilter> qfList = grf.getFilters();
		return qfList;
	}
}
