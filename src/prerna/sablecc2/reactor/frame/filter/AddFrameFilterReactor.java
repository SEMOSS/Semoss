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

public class AddFrameFilterReactor extends AbstractFilterReactor {

	public AddFrameFilterReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILTERS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		ITableDataFrame frame = getFrame();

		// get the filters to add
		GenRowFilters grf = getFilters();
		if (grf.isEmpty()) {
			throw new IllegalArgumentException("No filter found to add to frame");
		}

		// get existing filters
		GenRowFilters filters = frame.getFrameFilters();
		List<IQueryFilter> addFiltersList = grf.getFilters();
		int addFiltersSize = addFiltersList.size();
		// keep track of empty filters to remove the index if we need to
		List<Integer> indicesToRemove = new Vector<Integer>();
		List<Integer> addFiltersToIgnore = new Vector<Integer>();
		// for each qf...
		for (int addFilterIdx = 0; addFilterIdx < addFiltersSize; addFilterIdx++) {
			IQueryFilter addFilter = addFiltersList.get(addFilterIdx);
			// only consider simple filters
			if (addFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
				SimpleQueryFilter simpleAdd = (SimpleQueryFilter) addFilter;
				// compare the filter with existing filters to only delete the
				// correct one, assuming it does exist
				List<IQueryFilter> allCurrentFilters = filters.getFilters();
				for (int filterIndex = 0; filterIndex < allCurrentFilters.size(); filterIndex++) {
					IQueryFilter currentFilter = allCurrentFilters.get(filterIndex);
					// only consider simple filters
					if (currentFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
						SimpleQueryFilter curFilter = (SimpleQueryFilter) currentFilter;

						// are we modifying the same column?
						if(curFilter.equivalentColumnModifcation(simpleAdd, false)) {
							// are they any direct conflicts
							if(IQueryFilter.comparatorsDirectlyConflicting(curFilter.getComparator(), simpleAdd.getComparator())) {
								curFilter.subtractInstanceFilters(simpleAdd);
								// is the filter now gone?
								if (curFilter.isEmptyFilterValues()) {
									// grab the index
									indicesToRemove.add(filterIndex);
								}
								// if we are adding an equal
								// and its conflicting from a previous
								// then we just remove, do not need to add
								if(simpleAdd.getComparator().equals("==")) {
									addFiltersToIgnore.add(addFilterIdx);
								}
							}
							// or are there any indirect conflicts
							else if(IQueryFilter.comparatorsRegexConflicting(curFilter.getComparator(), simpleAdd.getComparator())) {
								if(curFilter.isOverlappingRegexValues(simpleAdd, true)) {
									// grab the index
									// will remove
									indicesToRemove.add(filterIndex);
								}
							}
							// or is the new one overshadowing the existing and we need to remove
							else if(IQueryFilter.newComparatorOvershadowsExisting(curFilter.getComparator(), simpleAdd.getComparator())) {
								if(curFilter.isOverlappingRegexValues(simpleAdd, false)) {
									// grab the index
									// will remove
									indicesToRemove.add(filterIndex);
								}
							}
						}
					}
				}
			}
		}

		// do we have things to remove?
		if (!indicesToRemove.isEmpty()) {
			Collections.sort(indicesToRemove);
			// first we need to delete the highest index in order to not change
			// the index of what we are deleting
			for (int i = indicesToRemove.size(); i > 0; i--) {
				// remove the filter at the index specified by the index list
				filters.removeFilter(indicesToRemove.get(i - 1).intValue());
			}
		}

		// now add the new filters
		for(int i = 0; i < addFiltersSize; i++) {
			if(!addFiltersToIgnore.contains(new Integer(i))) {
				frame.addFilter(addFiltersList.get(i));
			}
		}

		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.FRAME_FILTER);
		return noun;
	}
}
