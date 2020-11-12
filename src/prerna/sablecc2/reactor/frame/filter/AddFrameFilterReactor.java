package prerna.sablecc2.reactor.frame.filter;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.query.querystruct.filters.FilterBooleanVal;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
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
		List<IQueryFilter> compositeFilters = new Vector<IQueryFilter>();
		// keep track of empty filters to remove the index if we need to
		List<Integer> indicesToRemove = new Vector<Integer>();
		List<Integer> addFiltersToIgnore = new Vector<Integer>();
		List<Integer> indicesRemovedByDirectConflict = new Vector<Integer>();
		// for each qf...
		for (int addFilterIdx = 0; addFilterIdx < addFiltersList.size(); addFilterIdx++) {
			IQueryFilter addFilter = addFiltersList.get(addFilterIdx);
			// only consider simple filters
			if (addFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
				SimpleQueryFilter simpleAdd = (SimpleQueryFilter) addFilter;
				// account for the select all statements that may happen
				if(SimpleQueryFilter.isSelectAll(simpleAdd) || SimpleQueryFilter.isUnselectAll(simpleAdd)) {
					// we will need to go through the current filters that touch the column
					// and remove them
					String column = simpleAdd.getAllUsedColumns().iterator().next();
					filters.removeColumnFilter(column);
				}
				
				for (int filterIndex = 0; filterIndex < filters.getFilters().size(); filterIndex++) {
					IQueryFilter currentFilter = filters.getFilters().get(filterIndex);
					// only consider simple filters
					if (currentFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
						SimpleQueryFilter curFilter = (SimpleQueryFilter) currentFilter;
						
						IQueryFilter requireNewFilter = processSimpleQueryFilter(simpleAdd, addFilterIdx, curFilter, filterIndex,
								indicesToRemove, addFiltersToIgnore, indicesRemovedByDirectConflict);
						if(requireNewFilter != null) {
							compositeFilters.add(requireNewFilter);
						}
					} else if(currentFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.OR) {
						OrQueryFilter curFilter = (OrQueryFilter) currentFilter;
						// is this only affecting this one column?
						if(curFilter.getAllUsedColumns().containsAll(simpleAdd.getAllUsedColumns())) {
							// if we are to add this filter
							// we could get
							// x = a AND (x = b or x ?like c)
							for(IQueryFilter orInnerFilter : curFilter.getFilterList()) {
								// assuming you only have simple queries in here...
								if(orInnerFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
									SimpleQueryFilter simpleOrInnerFilter = (SimpleQueryFilter) orInnerFilter;
									if(simpleOrInnerFilter.getComparator().equals(simpleAdd.getComparator())) {
										// combine simpleAdd to this query and ignore the index
										simpleOrInnerFilter.addInstanceFilters(simpleAdd);
										addFiltersToIgnore.add(addFilterIdx);
									}
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
			GenRowFilters removedFilters = new GenRowFilters();
			for (int i = indicesToRemove.size(); i > 0; i--) {
				// remove the filter at the index specified by the index list
				IQueryFilter removedFilter = filters.removeFilter(indicesToRemove.get(i - 1).intValue());
				// this should only be a simple query filter
				if(indicesRemovedByDirectConflict.contains(new Integer(i - 1))) {
					removedFilters.addFilters(removedFilter);
				}
			}

			Set<String> columnsRemoved = removedFilters.getAllFilteredColumns();
			for(String colRem : columnsRemoved) {
				addFiltersList.add(SimpleQueryFilter.makeColToValFilter(colRem, "?nlike", ""));
			}
		}

		// now add the new filters
		for(int i = 0; i < addFiltersList.size(); i++) {
			if(!addFiltersToIgnore.contains(new Integer(i))) {
				frame.addFilter(addFiltersList.get(i));
			}
		}
		for(int i = 0; i < compositeFilters.size(); i++) {
			frame.addFilter(compositeFilters.get(i));
		}

		FilterBooleanVal fFilterVal = FilterBooleanVal.getFrameFilter();
		fFilterVal.setName(frame.getName());
		fFilterVal.setFilterVal(true);
		NounMetadata noun = new NounMetadata(fFilterVal, PixelDataType.FILTER_BOOLEAN_VAL, PixelOperationType.FRAME_FILTER_CHANGE);
		return noun;
	}

	/**
	 * 
	 * @param simpleAdd
	 * @param addFilterIndex
	 * @param curFilter
	 * @param currentFilterIndex
	 * @param indicesToRemove
	 * @param addFiltersToIgnore
	 * @param indicesRemovedByDirectConflict
	 */
	private IQueryFilter processSimpleQueryFilter(
			SimpleQueryFilter simpleAdd, int addFilterIndex, 
			SimpleQueryFilter curFilter, int currentFilterIndex,
			List<Integer> indicesToRemove, 
			List<Integer> addFiltersToIgnore, 
			List<Integer> indicesRemovedByDirectConflict) {

		// are we modifying the same column?
		if(curFilter.equivalentColumnModifcation(simpleAdd, false)) {
			// are they any direct conflicts
			if(IQueryFilter.comparatorsDirectlyConflicting(curFilter.getComparator(), simpleAdd.getComparator())) {
				curFilter.subtractInstanceFilters(simpleAdd);
				// we removed from the existing filter
				// don't need to add to the overall filters
				addFiltersToIgnore.add(addFilterIndex);
				// but is the entire filter gone now?
				if (curFilter.isEmptyFilterValues()) {
					// grab the index
					indicesToRemove.add(currentFilterIndex);
					// if we are adding an equal
					// and its conflicting from a previous
					// then we just remove, do not need to add
					if(simpleAdd.getComparator().equals("!=")) {
						// if i just removed everything from an == filter
						// then nothing is selected
						indicesRemovedByDirectConflict.add(currentFilterIndex);
					}
				}
			}
			// or are there any indirect conflicts
			else if(IQueryFilter.comparatorsRegexConflicting(curFilter.getComparator(), simpleAdd.getComparator())) {
				if(curFilter.isOverlappingRegexValues(simpleAdd, true)) {
					// grab the index
					// will remove
					indicesToRemove.add(currentFilterIndex);
				}
			}
			// or is the new one overshadowing the existing and we need to remove
			else if(IQueryFilter.newComparatorOvershadowsExisting(curFilter.getComparator(), simpleAdd.getComparator())) {
				if(curFilter.isOverlappingRegexValues(simpleAdd, false)) {
					// grab the index
					// will remove
					indicesToRemove.add(currentFilterIndex);
				} else if(curFilter.getComparator().equals("==")) {
					// remove this filter
					// do not add the simple filter
					// because we will add the OR filter
					indicesToRemove.add(currentFilterIndex);
					addFiltersToIgnore.add(addFilterIndex);

					// make the OR and return
					OrQueryFilter orFilter = new OrQueryFilter();
					orFilter.addFilter(curFilter);
					orFilter.addFilter(simpleAdd);
					return orFilter;
				}
			}
			// if we have a ?like and ==, we need an OR
			else if(IQueryFilter.comparatorsRequireOrStatement(curFilter.getComparator(), simpleAdd.getComparator())) {
				// remove this filter
				// do not add the simple filter
				// because we will add the OR filter
				indicesToRemove.add(currentFilterIndex);
				addFiltersToIgnore.add(addFilterIndex);

				// make the OR and return
				OrQueryFilter orFilter = new OrQueryFilter();
				orFilter.addFilter(curFilter);
				orFilter.addFilter(simpleAdd);
				return orFilter;
			}
		}
		
		return null;
	}

}
