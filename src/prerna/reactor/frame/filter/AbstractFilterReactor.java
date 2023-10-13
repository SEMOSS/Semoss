package prerna.reactor.frame.filter;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import prerna.om.InsightPanel;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public abstract class AbstractFilterReactor extends AbstractFrameReactor {

	protected String DYNAMIC_KEY = "dynamic";
	protected String OPTIONS_CACHE_KEY = "optionsCache";
	protected String TASK_REFRESH_KEY = "taskRefresh";

	/**
	 * Get the filters passed into the reactor
	 * 
	 * @return
	 */
	protected GenRowFilters getFilters() {
		// generate a grf with the wanted filters
		GenRowFilters grf = new GenRowFilters();
		int size = this.curRow.size();
		for (int i = 0; i < size; i++) {
			IQueryFilter nextFilter = (IQueryFilter) this.curRow.get(i);
			if (nextFilter != null) {
				grf.addFilters(nextFilter);
			}
		}
		return grf;
	}

	protected InsightPanel getInsightPanel() {
		// passed in directly as panel
		GenRowStruct genericReactorGrs = this.store.getNoun(ReactorKeysEnum.PANEL.getKey());
		if(genericReactorGrs != null && !genericReactorGrs.isEmpty()) {
			NounMetadata noun = genericReactorGrs.getNoun(0);
			PixelDataType nounType = noun.getNounType();
			if(nounType == PixelDataType.PANEL) {
				return (InsightPanel) noun.getValue();
			} else if(nounType == PixelDataType.COLUMN || nounType == PixelDataType.CONST_STRING
					|| nounType == PixelDataType.CONST_INT) {
				String panelId = noun.getValue().toString();
				return this.insight.getInsightPanel(panelId);
			}
		}

		// see if it is in the curRow
		// if it was passed directly in as a variable
		List<NounMetadata> panelNouns = this.curRow.getNounsOfType(PixelDataType.PANEL);
		if(panelNouns != null && !panelNouns.isEmpty()) {
			return (InsightPanel) panelNouns.get(0).getValue();
		}

		// see if string or column passed in
		List<String> strInputs = this.curRow.getAllStrValues();
		if(strInputs != null && !strInputs.isEmpty()) {
			for(String panelId : strInputs) {
				InsightPanel panel = this.insight.getInsightPanel(panelId);
				if(panel != null) {
					return panel;
				}
			}
		}
		List<NounMetadata> strNouns = this.curRow.getNounsOfType(PixelDataType.CONST_INT);
		if(strNouns != null && !strNouns.isEmpty()) {
			return this.insight.getInsightPanel(strNouns.get(0).getValue().toString());
		}

		return null;
	}
	
	/**
	 * Logic to merge 2 gen row filters together to get the final intended result
	 * @param newFiltersToAdd
	 * @param existingFilters
	 */
	protected void mergeFilters(GenRowFilters newFiltersToAdd, GenRowFilters existingFilters) {
		List<IQueryFilter> addFiltersList = newFiltersToAdd.getFilters();
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
				boolean selectAll = false;
				if((selectAll = SimpleQueryFilter.isSelectAll(simpleAdd)) || SimpleQueryFilter.isUnselectAll(simpleAdd)) {
					// we will need to go through the current filters that touch the column
					// and remove them
					String column = simpleAdd.getAllUsedColumns().iterator().next();
					existingFilters.removeColumnFilter(column);
					if(selectAll) {
						// dont need to add to the filter list
						addFiltersToIgnore.add(addFilterIdx);
					}
				}
				
				for (int filterIndex = 0; filterIndex < existingFilters.getFilters().size(); filterIndex++) {
					IQueryFilter currentFilter = existingFilters.getFilters().get(filterIndex);
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
				IQueryFilter removedFilter = existingFilters.removeFilter(indicesToRemove.get(i - 1).intValue());
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
				existingFilters.addFilters(addFiltersList.get(i));
			}
		}
		for(int i = 0; i < compositeFilters.size(); i++) {
			existingFilters.addFilters(compositeFilters.get(i));
		}
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
	protected IQueryFilter processSimpleQueryFilter(
			SimpleQueryFilter simpleAdd, int addFilterIndex, 
			SimpleQueryFilter curFilter, int currentFilterIndex,
			List<Integer> indicesToRemove, 
			List<Integer> addFiltersToIgnore, 
			List<Integer> indicesRemovedByDirectConflict) {

		// are we modifying the same column?
		if(curFilter.equivalentColumnModifcation(simpleAdd, false)) {
			// are they any direct conflicts
			if(IQueryFilter.comparatorsDirectlyConflicting(curFilter.getComparator(), simpleAdd.getComparator())) {
				boolean completeOverlap = curFilter.subtractInstanceFilters(simpleAdd);
				// we removed from the existing filter
				// don't need to add to the overall filters
				if(completeOverlap) {
					addFiltersToIgnore.add(addFilterIndex); 
				}
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
			// if we have two expressions and the comparators are the same
			else if(IQueryFilter.comparatorsCanCombine(curFilter.getComparator(), simpleAdd.getComparator())) {
				// we can ignore this filter
				addFiltersToIgnore.add(addFilterIndex);
				
				// and we can merge the filters
				curFilter.merge(simpleAdd);
			}
		}
		
		return null;
	}

	/**
	 * Should we refresh the tasks after the filter
	 * @return
	 */
	protected boolean isRefreshTasks() {
		Boolean isRefresh = null;
		// passed in
		GenRowStruct grs = this.store.getNoun(TASK_REFRESH_KEY);
		if(grs != null && !grs.isEmpty()) {
			isRefresh = Boolean.parseBoolean(grs.get(0) + "");
		}
		// set in insight state
		if(isRefresh == null) {
			isRefresh = this.insight.getInsightFilterRefresh();
		}
		// default value
		if(isRefresh == null) {
			isRefresh = false;
		}
		
		return isRefresh;
	}
	
}