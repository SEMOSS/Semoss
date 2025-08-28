/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.reactor.panel.filter;

import java.util.Set;
import org.apache.logging.log4j.Logger;
import prerna.om.InsightPanel;
import prerna.query.querystruct.filters.BooleanValMetadata;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.reactor.frame.filter.AbstractFilterReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.insight.InsightUtility;

public class SetPanelFilterReactor extends AbstractFilterReactor {

	public SetPanelFilterReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.FILTERS.getKey(),
				TASK_REFRESH_KEY};
	}

	@Override
	public NounMetadata execute() {
		InsightPanel panel = getInsightPanel();
		GenRowFilters panelGrf = panel.getPanelFilters();

		// get the filters
		GenRowFilters newFilters = getFilters();
		if (newFilters.isEmpty()) {
			throw new IllegalArgumentException("No filter found to set to panel");
		}
		// check if we are filtering or actually removing a filter
		if (newFilters.size() == 1) {
			IQueryFilter singleFilter = newFilters.getFilters().get(0);
			if (singleFilter instanceof SimpleQueryFilter) {
				boolean unfilter = ((SimpleQueryFilter) singleFilter).isEmptyFilterValues();
				if (unfilter) {
					QueryColumnSelector cSelector = singleFilter.getAllQueryColumns().get(0);
					boolean isValidFilter = panelGrf.removeColumnFilter(cSelector.getAlias());

					BooleanValMetadata pFilterVal = BooleanValMetadata.getPanelVal();
					pFilterVal.setName(panel.getPanelId());
					pFilterVal.setFilterVal(isValidFilter);
					NounMetadata noun = new NounMetadata(pFilterVal, PixelDataType.BOOLEAN_METADATA,
							PixelOperationType.PANEL_FILTER_CHANGE);
					return noun;
				}
			}
		}

		// we got to this point, apply the filter
		// remove the existing filters for the columns affected
		Set<String> allColsUsed = newFilters.getAllFilteredColumns();
		panelGrf.removeColumnFilters(allColsUsed);
		// add the new filters
		panelGrf.merge(newFilters);

		BooleanValMetadata pFilterVal = BooleanValMetadata.getPanelVal();
		pFilterVal.setName(panel.getPanelId());
		pFilterVal.setFilterVal(true);
		NounMetadata noun = new NounMetadata(pFilterVal, PixelDataType.BOOLEAN_METADATA,
				PixelOperationType.PANEL_FILTER_CHANGE);
		if (isRefreshTasks()) {
			Logger logger = getLogger(SetPanelFilterReactor.class.getName());
			InsightUtility.addInsightPanelRefreshFromPanelFilter(insight, panel, noun, logger);
		}
		return noun;
	}
}
