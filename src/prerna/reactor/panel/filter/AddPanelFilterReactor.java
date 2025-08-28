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

import org.apache.logging.log4j.Logger;
import prerna.om.InsightPanel;
import prerna.query.querystruct.filters.BooleanValMetadata;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.reactor.frame.filter.AbstractFilterReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.insight.InsightUtility;

public class AddPanelFilterReactor extends AbstractFilterReactor {

  public AddPanelFilterReactor() {
    this.keysToGet =
        new String[] {
          ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.FILTERS.getKey(), TASK_REFRESH_KEY
        };
  }

  @Override
  public NounMetadata execute() {
    InsightPanel panel = getInsightPanel();
    if (panel == null) {
      throw new NullPointerException("Cannot find the input panel for add panel filter");
    }

    // get the filters to add
    GenRowFilters newFiltersToAdd = getFilters();
    if (newFiltersToAdd.isEmpty()) {
      throw new IllegalArgumentException("No filter found to add to panel");
    }

    // get existing filters
    GenRowFilters existingFilters = panel.getPanelFilters();

    // add the new filters by merging into the existing state
    mergeFilters(newFiltersToAdd, existingFilters);

    BooleanValMetadata pFilterVal = BooleanValMetadata.getPanelVal();
    pFilterVal.setName(panel.getPanelId());
    pFilterVal.setFilterVal(true);
    NounMetadata noun =
        new NounMetadata(
            pFilterVal, PixelDataType.BOOLEAN_METADATA, PixelOperationType.PANEL_FILTER_CHANGE);
    if (isRefreshTasks()) {
      Logger logger = getLogger(AddPanelFilterReactor.class.getName());
      InsightUtility.addInsightPanelRefreshFromPanelFilter(insight, panel, noun, logger);
    }
    return noun;
  }
}
