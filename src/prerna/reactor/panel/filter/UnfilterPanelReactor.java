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

import java.util.List;
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

public class UnfilterPanelReactor extends AbstractFilterReactor {

  public UnfilterPanelReactor() {
    this.keysToGet =
        new String[] {
          ReactorKeysEnum.PANEL.getKey(), ReactorKeysEnum.COLUMNS.getKey(), TASK_REFRESH_KEY
        };
  }

  @Override
  public NounMetadata execute() {
    InsightPanel panel = getInsightPanel();

    List<Object> colsToUnfilter = null;
    if (this.curRow.size() > 0) {
      colsToUnfilter = this.curRow.getValuesOfType(PixelDataType.CONST_STRING);
    }

    boolean hasFilters = false;
    if (colsToUnfilter == null || colsToUnfilter.isEmpty()) {
      GenRowFilters grf = panel.getPanelFilters();
      if (!grf.isEmpty()) {
        grf.removeAllFilters();
        hasFilters = true;
      }
    } else {
      GenRowFilters grf = panel.getPanelFilters();
      for (Object col : colsToUnfilter) {
        String colName = col + "";
        if (grf.hasFilter(colName)) {
          grf.removeColumnFilter(colName);
          hasFilters = true;
        }
      }
    }

    BooleanValMetadata pFilterVal = BooleanValMetadata.getPanelVal();
    pFilterVal.setName(panel.getPanelId());
    pFilterVal.setFilterVal(hasFilters);
    NounMetadata noun =
        new NounMetadata(
            pFilterVal, PixelDataType.BOOLEAN_METADATA, PixelOperationType.PANEL_FILTER_CHANGE);
    if (hasFilters && isRefreshTasks()) {
      Logger logger = getLogger(UnfilterPanelReactor.class.getName());
      InsightUtility.addInsightPanelRefreshFromPanelFilter(insight, panel, noun, logger);
    }
    return noun;
  }
}
