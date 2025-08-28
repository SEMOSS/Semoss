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
package prerna.reactor.frame.filter;

import java.util.List;
import org.apache.logging.log4j.Logger;
import prerna.algorithm.api.ITableDataFrame;
import prerna.query.querystruct.filters.BooleanValMetadata;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.insight.InsightUtility;

public class UnfilterFrameReactor extends AbstractFilterReactor {

  public UnfilterFrameReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.COLUMNS.getKey(), TASK_REFRESH_KEY};
  }

  @Override
  public NounMetadata execute() {
    ITableDataFrame frame = getFrame();
    boolean foundFilter = false;
    List<Object> colsToUnfilter = null;
    if (this.curRow.size() > 0) {
      colsToUnfilter = this.curRow.getValuesOfType(PixelDataType.COLUMN);
    }

    if (colsToUnfilter == null || colsToUnfilter.isEmpty()) {
      foundFilter = frame.unfilter();
    } else {
      for (Object col : colsToUnfilter) {
        QueryColumnSelector cSelector = new QueryColumnSelector(col + "");
        boolean isValidFilter = frame.unfilter(cSelector.getAlias());
        if (isValidFilter) {
          foundFilter = true;
        }
      }
    }

    // clear panel temp filter model state
    InsightUtility.clearPanelTempFilterModel(this.insight, frame);

    BooleanValMetadata fFilterVal = BooleanValMetadata.getFrameVal();
    fFilterVal.setName(frame.getOriginalName());
    fFilterVal.setFilterVal(foundFilter);
    NounMetadata noun =
        new NounMetadata(
            fFilterVal, PixelDataType.BOOLEAN_METADATA, PixelOperationType.FRAME_FILTER_CHANGE);
    if (foundFilter && isRefreshTasks()) {
      Logger logger = getLogger(SetFrameFilterReactor.class.getName());
      InsightUtility.addInsightPanelRefreshFromFrameFilter(this.insight, frame, noun, logger);
    }
    return noun;
  }
}
