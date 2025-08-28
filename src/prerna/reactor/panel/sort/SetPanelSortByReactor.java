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
package prerna.reactor.panel.sort;

import java.util.List;
import java.util.Vector;
import prerna.om.InsightPanel;
import prerna.query.querystruct.filters.BooleanValMetadata;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetPanelSortByReactor extends AbstractPanelSortReactor {

  public SetPanelSortByReactor() {
    this.keysToGet =
        new String[] {
          ReactorKeysEnum.PANEL.getKey(),
          ReactorKeysEnum.COLUMNS.getKey(),
          ReactorKeysEnum.SORT.getKey()
        };
  }

  @Override
  public NounMetadata execute() {
    InsightPanel panel = getInsightPanel();

    // get the sort information
    IQuerySort sortBy = getCustomSortBy();
    List<IQuerySort> sortByList = new Vector<>();
    sortByList.add(sortBy);
    panel.setPanelOrderBys(sortByList);

    BooleanValMetadata pSortVal = BooleanValMetadata.getPanelVal();
    pSortVal.setName(panel.getPanelId());
    pSortVal.setFilterVal(true);
    NounMetadata noun =
        new NounMetadata(pSortVal, PixelDataType.BOOLEAN_METADATA, PixelOperationType.PANEL_SORT);
    return noun;
  }
}
