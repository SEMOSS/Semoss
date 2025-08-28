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
package prerna.util.usertracking;

import java.util.List;
import java.util.Map;
import prerna.algorithm.api.ITableDataFrame;
import prerna.om.Insight;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.task.options.TaskOptions;

public class NullUserTracker implements IUserTracker {

  /** Constructor is protected so it can only be created by the builder */
  protected NullUserTracker() {}

  /*
   * This class i used when we do not want to do any tracking
   * So no methods will be implemented
   */

  @Override
  public void trackVizWidget(Insight in, TaskOptions taskOptions, SelectQueryStruct qs) {
    // TODO Auto-generated method stub

  }

  @Override
  public void trackAnalyticsWidget(
      Insight in, ITableDataFrame frame, String routineName, Map<String, List<String>> keyValue) {
    // TODO Auto-generated method stub

  }

  @Override
  public void trackPixelExecution(Insight in, String pixel, boolean meta) {
    // TODO Auto-generated method stub

  }

  @Override
  public void trackInsightExecution(Insight in) {
    // TODO Auto-generated method stub

  }

  @Override
  public void trackDataImport(Insight in, SelectQueryStruct qs) {
    // TODO Auto-generated method stub

  }

  @Override
  public void trackQueryData(Insight in, SelectQueryStruct qs) {
    // TODO Auto-generated method stub

  }

  @Override
  public void trackUserWidgetMods(List<Object[]> rows) {
    // TODO Auto-generated method stub

  }

  @Override
  public void trackError(
      Insight in,
      String pixel,
      String reactorName,
      String parentReactorName,
      boolean meta,
      Exception ex) {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean isActive() {
    return false;
  }
}
