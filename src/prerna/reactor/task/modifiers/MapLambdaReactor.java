/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.task.modifiers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import prerna.reactor.task.TaskBuilderReactor;
import prerna.reactor.task.lambda.map.IMapLambda;
import prerna.reactor.task.lambda.map.function.MapLambdaFactory;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;

public class MapLambdaReactor extends TaskBuilderReactor {

  /**
   * Allow you to modidy an existing column(s) or add new columns Will not allow you to add new rows
   */
  public MapLambdaReactor() {
    this.keysToGet =
        new String[] {
          ReactorKeysEnum.LAMBDA.getKey(),
          ReactorKeysEnum.COLUMNS.getKey(),
          ReactorKeysEnum.PARAM_KEY.getKey()
        };
  }

  @Override
  protected void buildTask() {
    String lambda = getLambda();
    List<String> columns = getColumns();
    Map params = getMap();

    IMapLambda mapLambda = MapLambdaFactory.getLambda(lambda);
    if (mapLambda == null) {
      throw new IllegalArgumentException("Unknown transformation type = " + lambda);
    }
    mapLambda.setUser(this.insight.getUser());
    mapLambda.setParams(params);
    mapLambda.init(this.task.getHeaderInfo(), columns);

    // create a new task and add to stores
    prerna.reactor.task.lambda.map.MapLambdaTask newTask =
        new prerna.reactor.task.lambda.map.MapLambdaTask();
    newTask.setInnerTask(this.task);
    newTask.setLambda(mapLambda);
    newTask.setHeaderInfo(mapLambda.getModifiedHeaderInfo());

    this.task = newTask;
    this.insight.getTaskStore().addTask(this.task);
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////

  // inputs

  private String getLambda() {
    GenRowStruct colGrs = this.store.getNoun(keysToGet[0]);
    if (colGrs != null && !colGrs.isEmpty()) {
      return colGrs.get(0).toString();
    }

    throw new IllegalArgumentException("No transformation type was entered");
  }

  private List<String> getColumns() {
    GenRowStruct colGrs = this.store.getNoun(keysToGet[1]);
    if (colGrs != null && !colGrs.isEmpty()) {
      int size = colGrs.size();
      List<String> columns = new ArrayList<String>();
      for (int i = 0; i < size; i++) {
        columns.add(colGrs.get(i).toString());
      }
      return columns;
    }

    return null;
  }

  private Map getMap() {
    GenRowStruct colGrs = this.store.getNoun(keysToGet[2]);
    if (colGrs != null && !colGrs.isEmpty()) {
      return (Map) colGrs.get(0);
    }

    return null;
  }

  public String getName() {
    return "MapLambda";
  }
}
