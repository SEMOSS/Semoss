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
import prerna.reactor.task.TaskBuilderReactor;
import prerna.reactor.task.lambda.map.MapLambdaTask;
import prerna.reactor.task.lambda.map.function.ToUrlTypeLambda;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;

public class ToUrlTypeReactor extends TaskBuilderReactor {

  public ToUrlTypeReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.COLUMNS.getKey()};
  }

  @Override
  protected void buildTask() {
    // get the columns
    List<String> cols = getColumns();

    // create a new task and add to stores
    MapLambdaTask newTask = new MapLambdaTask();
    newTask.setInnerTask(this.task);
    ToUrlTypeLambda transformation = new ToUrlTypeLambda();
    transformation.init(this.task.getHeaderInfo(), cols);
    newTask.setLambda(transformation);
    newTask.setHeaderInfo(transformation.getModifiedHeaderInfo());
    this.task = newTask;
    this.insight.getTaskStore().addTask(this.task);
  }

  private List<String> getColumns() {
    GenRowStruct colGrs = this.store.getNoun(keysToGet[0]);
    if (colGrs != null && !colGrs.isEmpty()) {
      int size = colGrs.size();
      List<String> columns = new ArrayList<String>();
      for (int i = 0; i < size; i++) {
        columns.add(colGrs.get(i).toString());
      }
      return columns;
    }

    List<String> columns = new ArrayList<String>();
    int size = this.curRow.size();
    for (int i = 0; i < size; i++) {
      columns.add(this.curRow.get(i).toString());
    }
    return columns;
  }

  public String getName() {
    return "ToUrlType";
  }
}
