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
package prerna.reactor.frame;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.ds.py.PandasFrame;
import prerna.ds.r.RDataTable;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.reactor.task.TaskBuilderReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;

public class CollectNewColReactor extends TaskBuilderReactor {

  public CollectNewColReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.QUERY_STRUCT.getKey()};
  }

  public NounMetadata execute() {
    // based on the frame type we will create the formula
    if (!((this.task = getTask()) instanceof BasicIteratorTask)) {
      throw new IllegalArgumentException(
          "Can only add a new column using a basic query on a frame");
    }
    SelectQueryStruct qs = ((BasicIteratorTask) this.task).getQueryStruct();
    ITableDataFrame frame = qs.getFrame();
    TaskBuilderReactor reactor = null;
    if (frame instanceof RDataTable) {
      reactor = new prerna.reactor.frame.r.RCollectNewColReactor();
    } else if (frame instanceof PandasFrame) {
      reactor = new prerna.reactor.frame.py.PyCollectNewColReactor();
    } else if (frame instanceof NativeFrame) {
      reactor = new prerna.reactor.frame.nativeframe.NativeCollectNewColReactor();
    } else {
      throw new IllegalArgumentException("Can only add a new column using an R or Pandas frame");
    }

    // set the task in the store directly instead of double executing
    this.store
        .makeNoun(PixelDataType.TASK.getKey())
        .add(new NounMetadata(this.task, PixelDataType.TASK));
    // pass the references/values
    // return the execution result
    reactor.In();
    reactor.setInsight(this.insight);
    reactor.setNounStore(this.store);
    return reactor.execute();
  }

  @Override
  protected void buildTask() {
    // do nothing
  }
}
