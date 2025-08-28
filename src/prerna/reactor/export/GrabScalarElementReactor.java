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
package prerna.reactor.export;

import java.io.IOException;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.util.Constants;
import prerna.util.Utility;

public class GrabScalarElementReactor extends AbstractReactor {

  /**
   * This class is responsible for collecting the first element from a task and returning it as a
   * noun
   */
  private static final String CLEAN_UP_KEY = "cleanUp";

  private static final Logger classLogger = LogManager.getLogger(GrabScalarElementReactor.class);

  public GrabScalarElementReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.TASK.getKey(), CLEAN_UP_KEY};
  }

  @Override
  public NounMetadata execute() {
    ITask task = getTask();
    if (task == null) {
      throw new IllegalArgumentException("Could not find task to retrieve data from!");
    }
    String stringType = (String) task.getHeaderInfo().get(0).get("dataType");

    PixelDataType nounType = null;
    Object nounValue = null;
    if (task.hasNext()) {
      nounValue = task.next().getValues()[0];
      if (Utility.isNumericType(stringType)) {
        nounType = PixelDataType.CONST_DECIMAL;
      } else {
        nounType = PixelDataType.CONST_STRING;
      }
    } else {
      nounType = PixelDataType.NULL_VALUE;
    }

    boolean cleanUp = cleanUp();
    if (cleanUp) {
      try {
        task.close();
      } catch (IOException e) {
        classLogger.error(Constants.STACKTRACE, e);
      }
      this.insight.getTaskStore().removeTask(task.getId());
    }

    return new NounMetadata(nounValue, nounType);
  }

  // This gets the task to collect from
  private ITask getTask() {
    ITask task;

    List<Object> tasks = curRow.getValuesOfType(PixelDataType.TASK);
    // if we don't have jobs in the curRow, check if it exists in genrow under the key job
    if (tasks == null || tasks.size() == 0) {
      task = (ITask) getNounStore().getNoun(PixelDataType.TASK.getKey()).get(0);
    } else {
      task = (ITask) curRow.getValuesOfType(PixelDataType.TASK).get(0);
    }
    return task;
  }

  private boolean cleanUp() {
    GenRowStruct cleanUpGrs = this.store.getNoun(CLEAN_UP_KEY);
    if (cleanUpGrs != null && !cleanUpGrs.isEmpty()) {
      boolean cleanUp = (boolean) cleanUpGrs.get(0);
      return cleanUp;
    }

    // default is to stop the iterator and clean up
    return true;
  }

  ///////////////////////// KEYS /////////////////////////////////////

  @Override
  protected String getDescriptionForKey(String key) {
    if (key.equals(CLEAN_UP_KEY)) {
      return "Boolean indication (true or false) to clear the task - defaults to true";
    } else {
      return super.getDescriptionForKey(key);
    }
  }
}
