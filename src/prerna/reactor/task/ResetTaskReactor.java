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
package prerna.reactor.task;

import java.util.List;
import java.util.Vector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.util.Constants;

public class ResetTaskReactor extends AbstractReactor {

  private static final Logger classLogger = LogManager.getLogger(ResetTaskReactor.class);

  public ResetTaskReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.TASK_ID.getKey()};
  }

  @Override
  public NounMetadata execute() {
    // this just returns the task id
    ITask task = getTask();
    try {
      task.reset();
    } catch (Exception e) {
      classLogger.error(Constants.STACKTRACE, e);
      throw new SemossPixelException(e.getMessage());
    }
    return new NounMetadata(task, PixelDataType.TASK, PixelOperationType.TASK);
  }

  protected ITask getTask() {
    ITask task = null;

    GenRowStruct grsTasks = this.store.getNoun(PixelDataType.TASK.getKey());
    // if we don't have jobs in the curRow, check if it exists in genrow under the key job
    if (grsTasks != null && !grsTasks.isEmpty()) {
      task = (ITask) grsTasks.get(0);
    } else {
      List<Object> tasks = this.curRow.getValuesOfType(PixelDataType.TASK);
      if (tasks != null && !tasks.isEmpty()) {
        task = (ITask) tasks.get(0);
      }
    }

    // maybe the user passed in a string
    if (task == null) {
      String taskId = this.curRow.get(0).toString();
      task = this.insight.getTaskStore().getTask(taskId);
    }

    return task;
  }

  @Override
  public List<NounMetadata> getOutputs() {
    List<NounMetadata> outputs = super.getOutputs();
    if (outputs != null) {
      return outputs;
    }

    outputs = new Vector<NounMetadata>();
    // since output is lazy
    // just return the execute
    outputs.add((NounMetadata) execute());
    return outputs;
  }
}
