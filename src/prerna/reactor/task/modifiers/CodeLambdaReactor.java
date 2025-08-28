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

import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.reactor.task.lambda.map.GenericMapLambda;
import prerna.reactor.task.lambda.map.MapLambdaTask;
import prerna.util.Constants;

public class CodeLambdaReactor extends AbstractLambdaTaskReactor {

  private static final Logger classLogger = LogManager.getLogger(CodeLambdaReactor.class);

  /** Abstract lambda class is responsible for getting data from the noun store / prop store */
  public CodeLambdaReactor() {
    this.keysToGet = new String[] {"CODE", IMPORTS_KEY};
  }

  @Override
  protected void buildTask() {
    String code = getCode();
    List<String> imports = getImports();

    // create the transformation
    // TODO: do this by reflection?
    GenericMapLambda lambda = new GenericMapLambda();
    try {
      lambda.init(code, imports);
      lambda.setUser(this.insight.getUser());
    } catch (InstantiationException | IllegalAccessException e) {
      classLogger.error(Constants.STACKTRACE, e);
      throw new IllegalArgumentException("Error with creating generic lambda!");
    }

    // create a new task and add to stores
    MapLambdaTask newTask = new MapLambdaTask();
    newTask.setInnerTask(this.task);
    newTask.setLambda(lambda);
    this.task = newTask;
    this.insight.getTaskStore().addTask(this.task);
  }
}
