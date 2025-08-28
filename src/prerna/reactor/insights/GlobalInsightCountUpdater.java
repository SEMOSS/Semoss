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
package prerna.reactor.insights;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.util.Constants;

public class GlobalInsightCountUpdater {

  private static final Logger classLogger = LogManager.getLogger(GlobalInsightCountUpdater.class);

  /*
   * Creating a class to manage updating the insight count
   * This is necessary since we will get version conflicts
   * if you run 2 insights at the same time
   */

  private static GlobalInsightCountUpdater singleton;

  private BlockingQueue<String[]> queue;
  private CountUpdater updater;

  private GlobalInsightCountUpdater() {
    queue = new ArrayBlockingQueue<String[]>(2_000);
    updater = new CountUpdater(queue);

    new Thread(updater).start();
  }

  public static GlobalInsightCountUpdater getInstance() {
    if (singleton == null) {
      singleton = new GlobalInsightCountUpdater();
    }
    return singleton;
  }

  public void addToQueue(String engineId, String id) {
    try {
      queue.add(new String[] {engineId, id});
    } catch (Exception e) {
      classLogger.error(Constants.STACKTRACE, e);
    }
  }
}

class CountUpdater implements Runnable {

  private static final Logger classLogger = LogManager.getLogger(CountUpdater.class);

  protected BlockingQueue<String[]> queue = null;

  public CountUpdater(BlockingQueue<String[]> queue) {
    this.queue = queue;
  }

  public void run() {
    try {
      String[] update = null;
      while ((update = queue.take()) != null) {
        SecurityInsightUtils.updateExecutionCount(update[0], update[1]);
      }
    } catch (InterruptedException e) {
      classLogger.error(Constants.STACKTRACE, e);
    }
  }
}
