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
package prerna.util.git;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jgit.lib.ProgressMonitor;

public class GitProgressMonitor implements ProgressMonitor {

  Logger logger = LogManager.getLogger(this.getClass());

  boolean complete = false;

  @Override
  public void beginTask(String arg0, int arg1) {
    // TODO Auto-generated method stub
    logger.info("Started this task !!");
  }

  @Override
  public void endTask() {
    // TODO Auto-generated method stub
    logger.info("Completed this task !!");
    complete = true;
  }

  @Override
  public boolean isCancelled() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void start(int arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void update(int arg0) {
    // TODO Auto-generated method stub

  }
}
