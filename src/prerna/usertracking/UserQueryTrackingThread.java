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
package prerna.usertracking;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.User;
import prerna.util.Utility;

public class UserQueryTrackingThread implements Runnable {

  private static final Logger classLogger = LogManager.getLogger(UserQueryTrackingThread.class);

  private User user = null;
  private String engineId = null;
  private String query = null;
  private java.sql.Timestamp startTime = null;
  private java.sql.Timestamp endTime = null;
  private boolean failed = false;

  /**
   * @param user
   * @param engineId
   */
  public UserQueryTrackingThread(User user, String engineId) {
    this.user = user;
    this.engineId = engineId;
  }

  @Override
  public void run() {
    Long executionTime = null;
    if (endTime != null) {
      executionTime = endTime.getTime() - startTime.getTime();
    }
    if (this.startTime == null) {
      classLogger.warn("Storing query execution without a start time.");
    }
    UserTrackingUtils.trackQueryExecution(
        user, engineId, query, startTime, endTime, executionTime, failed);
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public void setFailed() {
    this.failed = true;
  }

  public void setStartTimeNow() {
    this.startTime = Utility.getCurrentSqlTimestampUTC();
  }

  public void setEndTimeNow() {
    this.endTime = Utility.getCurrentSqlTimestampUTC();
  }
}
