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
package prerna.rpa.quartz;

import static org.quartz.impl.matchers.GroupMatcher.jobGroupEquals;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.UnableToInterruptJobException;

public class QuartzUtil {

  private static final Logger LOGGER = LogManager.getLogger(QuartzUtil.class.getName());

  private QuartzUtil() {
    throw new IllegalStateException("Utility class");
  }

  public static final void removeAllJobsInGroup(Scheduler scheduler, String jobGroup) {
    LOGGER.info("Removing jobs in the " + jobGroup + " group.");
    for (JobKey jobKey : getJobKeysInGroup(scheduler, jobGroup)) {
      removeJob(scheduler, jobKey);
    }
  }

  public static final void removeJob(Scheduler scheduler, JobKey jobKey) {
    try {
      scheduler.deleteJob(jobKey);
      LOGGER.info("Removing " + jobKey.getName() + " job from the scheduler.");
    } catch (SchedulerException e) {
      LOGGER.error("Failed to remove " + jobKey.getName() + " job from the scheduler.", e);
    }
  }

  public static final void terminateAllJobsInGroup(Scheduler scheduler, String jobGroup) {
    LOGGER.info("Terminating jobs in the " + jobGroup + " group.");
    for (JobKey jobKey : getJobKeysInGroup(scheduler, jobGroup)) {
      terminateJob(scheduler, jobKey);
    }
  }

  public static final void terminateJob(Scheduler scheduler, JobKey jobKey) {
    try {
      LOGGER.info("Terminating the " + jobKey.getName() + " job.");
      scheduler.interrupt(jobKey);
    } catch (UnableToInterruptJobException e) {
      LOGGER.error("Unable to terminate the " + jobKey.getName() + " job.", e);
    }
  }

  public static final Set<JobKey> getJobKeysInGroup(Scheduler scheduler, String jobGroup) {
    Set<JobKey> jobKeysInGroup = new HashSet<>();
    try {
      jobKeysInGroup = scheduler.getJobKeys(jobGroupEquals(jobGroup));
    } catch (SchedulerException e) {
      LOGGER.error("Failed to retrieve jobs in the " + jobGroup + " group.", e);
    }
    return jobKeysInGroup;
  }

  // For testing
  public static final String composeCronForNowPlus(int seconds) {
    LocalDateTime now = LocalDateTime.now();
    now.plusSeconds(seconds);
    int hour = now.getHour();
    int minute = now.getMinute();
    int second = now.getSecond();
    StringBuilder cronStringBuilder = new StringBuilder();
    cronStringBuilder.append(String.format("%02d", second));
    cronStringBuilder.append(" ");
    cronStringBuilder.append(String.format("%02d", minute));
    cronStringBuilder.append(" ");
    cronStringBuilder.append(String.format("%02d", hour));
    cronStringBuilder.append("  1/1 * ? *");
    return cronStringBuilder.toString();
  }
}
