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
package prerna.sablecc2.comm;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import prerna.util.Utility;

public class InMemoryConsole extends Logger {

  private String jobID;
  private boolean partial;

  // Store the FQCN of this class to help Log4j identify the correct caller
  private static final String FQCN = InMemoryConsole.class.getName();

  public InMemoryConsole(String name, String jobId) {
    super((LoggerContext) LogManager.getContext(false), name, null);
    this.jobID = jobId;
    setLevel(Level.INFO);
  }

  public void setPartial(boolean partial) {
    this.partial = partial;
  }

  public void setJobID(String jobID) {
    this.jobID = jobID;
  }

  @Override
  public void info(String message) {
    if (isEnabled(Level.INFO)) {
      String cleanedMessage = Utility.cleanLogString(message);
      // Use the log method with FQCN to preserve caller information
      logMessage(FQCN, Level.INFO, null, cleanedMessage);
      if (partial) {
        PixelJobManager.getManager().addPartialOut(jobID, message + "");
      } else {
        PixelJobManager.getManager().addStdOut(jobID, message + "");
      }
    }
  }

  @Override
  public void debug(String message) {
    if (isEnabled(Level.DEBUG)) {
      String cleanedMessage = Utility.cleanLogString(message);
      // Use the log method with FQCN to preserve caller information
      logMessage(FQCN, Level.DEBUG, null, cleanedMessage);
      PixelJobManager.getManager().addStdErr(jobID, message + "");
    }
  }

  @Override
  public void warn(String message) {
    if (isEnabled(Level.WARN)) {
      String cleanedMessage = Utility.cleanLogString(message);
      logMessage(FQCN, Level.WARN, null, cleanedMessage);
      PixelJobManager.getManager().addStdErr(jobID, message + "");
    }
  }

  @Override
  public void fatal(String message) {
    if (isEnabled(Level.FATAL)) {
      String cleanedMessage = Utility.cleanLogString(message);
      logMessage(FQCN, Level.FATAL, null, cleanedMessage);
      PixelJobManager.getManager().addStdErr(jobID, message + "");
    }
  }
}
