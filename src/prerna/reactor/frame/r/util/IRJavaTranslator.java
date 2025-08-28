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
package prerna.reactor.frame.r.util;

import java.util.List;
import org.apache.logging.log4j.Logger;
import org.rosuda.REngine.Rserve.RConnection;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.r.RDataTable;
import prerna.om.Insight;

public interface IRJavaTranslator {

  String R_CONN = "R_CONN";
  String R_PORT = "R_PORT";
  String R_ENGINE = "R_ENGINE";
  String R_GRAQH_FOLDERS = "R_GRAQH_FOLDERS";

  /** Initialize the environment */
  void initREnv();

  /** Initialize the environment */
  void initREnv(String env);

  /** start r server */
  void startR();

  /**
   * Execute an R Script YOU SHOULD ONLY BE USING THIS WHEN YOU NEED THE RETURN OTHERWISE, USE
   * executeEmptyR
   *
   * @param rScript
   */
  Object executeR(String rScript);

  /**
   * Execute an R Script without a return
   *
   * @param rScript
   */
  void executeEmptyR(String rScript);

  /**
   * Cancel the execution of the currently running R script. Different from stopRProcess in that the
   * R service still runs. Similar to stop in R Studio.
   *
   * @return cancelled
   */
  boolean cancelExecution();

  /**
   * Run a combination of r scripts
   *
   * @param rScript
   */
  void runR(String rScript);

  /**
   * Run a combination of r scripts
   *
   * @param rScript
   */
  String runRAndReturnOutput(String rScript);

  /**
   * Get a string from an r script
   *
   * @param script
   * @return
   */
  String getString(String script);

  /**
   * Get a string array from an r script
   *
   * @param script
   * @return
   */
  String[] getStringArray(String script);

  /**
   * Get a int from an r script
   *
   * @param script
   * @return
   */
  int getInt(String script);

  /**
   * Retrieve an int Array from an R Script
   *
   * @param rScript
   */
  int[] getIntArray(String rScript);

  /**
   * Retrieve a double from an R Script
   *
   * @param rScript
   */
  double getDouble(String rScript);

  /**
   * Retrieve a double Array from an R Script
   *
   * @param rScript
   */
  double[] getDoubleArray(String rScript);

  /**
   * Retrieve a double matrix from an R script
   *
   * @param rScript
   * @return
   */
  double[][] getDoubleMatrix(String rScript);

  /**
   * Retrieve a boolean
   *
   * @param rScript
   * @return
   */
  boolean getBoolean(String rScript);

  /**
   * Retrieve a factor from an R Script
   *
   * @param rScript
   */
  // TODO: why is this an object
  // TODO: why is this an object
  // TODO: why is this an object
  // TODO: why is this an object
  // TODO: why is this an object
  Object getFactor(String rScript);

  /**
   * Set the insight
   *
   * @param insight
   */
  void setInsight(Insight insight);

  /**
   * Set the logger
   *
   * @param logger
   */
  void setLogger(Logger logger);

  /**
   * Set the connection object for Rserve
   *
   * @param connection
   */
  void setConnection(RConnection connection);

  /**
   * Set the port for Rserve
   *
   * @param port
   */
  void setPort(String port);

  /** End the R */
  void endR();

  /** Stop R process */
  void stopRProcess();

  public String[] getColumnTypes(String frameName);

  public boolean isEmpty(String frameName);

  public boolean varExists(String varname);

  public void changeColumnType(String frameName, String columnName, SemossDataType typeToConvert);

  public void changeColumnType(
      String frameName,
      String columnName,
      SemossDataType typeToConvert,
      SemossDataType currentType);

  public String getColumnType(String frameName, String column);

  public void changeColumnType(
      RDataTable frame, String frameName, String colName, String newType, String dateFormat);

  public int getNumRows(String frameName);

  public void initEmptyMatrix(List<Object[]> matrix, int numRows, int numCols);

  public void checkPackages(String[] packages);

  public boolean checkPackages(String[] packages, Logger logger);

  /** This method is used to get the insight */
  public Insight getInsight();
}
