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
package prerna.sablecc2.om.task;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.Logger;
import prerna.ds.shared.RawCachedWrapper;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.reactor.export.IFormatter;
import prerna.sablecc2.om.task.options.TaskOptions;

public interface ITask extends Iterator<IHeadersDataRow>, Closeable {

  /**
   * Basic operation to get a certain number of records from the data Meta is used to determine if
   * we need to send additional meta data around the creation of the task
   *
   * @param numRecordsToGet
   * @param meta
   * @return
   * @throws Exception
   */
  Map<String, Object> collect(boolean meta) throws Exception;

  Map<String, Object> getMetaMap();

  boolean getMeta();

  void setMeta(boolean meta);

  void setNumCollect(int numCollect);

  int getNumCollect();

  void setId(String taskId);

  String getId();

  void setFormat(String formatType);

  void setFormat(IFormatter formatter);

  IFormatter getFormatter();

  void setFormatOptions(Map<String, Object> optionValues);

  void setTaskOptions(TaskOptions taskOptions);

  TaskOptions getTaskOptions();

  void setHeaderInfo(List<Map<String, Object>> headerInfo);

  List<Map<String, Object>> getHeaderInfo();

  void setSortInfo(List<Map<String, Object>> sortInfo);

  List<Map<String, Object>> getSortInfo();

  void setFilterInfo(GenRowFilters grf);

  List<Map<String, Object>> getFilterInfo();

  List<Object[]> flushOutIteratorAsGrid();

  void setLogger(Logger logger);

  void optimizeQuery(int limit) throws Exception;

  boolean isOptimized();

  void toOptimize(boolean toOptimize);

  void reset() throws Exception;

  // creates a cache object to be utilized
  RawCachedWrapper createCache() throws Exception;

  // get the pragma being set
  String getPragma(String key);
}
