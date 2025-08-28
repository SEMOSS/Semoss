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
package prerna.reactor.task.lambda.flatmap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.engine.api.IHeadersDataRow;
import prerna.io.connector.google.GoogleEntityResolver;
import prerna.om.EntityResolution;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class GoogleEntityAnalyzerLambda extends AbstractFlatMapLambda {

  private static final Logger classLogger = LogManager.getLogger(GoogleEntityAnalyzerLambda.class);

  // col index we care about to get lat/long from
  private int colIndex;
  // total number of columns
  private int totalCols;

  @Override
  public List<IHeadersDataRow> process(IHeadersDataRow row) {
    Object value = row.getValues()[colIndex];
    if (value == null || value.toString().isEmpty()) {
      return new Vector<IHeadersDataRow>();
    }
    // grab the column index we want to use as the address
    Map<String, Object> params = new HashMap<>();
    Map<String, Object> docParam = new HashMap<>();
    docParam.put("type", "PLAIN_TEXT");
    docParam.put("language", "EN");
    docParam.put("content", value.toString().replace("_", " "));
    params.put("document", docParam);

    // construct new values to append onto the row
    // add new headers
    String[] newHeaders =
        new String[] {"entity_name", "entity_type", "wiki_url", "content", "content_subtype"};

    List<IHeadersDataRow> retList = new Vector<IHeadersDataRow>();
    try {
      // loop through the results
      GoogleEntityResolver goog = new GoogleEntityResolver();
      Object resultObj = goog.execute(this.user, params);
      if (resultObj instanceof List) {
        List<EntityResolution> results = (List<EntityResolution>) resultObj;
        for (int i = 0; i < results.size(); i++) {
          EntityResolution entity = results.get(i);
          processEntity(entity, newHeaders, row, retList);
        }
      } else {
        EntityResolution entity = (EntityResolution) resultObj;
        processEntity(entity, newHeaders, row, retList);
      }
    } catch (Exception e) {
      classLogger.error(Constants.STACKTRACE, e);
    }

    return retList;
  }

  private void processEntity(
      EntityResolution entity,
      String[] newHeaders,
      IHeadersDataRow curRow,
      List<IHeadersDataRow> retList) {
    Object[] newValues = new Object[5];
    newValues[0] = entity.getEntity_name();
    newValues[1] = entity.getEntity_type();
    newValues[2] = entity.getWiki_url();
    newValues[3] = entity.getContent();
    newValues[4] = entity.getContent_subtype();

    // copy the row so we dont mess up references
    IHeadersDataRow rowCopy = curRow.copy();
    rowCopy.addFields(newHeaders, newValues);
    retList.add(rowCopy);
  }

  @Override
  public void init(List<Map<String, Object>> headerInfo, List<String> columns) {
    if (this.user == null) {
      SemossPixelException exception =
          new SemossPixelException(
              new NounMetadata(
                  "Requires login to google",
                  PixelDataType.CONST_STRING,
                  PixelOperationType.ERROR));
      exception.setContinueThreadOfExecution(false);
      throw exception;
    }
    AccessToken googleAccess = this.user.getAccessToken(AuthProvider.GOOGLE);
    if (googleAccess == null) {
      SemossPixelException exception =
          new SemossPixelException(
              new NounMetadata(
                  "Requires login to google",
                  PixelDataType.CONST_STRING,
                  PixelOperationType.ERROR));
      exception.setContinueThreadOfExecution(false);
      throw exception;
    }

    this.headerInfo = headerInfo;
    this.totalCols = headerInfo.size();

    String headerToConvert = columns.get(0);
    for (int j = 0; j < totalCols; j++) {
      Map<String, Object> headerMap = headerInfo.get(j);
      String alias = headerMap.get("alias").toString();
      if (alias.equals(headerToConvert)) {
        // we found the index
        this.colIndex = j;
      }
    }

    // this modifies the header info map by reference
    Map<String, Object> entityHeader = getBaseHeader("entity_name", "STRING");
    this.headerInfo.add(entityHeader);
    Map<String, Object> typeHeader = getBaseHeader("entity_type", "STRING");
    this.headerInfo.add(typeHeader);
    Map<String, Object> wikiHeader = getBaseHeader("wiki_url", "STRING");
    this.headerInfo.add(wikiHeader);
    Map<String, Object> contentHeader = getBaseHeader("content", "STRING");
    this.headerInfo.add(contentHeader);
    Map<String, Object> contentTypeHeader = getBaseHeader("content_subtype", "STRING");
    this.headerInfo.add(contentTypeHeader);
  }

  /**
   * Grab a base header object
   *
   * @param name
   * @param type
   * @return
   */
  private Map<String, Object> getBaseHeader(String name, String type) {
    Map<String, Object> header = new HashMap<String, Object>();
    header.put("alias", name);
    header.put("header", name);
    header.put("derived", true);
    header.put("type", type.toUpperCase());
    return header;
  }
}
