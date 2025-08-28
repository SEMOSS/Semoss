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
package prerna.wikidata;

import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.Callable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.wikidata.wdtk.datamodel.implementation.ItemIdValueImpl;
import org.wikidata.wdtk.datamodel.implementation.PropertyIdValueImpl;
import org.wikidata.wdtk.datamodel.implementation.StringValueImpl;
import org.wikidata.wdtk.datamodel.interfaces.Claim;
import org.wikidata.wdtk.datamodel.interfaces.EntityDocument;
import org.wikidata.wdtk.datamodel.interfaces.ItemDocument;
import org.wikidata.wdtk.datamodel.interfaces.MonolingualTextValue;
import org.wikidata.wdtk.datamodel.interfaces.PropertyIdValue;
import org.wikidata.wdtk.datamodel.interfaces.Snak;
import org.wikidata.wdtk.datamodel.interfaces.Statement;
import org.wikidata.wdtk.datamodel.interfaces.StatementGroup;
import org.wikidata.wdtk.datamodel.interfaces.Value;
import org.wikidata.wdtk.datamodel.interfaces.ValueSnak;
import org.wikidata.wdtk.wikibaseapi.WbSearchEntitiesResult;
import org.wikidata.wdtk.wikibaseapi.WikibaseDataFetcher;

public class WikiLogicalNameCallable implements Callable<List<String>> {

  public static final Logger LOGGER = LogManager.getLogger(WikiLogicalNameCallable.class.getName());
  private Logger logger;

  WbSearchEntitiesResult res;
  WikibaseDataFetcher wbdf;

  public WikiLogicalNameCallable(WikibaseDataFetcher wbdf, WbSearchEntitiesResult res) {
    this.wbdf = wbdf;
    this.res = res;
  }

  @Override
  public List<String> call() throws Exception {
    return getLogicalNames();
  }

  private List<String> getLogicalNames() throws Exception {
    Logger logger = getLogger();
    List<String> logicalNames = new Vector<String>();

    String entityId = res.getEntityId();
    EntityDocument entity = wbdf.getEntityDocument(entityId);
    if (entity instanceof ItemDocument) {
      ItemDocument document = (ItemDocument) entity;
      String documentLabel = null;

      // for logging
      {
        Map<String, MonolingualTextValue> labels = document.getLabels();
        if (labels.get("en") != null) {
          documentLabel = labels.get("en").getText();
          logger.info("Processing document = " + documentLabel);
        }
      }

      List<PropertyIdValue> pList = new Vector<PropertyIdValue>(5);
      // instance of
      // https://www.wikidata.org/wiki/Property:P31
      pList.add(new PropertyIdValueImpl("P31", "http://www.wikidata.org/entity/"));
      // occupation
      // https://www.wikidata.org/wiki/Property:P106
      pList.add(new PropertyIdValueImpl("P106", "http://www.wikidata.org/entity/"));
      // subclass of
      // https://www.wikidata.org/wiki/Property:P279
      pList.add(new PropertyIdValueImpl("P279", "http://www.wikidata.org/entity/"));
      // part of
      // https://www.wikidata.org/wiki/Property:P361
      pList.add(new PropertyIdValueImpl("P361", "http://www.wikidata.org/entity/"));
      // commons category
      // https://www.wikidata.org/wiki/Property:P373
      pList.add(new PropertyIdValueImpl("P373", "http://www.wikidata.org/entity/"));

      List<StatementGroup> statementGroups = document.getStatementGroups();
      for (StatementGroup group : statementGroups) {
        PropertyIdValue gProp = group.getProperty();
        if (pList.contains(gProp)) {
          List<Statement> stmts = group.getStatements();
          for (Statement stmt : stmts) {
            Claim claim = stmt.getClaim();
            if (claim == null) {
              continue;
            }
            Snak snak = claim.getMainSnak();
            if (snak instanceof ValueSnak) {
              Value value = ((ValueSnak) snak).getValue();
              if (value instanceof StringValueImpl) {
                logicalNames.add(((StringValueImpl) value).getValue());
              } else if (value instanceof ItemIdValueImpl) {
                String innerId = ((ItemIdValueImpl) value).getId();
                EntityDocument innerEntity = wbdf.getEntityDocument(innerId);
                if (innerEntity instanceof ItemDocument) {
                  ItemDocument innerDocument = (ItemDocument) innerEntity;
                  MonolingualTextValue labels = innerDocument.getLabels().get("en");
                  if (labels != null) {
                    logicalNames.add(labels.getText());
                  }
                  // if it is the sublcass
                  // also add the document label
                  if (gProp.equals(pList.get(2))) {
                    logicalNames.add(documentLabel);
                  }
                }
              } else {
                System.out.println("Need to account for " + value.getClass().getName());
              }
            }
          }
        }
      }
    }

    return logicalNames;
  }

  public void setLogger(Logger logger) {
    this.logger = logger;
  }

  /**
   * Get the correct logger
   *
   * @return
   */
  private Logger getLogger() {
    if (this.logger == null) {
      return LOGGER;
    }
    return this.logger;
  }
}
