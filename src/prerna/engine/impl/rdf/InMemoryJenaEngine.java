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
package prerna.engine.impl.rdf;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.update.UpdateAction;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.engine.api.IRDFDatabase;
import prerna.engine.impl.AbstractDatabaseEngine;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Holds the database in memory, and uses the Jena API to facilitate querying of RDF data sources.
 */
public class InMemoryJenaEngine extends AbstractDatabaseEngine implements IRDFDatabase {

  private static final Logger classLogger = LogManager.getLogger(InMemoryJenaEngine.class);

  Model jenaModel = null;

  @Override
  public void open(String propFile) {
    // does nothing .. have to set model directly
  }

  @Override
  public void open(Properties smssProp) {
    // does nothing .. have to set model directly
  }

  /**
   * Closes the data base associated with the engine. This will prevent further changes from being
   * made in the data store and safely ends the active transactions and closes the engine.
   */
  @Override
  public void close() {
    // do nothing
  }

  /**
   * Runs the passed string query against the engine as a SELECT query. The query passed must be in
   * the structure of a SELECT SPARQL query and the result format will depend on the engine type.
   *
   * @param query the string version of the SELECT query to be run against the engine
   * @return triple query results that can be displayed as a grid
   */
  @Override
  public Object execQuery(String query) {
    ResultSet rs = null;
    try {
      // QueryExecutionFactory.
      Query q2 = QueryFactory.create(query);
      QueryExecution qex = QueryExecutionFactory.create(q2, jenaModel);
      rs = qex.execSelect();
    } catch (RuntimeException e) {
      classLogger.error(Constants.STACKTRACE, e);
    }
    return rs;
  }

  /**
   * Runs the passed string query against the engine as an INSERT query. The query passed must be in
   * the structure of an INSERT SPARQL query or an INSERT DATA SPARQL query and there are no
   * returned results. The query will result in the specified triples getting added to the data
   * store.
   *
   * @param query the INSERT or INSERT DATA SPARQL query to be run against the engine
   */
  @Override
  public void insertData(String query) {
    UpdateRequest request = UpdateFactory.create();
    request.add(query);
    UpdateAction.execute(request, this.jenaModel);
  }

  @Override
  public void removeData(String query) {
    UpdateRequest request = UpdateFactory.create();
    request.add(query);
    UpdateAction.execute(request, this.jenaModel);
  }

  /**
   * Method setModel. Sets the jena Model to the parameter jena model.
   *
   * @param jenaModel Model - Name of the model that this is being set to.
   */
  public void setModel(Model jenaModel) {
    this.jenaModel = jenaModel;
  }

  public Model getJenaModel() {
    return this.jenaModel;
  }

  @Override
  public DATABASE_TYPE getDatabaseType() {
    return DATABASE_TYPE.JENA;
  }

  /**
   * Processes a SELECT query just like {@link #execSelectQuery(String)} but then parses the results
   * to get only their instance names. These instance names are then returned as the Vector of
   * Strings.
   *
   * @param sparqlQuery the SELECT SPARQL query to be run against the engine
   * @return the Vector of Strings representing the instance names of all of the query results
   */
  @Override
  public Vector<Object> getEntityOfType(String sparqlQuery) {
    // TODO: Don't return null
    return null;
  }

  /**
   * Returns whether or not an engine is currently connected to the data store. The connection
   * becomes true when {@link #open(String)} is called and the connection becomes false when {@link
   * #close()} is called.
   *
   * @return true if the engine is connected to its data store and false if it is not
   */
  @Override
  public boolean isConnected() {
    return true;
  }

  @Override
  public void commit() {
    this.jenaModel.commit();
  }

  @Override
  public void delete() {
    // This does nothing

  }

  @Override
  public boolean holdsFileLocks() {
    return false;
  }

  @Override
  public void addStatement(Object[] args) {
    processStatement(args, true);
  }

  @Override
  public void removeStatement(Object[] args) {
    processStatement(args, false);
  }

  @Override
  public void bulkInsert(List<Object[]> args) {
    for (Object[] obj : args) {
      processStatement(obj, true);
    }
    this.commit();
  }

  @Override
  public void bulkRemoval(List<Object[]> args) {
    for (Object[] obj : args) {
      processStatement(obj, false);
    }
    this.commit();
  }

  /**
   * Adds or removes a triple from the sail connection
   *
   * @param args array contains the following subject String - RDF Subject predicate String - RDF
   *     Predicate object Object - RDF Object concept boolean - True if the statement is a concept
   *     (URI), False if it is a property (Literal)
   * @param add if we are adding or removing the triple
   */
  private void processStatement(Object[] args, boolean add) {
    String subject = args[0] + "";
    String predicate = args[1] + "";
    Object object = args[2];
    Boolean concept = (Boolean) args[3];

    Resource newSub = null;
    Property newPred = null;
    String subString = null;
    String predString = null;
    String sub = subject.trim();
    String pred = predicate.trim();

    subString = Utility.cleanString(sub, false);
    newSub = this.jenaModel.createResource(subString);

    predString = Utility.cleanString(pred, false);
    newPred = this.jenaModel.createProperty(predString);

    RDFNode newObject = null;

    if (concept) {
      String objString = Utility.cleanString((object + "").trim(), false);
      newObject = this.jenaModel.createResource(objString);
    } else {
      if (object instanceof Number) {
        classLogger.debug("Found Double " + object);
        newObject = ResourceFactory.createTypedLiteral(((Number) object).doubleValue());
      } else if (object instanceof Date) {
        classLogger.debug("Found Date " + object);
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        String date = df.format(object);
        newObject = ResourceFactory.createTypedLiteral(date, XSDDatatype.XSDdateTime);
      } else if (object instanceof Boolean) {
        classLogger.debug("Found Boolean " + object);
        newObject = ResourceFactory.createTypedLiteral((Boolean) object);
      } else {
        classLogger.debug("Found String " + object);
        newObject = ResourceFactory.createTypedLiteral(object + "");
      }
    }

    if (add) {
      this.jenaModel.add(newSub, newPred, newObject);
    } else {
      this.jenaModel.remove(newSub, newPred, newObject);
    }
  }

  @Override
  public void infer() throws Exception {
    // do nothing
  }

  @Override
  public void exportDB() throws Exception {
    // do nothing

  }
}
