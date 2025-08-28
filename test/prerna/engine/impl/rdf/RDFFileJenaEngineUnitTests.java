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

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.*;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Stream;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.update.UpdateAction;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import prerna.engine.api.IDatabaseEngine;
import prerna.util.Constants;

public class RDFFileJenaEngineUnitTests {

  private RDFFileJenaEngine engine;
  private String baseUri;
  private String rdfPath;

  @Mock private Model model;

  @BeforeEach
  void setUp(@TempDir Path tempDir) throws Exception {
    MockitoAnnotations.openMocks(this);
    engine = new RDFFileJenaEngine();

    Path rdf = tempDir.resolve("rdf.owl");
    Files.createDirectories(rdf.getParent());
    URI uri = rdf.toUri();
    baseUri = uri.toString();
    rdfPath = rdf.toAbsolutePath().toString();

    Properties props = new Properties();
    props.setProperty(Constants.ENGINE, "engine-01");
    props.setProperty(Constants.ENGINE_ALIAS, "ea");
    props.setProperty(Constants.RDF_FILE_NAME, rdfPath);
    props.setProperty(Constants.RDF_FILE_BASE_URI, baseUri);
    props.setProperty(Constants.RDF_FILE_TYPE, "RDF/XML");

    engine.setBasic(true);

    try (MockedStatic<RDFDataMgr> ignored = Mockito.mockStatic(RDFDataMgr.class);
        MockedStatic<ModelFactory> mfStatic = Mockito.mockStatic(ModelFactory.class)) {
      mfStatic.when(ModelFactory::createDefaultModel).thenReturn(model);
      engine.open(props);
    }
  }

  @Test
  void testOpen() throws Exception {
    // open is called in before each
    assertNotNull(engine.getJenaModel());
    assertEquals("engine-01", engine.getEngineId());
    assertEquals("ea", engine.getEngineName());
    assertTrue(engine.isConnected());
  }

  @Test
  void testClose() throws Exception {
    engine.close();
    verify(model, times(1)).close();
  }

  @Test
  void testExecQueryRS() {
    String queryString = "Select";
    Query q2 = mock(Query.class);
    QueryExecution qe = mock(QueryExecution.class);
    try (MockedStatic<QueryFactory> qfMockedStatic = Mockito.mockStatic(QueryFactory.class);
        MockedStatic<QueryExecutionFactory> qefMockedStatic =
            Mockito.mockStatic(QueryExecutionFactory.class); ) {

      qfMockedStatic.when(() -> QueryFactory.create(queryString)).thenReturn(q2);
      qefMockedStatic.when(() -> QueryExecutionFactory.create(q2, model)).thenReturn(qe);

      when(q2.isSelectType()).thenReturn(true);
      ResultSet rs = mock(ResultSet.class);
      when(qe.execSelect()).thenReturn(rs);

      Object result = engine.execQuery(queryString);
      assertEquals(rs, result);
    }
  }

  @Test
  void testExecQueryConstruct() {
    String queryString = "Select";
    Query q2 = mock(Query.class);
    QueryExecution qe = mock(QueryExecution.class);
    try (MockedStatic<QueryFactory> qfMockedStatic = Mockito.mockStatic(QueryFactory.class);
        MockedStatic<QueryExecutionFactory> qefMockedStatic =
            Mockito.mockStatic(QueryExecutionFactory.class); ) {

      qfMockedStatic.when(() -> QueryFactory.create(queryString)).thenReturn(q2);
      qefMockedStatic.when(() -> QueryExecutionFactory.create(q2, model)).thenReturn(qe);

      when(q2.isSelectType()).thenReturn(false);
      when(q2.isConstructType()).thenReturn(true);
      Model resultModel = mock(Model.class);
      when(qe.execConstruct()).thenReturn(resultModel);

      Object result = engine.execQuery(queryString);
      assertEquals(resultModel, result);
    }
  }

  @Test
  void testExecQueryAskType() {
    String queryString = "Select";
    Query q2 = mock(Query.class);
    QueryExecution qe = mock(QueryExecution.class);
    try (MockedStatic<QueryFactory> qfMockedStatic = Mockito.mockStatic(QueryFactory.class);
        MockedStatic<QueryExecutionFactory> qefMockedStatic =
            Mockito.mockStatic(QueryExecutionFactory.class); ) {

      qfMockedStatic.when(() -> QueryFactory.create(queryString)).thenReturn(q2);
      qefMockedStatic.when(() -> QueryExecutionFactory.create(q2, model)).thenReturn(qe);

      when(q2.isSelectType()).thenReturn(false);
      when(q2.isConstructType()).thenReturn(false);
      when(q2.isAskType()).thenReturn(true);

      when(qe.execAsk()).thenReturn(true);

      Object result = engine.execQuery(queryString);
      assertTrue((Boolean) result);
    }
  }

  @Test
  void testExecQueryNull() {
    String queryString = "Select";
    Query q2 = mock(Query.class);
    QueryExecution qe = mock(QueryExecution.class);
    try (MockedStatic<QueryFactory> qfMockedStatic = Mockito.mockStatic(QueryFactory.class);
        MockedStatic<QueryExecutionFactory> qefMockedStatic =
            Mockito.mockStatic(QueryExecutionFactory.class); ) {

      qfMockedStatic.when(() -> QueryFactory.create(queryString)).thenReturn(q2);
      qefMockedStatic.when(() -> QueryExecutionFactory.create(q2, model)).thenReturn(qe);

      when(q2.isSelectType()).thenReturn(false);
      when(q2.isConstructType()).thenReturn(false);
      when(q2.isAskType()).thenReturn(false);

      Object result = engine.execQuery(queryString);
      assertNull(result);
    }
  }

  @Test
  void testInsertData() {
    UpdateRequest p = mock(UpdateRequest.class);
    try (MockedStatic<UpdateFactory> factoryMockedStatic = Mockito.mockStatic(UpdateFactory.class);
        MockedStatic<UpdateAction> actionMockedStatic = Mockito.mockStatic(UpdateAction.class)) {
      factoryMockedStatic.when(UpdateFactory::create).thenReturn(p);

      engine.insertData("test");

      factoryMockedStatic.verify(UpdateFactory::create, times(1));
      actionMockedStatic.verify(() -> UpdateAction.execute(p, model), times(1));
    }
  }

  @Test
  void testRemoveData() {
    UpdateRequest p = mock(UpdateRequest.class);
    try (MockedStatic<UpdateFactory> factoryMockedStatic = Mockito.mockStatic(UpdateFactory.class);
        MockedStatic<UpdateAction> actionMockedStatic = Mockito.mockStatic(UpdateAction.class)) {
      factoryMockedStatic.when(UpdateFactory::create).thenReturn(p);

      engine.removeData("test");

      factoryMockedStatic.verify(UpdateFactory::create, times(1));
      actionMockedStatic.verify(() -> UpdateAction.execute(p, model), times(1));
    }
  }

  @Test
  void testGetDatabaseType() {
    assertEquals(IDatabaseEngine.DATABASE_TYPE.JENA, engine.getDatabaseType());
  }

  @Test
  void testHoldsFileLock() {
    assertTrue(engine.holdsFileLocks());
  }

  @Test
  void testCleanSelect() {
    String queryString = "Select";
    Query q2 = mock(Query.class);
    QueryExecution qe = mock(QueryExecution.class);
    try (MockedStatic<QueryFactory> qfMockedStatic = Mockito.mockStatic(QueryFactory.class);
        MockedStatic<QueryExecutionFactory> qefMockedStatic =
            Mockito.mockStatic(QueryExecutionFactory.class); ) {

      qfMockedStatic.when(() -> QueryFactory.create(queryString)).thenReturn(q2);
      qefMockedStatic.when(() -> QueryExecutionFactory.create(q2, model)).thenReturn(qe);

      when(q2.isSelectType()).thenReturn(true);
      ResultSet rs = mock(ResultSet.class);
      when(qe.execSelect()).thenReturn(rs);

      when(rs.hasNext()).thenReturn(true).thenReturn(false);
      List<String> vals = new ArrayList<>();
      vals.add("test-1");
      when(rs.getResultVars()).thenReturn(vals);

      QuerySolution qs = mock(QuerySolution.class);
      RDFNode rdfNode = mock(RDFNode.class);
      when(qs.get("test-1")).thenReturn(rdfNode);
      when(rs.next()).thenReturn(qs);

      Vector<Object> result = engine.getCleanSelect(queryString);

      assertEquals(1, result.size());
      assertEquals(rdfNode.toString(), result.get(0));
    }
  }

  @Test
  void testGetEntityOfType() {
    Properties p = new Properties();
    p.setProperty(Constants.TYPE_QUERY, "@entity@");
    engine.setSmssProp(p);
    String queryString = "Select";
    Query q2 = mock(Query.class);
    QueryExecution qe = mock(QueryExecution.class);
    try (MockedStatic<QueryFactory> qfMockedStatic = Mockito.mockStatic(QueryFactory.class);
        MockedStatic<QueryExecutionFactory> qefMockedStatic =
            Mockito.mockStatic(QueryExecutionFactory.class); ) {

      qfMockedStatic.when(() -> QueryFactory.create(queryString)).thenReturn(q2);
      qefMockedStatic.when(() -> QueryExecutionFactory.create(q2, model)).thenReturn(qe);

      when(q2.isSelectType()).thenReturn(true);
      ResultSet rs = mock(ResultSet.class);
      when(qe.execSelect()).thenReturn(rs);

      when(rs.hasNext()).thenReturn(true).thenReturn(false);
      List<String> vals = new ArrayList<>();
      vals.add("test-1");
      when(rs.getResultVars()).thenReturn(vals);

      QuerySolution qs = mock(QuerySolution.class);
      RDFNode rdfNode = mock(RDFNode.class);
      when(qs.get("test-1")).thenReturn(rdfNode);
      when(rs.next()).thenReturn(qs);

      Vector<Object> result = engine.getEntityOfType(queryString);

      assertEquals(1, result.size());
      assertEquals(rdfNode.toString(), result.get(0));
    }
  }

  @Test
  void testIsConnected() {
    assertTrue(engine.isConnected());
  }

  static Stream<Arguments> determineLangSupplier() {
    return Stream.of(
        arguments("RDF/XML", Lang.RDFXML),
        arguments("TURTLE", Lang.TURTLE),
        arguments("N3", Lang.N3),
        arguments("NTRIPLES", Lang.NTRIPLES),
        arguments("TRIG", Lang.TRIG),
        arguments("TRIX", Lang.TRIX));
  }

  @ParameterizedTest
  @MethodSource("determineLangSupplier")
  void testDetermineLang(String queryString, Lang lang) {
    assertEquals(lang, engine.determineLang(queryString));
  }

  @Test
  void testDetermineLangNull() {
    assertNull(engine.determineLang("not found"));
  }

  @Test
  void testCommit() {
    engine.commit();
    verify(model, times(1)).commit();
  }

  @Test
  void testAddStatementNumber() {
    String subject = " subject ";
    String predicate = " predicate ";
    Integer number = 2;
    Boolean concept = false;

    Resource resource = mock(Resource.class);
    Property prop = mock(Property.class);
    when(model.createResource("subject")).thenReturn(resource);
    when(model.createProperty("predicate")).thenReturn(prop);

    Object[] args = {subject, predicate, number, concept};

    engine.addStatement(args);

    RDFNode node = ResourceFactory.createTypedLiteral(2.0);
    verify(model, times(1)).add(resource, prop, node);
  }

  @Test
  void testAddStatementDate() {
    String subject = " subject ";
    String predicate = " predicate ";
    Date dateObject = new Date();
    Boolean concept = false;

    Resource resource = mock(Resource.class);
    Property prop = mock(Property.class);
    when(model.createResource("subject")).thenReturn(resource);
    when(model.createProperty("predicate")).thenReturn(prop);

    Object[] args = {subject, predicate, dateObject, concept};

    engine.addStatement(args);

    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    String date = df.format(dateObject);
    RDFNode newObject = ResourceFactory.createTypedLiteral(date, XSDDatatype.XSDdateTime);
    verify(model, times(1)).add(resource, prop, newObject);
  }

  @Test
  void testAddStatementBoolean() {
    String subject = " subject ";
    String predicate = " predicate ";
    Boolean boolObject = true;
    Boolean concept = false;

    Resource resource = mock(Resource.class);
    Property prop = mock(Property.class);
    when(model.createResource("subject")).thenReturn(resource);
    when(model.createProperty("predicate")).thenReturn(prop);

    Object[] args = {subject, predicate, boolObject, concept};

    engine.addStatement(args);

    RDFNode node = ResourceFactory.createTypedLiteral(true);
    verify(model, times(1)).add(resource, prop, node);
  }

  @Test
  void testAddStatementString() {
    String subject = " subject ";
    String predicate = " predicate ";
    String string = "test";
    Boolean concept = false;

    Resource resource = mock(Resource.class);
    Property prop = mock(Property.class);
    when(model.createResource("subject")).thenReturn(resource);
    when(model.createProperty("predicate")).thenReturn(prop);

    Object[] args = {subject, predicate, string, concept};

    engine.addStatement(args);

    RDFNode node = ResourceFactory.createTypedLiteral("test");
    verify(model, times(1)).add(resource, prop, node);
  }

  @Test
  void testRemoveStatement() {
    String subject = " subject ";
    String predicate = " predicate ";
    String string = "test";
    Boolean concept = true;

    Resource resource = mock(Resource.class);
    Property prop = mock(Property.class);
    when(model.createResource("subject")).thenReturn(resource);
    when(model.createProperty("predicate")).thenReturn(prop);

    Resource r2 = mock(Resource.class);
    when(model.createResource("test")).thenReturn(r2);

    Object[] args = {subject, predicate, string, concept};

    engine.removeStatement(args);

    verify(model, times(1)).remove(resource, prop, r2);
  }

  @Test
  void testGetJenaModel() {
    assertEquals(model, engine.getJenaModel());
  }
}
