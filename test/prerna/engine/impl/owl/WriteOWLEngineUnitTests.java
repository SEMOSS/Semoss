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
package prerna.engine.impl.owl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.SemossUnitTest;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class WriteOWLEngineUnitTests extends SemossUnitTest {

	private WriteOWLEngine engine = null;

	private ReentrantLock writeLock = null;

	private RDFFileSesameEngine rfse = null;

	private Path rdf = null;

	@BeforeEach
	void setup() throws Exception {
		FileUtils.cleanDirectory(tempDir.toFile());

		Properties coreProp = new Properties();
		coreProp.setProperty(Constants.BASE_FOLDER, tempDir.toString());
		DIHelper.getInstance().setCoreProp(coreProp);

		writeLock = new ReentrantLock();

		rdf = tempDir.resolve("rdf.owl");
		Files.createDirectories(rdf.getParent());

		URL url = WriteOWLEngineUnitTests.class.getResource("movie-book.owl");
		assert url != null;

		URI uri = rdf.toUri();
		String baseUri = uri.toString();
		String rdfPath = rdf.toAbsolutePath().toString();

		Path p = Paths.get(url.toURI());
		Files.copy(p, rdf);

		Path smss = tempDir.resolve("engine-01.smss");
		Files.createFile(smss);

		Properties props = new Properties();
		props.setProperty(Constants.ENGINE, "engine-01");
		props.setProperty(Constants.ENGINE_ALIAS, "ea");
		props.setProperty(Constants.RDF_FILE_NAME, rdfPath);
		props.setProperty(Constants.RDF_FILE_PATH, rdfPath);
		props.setProperty(Constants.RDF_FILE_BASE_URI, baseUri);

		props.setProperty(Constants.RDF_FILE_TYPE, "RDF/XML");

		// not exactly sure what to put here
		String typeQuery = "";
		props.setProperty(Constants.TYPE_QUERY, "");

		rfse = new RDFFileSesameEngine();
		rfse.setBasic(true);
		rfse.open(props);
		rfse.createBaseRelationEngine();

		Path db = tempDir.resolve("db");
		Path ea = db.resolve("ea__engine-01");
		Files.createDirectories(ea);

		Files.createFile(ea.resolve("ea_OWL.OWL"));

		engine = new WriteOWLEngine(writeLock, rfse, IDatabaseEngine.DATABASE_TYPE.SESAME, "engine-01", "ea");
	}

	@Test
	void testClose() throws IOException {
		// constructing the engine does not take the write lock
		assertFalse(writeLock.isLocked());

		// simulate OWLEngineFactory.getWriteOWL() acquiring the write lock
		writeLock.lock();
		assertTrue(writeLock.isHeldByCurrentThread());

		// close() releases the lock held by this thread
		engine.close();
		assertFalse(writeLock.isHeldByCurrentThread());
		assertFalse(writeLock.isLocked());

		// a second close() without holding the lock is a safe no-op
		engine.close();
		assertFalse(writeLock.isLocked());
	}

	@Test
	void loadDatabaseValues() {
		engine.loadDatabaseValues();

		Map<String, String> concept = engine.getConceptHash();
		Map<String, String> prop = engine.getPropHash();
		Map<String, String> relation = engine.getRelationHash();

		assertEquals(1, concept.size());
		assertEquals(1, prop.size());
		// Unsure on how to get relations in the hash
		assertEquals(0, relation.size());

		assertEquals("http://semoss.org/ontologies/Concept/TITLE", concept.get("TITLE"));
		assertEquals("http://semoss.org/ontologies/Relation/Contains/BOOK/TITLE", prop.get("TITLE%TITLE"));
	}

	@Test
	void testCreateEmptyOWLFile() throws Exception {
		engine.createEmptyOWLFile();

		Map<String, String> concept = engine.getConceptHash();
		Map<String, String> prop = engine.getPropHash();
		Map<String, String> relation = engine.getRelationHash();

		assertEquals(0, concept.size());
		assertEquals(0, prop.size());
		assertEquals(0, relation.size());
	}

	@Test
	void testReloadFile() throws Exception {
		URL url = WriteOWLEngineUnitTests.class.getResource("empty.owl");
		assert url != null;
		Path p = Paths.get(url.toURI());
		Files.copy(p, rdf, StandardCopyOption.REPLACE_EXISTING);

		engine.reloadOWLFile();

		engine.loadDatabaseValues();
		Map<String, String> concept = engine.getConceptHash();
		Map<String, String> prop = engine.getPropHash();
		Map<String, String> relation = engine.getRelationHash();

		assertEquals(0, concept.size());
		assertEquals(0, prop.size());
		assertEquals(0, relation.size());
	}

	@Test
	void testAddConcept() {
		String val = engine.addConcept("Author");
		assertEquals("http://semoss.org/ontologies/Concept/Author", val);

		Map<String, String> concept = engine.getConceptHash();
		assertEquals(2, concept.size());
		assertEquals("http://semoss.org/ontologies/Concept/TITLE", concept.get("TITLE"));
		assertEquals("http://semoss.org/ontologies/Concept/Author", concept.get("Author"));
	}

	@Test
	void testAddRelation() throws IOException {
		engine.addConcept("AUTHOR");
		engine.addProp("AUTHOR", "MOVIE", "STRING");
		engine.addRelation("AUTHOR", "TITLE", "MOVIE");

		Map<String, String> relations = engine.getRelationHash();
		assertEquals(1, relations.size());
		assertEquals("http://semoss.org/ontologies/Relation/MOVIE", relations.get("AUTHORTITLEMOVIE"));

		engine.export(false);

		String fileContents = new String(Files.readAllBytes(rdf), StandardCharsets.UTF_8);

		String val = "<rdf:Description rdf:about=\"http://semoss.org/ontologies/Concept/AUTHOR\">\n"
				+ "\t<DatatypeProperty xmlns=\"http://www.w3.org/2002/07/owl#\" rdf:resource=\"http://semoss.org/ontologies/Relation/Contains/MOVIE\"/>\n"
				+ "</rdf:Description>\n" + "\n"
				+ "<rdf:Description rdf:about=\"http://semoss.org/ontologies/Relation/Contains/MOVIE\">\n"
				+ "\t<Class rdf:resource=\"TYPE:STRING\"/>\n"
				+ "\t<Pixel xmlns=\"http://semoss.org/ontologies/Relation/\" rdf:resource=\"http://semoss.org/ontologies/Relation/Contains/MOVIE/AUTHOR\"/>\n"
				+ "\t<Conceptual xmlns=\"http://semoss.org/ontologies/Relation/\">MOVIE</Conceptual>\n"
				+ "</rdf:Description>\n" + "\n"
				+ "<rdf:Description rdf:about=\"http://semoss.org/ontologies/Relation/MOVIE\">\n"
				+ "\t<subPropertyOf rdf:resource=\"http://semoss.org/ontologies/Relation\"/>\n" + "</rdf:Description>\n"
				+ "\n" + "<rdf:Description rdf:about=\"http://semoss.org/ontologies/Concept/AUTHOR\">\n"
				+ "\t<MOVIE xmlns=\"http://semoss.org/ontologies/Relation/\" rdf:resource=\"http://semoss.org/ontologies/Concept/TITLE\"/>\n"
				+ "</rdf:Description>";

		assertEquals(1, fileContents.split(val).length - 1);
	}

	@Test
	void testAddProp() {
		engine.addProp("TITLE", "Author", "STRING");
		Map<String, String> prop = engine.getPropHash();
		assertEquals(2, prop.size());
		assertEquals("http://semoss.org/ontologies/Relation/Contains/BOOK/TITLE", prop.get("TITLE%TITLE"));
		assertEquals("http://semoss.org/ontologies/Relation/Contains/Author", prop.get("TITLE%Author"));
	}

	// @Test
	// Need a bigger fix where we either can reset EngineUtility base folder
	// or we create a parent test class that can then use the same temp directory
	// Second approach would be nice because then we can create utils to create
	// engines for testing faster.
	void testAddUniqueCounts() throws IOException {

		ReentrantLock s1 = new ReentrantLock();
		WriteOWLEngine writer = new WriteOWLEngine(s1, rfse.getBaseDataEngine(), IDatabaseEngine.DATABASE_TYPE.SESAME,
				"engine-01", "ea");
		writer.addConcept("TITLE");
		writer.addProp("TITLE", "MOVIE", "STRING");
		writer.addProp("TITLE", "BOOK", "STRING");

		writer.export(false);

		engine.addUniqueCounts(rfse);

		String fileContents = new String(Files.readAllBytes(rdf), StandardCharsets.UTF_8);

		String unique1 = "<rdf:Description rdf:about=\"http://semoss.org/ontologies/Concept/TITLE\">\n"
				+ "\t<UNIQUE xmlns=\"http://semoss.org/ontologies/Relation/Contains/\">0</UNIQUE>\n"
				+ "</rdf:Description>\n" + "\n"
				+ "<rdf:Description rdf:about=\"http://semoss.org/ontologies/Relation/Contains/MOVIE\">\n"
				+ "\t<UNIQUE xmlns=\"http://semoss.org/ontologies/Relation/Contains/\">0</UNIQUE>\n"
				+ "</rdf:Description>\n" + "\n"
				+ "<rdf:Description rdf:about=\"http://semoss.org/ontologies/Relation/Contains/BOOK\">\n"
				+ "\t<UNIQUE xmlns=\"http://semoss.org/ontologies/Relation/Contains/\">0</UNIQUE>\n"
				+ "</rdf:Description>";

		// assert this string only occurs once
		assertEquals(1, fileContents.split(unique1).length - 1);
	}

	@Test
	void addSubClass() throws IOException {
		engine.addSubclass("CHILD", "PARENT");
		Map<String, String> concepts = engine.getConceptHash();
		assertEquals(3, concepts.size());
		assertEquals("http://semoss.org/ontologies/Concept/PARENT", concepts.get("PARENT"));
		assertEquals("http://semoss.org/ontologies/Concept/CHILD", concepts.get("CHILD"));

		engine.export(false);

		String fileContents = new String(Files.readAllBytes(rdf), StandardCharsets.UTF_8);

		String val = "<rdf:Description rdf:about=\"http://semoss.org/ontologies/Concept/CHILD\">\n"
				+ "\t<subClassOf rdf:resource=\"http://semoss.org/ontologies/Concept\"/>\n"
				+ "\t<Class rdf:resource=\"TYPE:STRING\"/>\n"
				+ "\t<Pixel xmlns=\"http://semoss.org/ontologies/Relation/\" rdf:resource=\"http://semoss.org/ontologies/Concept/CHILD\"/>\n"
				+ "\t<Conceptual xmlns=\"http://semoss.org/ontologies/Relation/\">CHILD</Conceptual>\n"
				+ "</rdf:Description>\n" + "\n"
				+ "<rdf:Description rdf:about=\"http://semoss.org/ontologies/Concept/PARENT\">\n"
				+ "\t<subClassOf rdf:resource=\"http://semoss.org/ontologies/Concept\"/>\n"
				+ "\t<Class rdf:resource=\"TYPE:STRING\"/>\n"
				+ "\t<Pixel xmlns=\"http://semoss.org/ontologies/Relation/\" rdf:resource=\"http://semoss.org/ontologies/Concept/PARENT\"/>\n"
				+ "\t<Conceptual xmlns=\"http://semoss.org/ontologies/Relation/\">PARENT</Conceptual>\n"
				+ "</rdf:Description>\n" + "\n"
				+ "<rdf:Description rdf:about=\"http://semoss.org/ontologies/Concept/CHILD\">\n"
				+ "\t<subClassOf rdf:resource=\"http://semoss.org/ontologies/Concept/PARENT\"/>\n"
				+ "</rdf:Description>";

		assertEquals(1, fileContents.split(val).length - 1);
	}

	@Test
	void testRemoveConcept() throws IOException {
		NounMetadata nm = engine.removeConcept("TITLE");

		assertTrue((Boolean) nm.getValue());
		assertEquals("Successfully removed concept and all its dependencies",
				nm.getAdditionalReturn().get(0).getValue().toString());

		// I'm not a huge fan of exporting and then reading the file
		// but it works and makes testing easier.
		engine.export(false);

		String fileContents = new String(Files.readAllBytes(rdf), StandardCharsets.UTF_8);

		String concept = "rdf:Description rdf:about=\"http://semoss.org/ontologies/Concept/TITLE\">\n"
				+ "\t<subClassOf rdf:resource=\"http://semoss.org/ontologies/Concept\"/>\n"
				+ "\t<domain>noData</domain>\n"
				+ "\t<Pixel xmlns=\"http://semoss.org/ontologies/Relation/\" rdf:resource=\"http://semoss.org/ontologies/Concept/TITLE\"/>\n"
				+ "\t<Conceptual xmlns=\"http://semoss.org/ontologies/Relation/\">TITLE</Conceptual>\n"
				+ "</rdf:Description>";

		String movieDataType = "<rdf:Description rdf:about=\"http://semoss.org/ontologies/Concept/TITLE\">\n"
				+ "\t<DatatypeProperty xmlns=\"http://www.w3.org/2002/07/owl#\" rdf:resource=\"http://semoss.org/ontologies/Relation/Contains/MOVIE/TITLE\"/>\n"
				+ "</rdf:Description>";

		String bookDataType = "<rdf:Description rdf:about=\"http://semoss.org/ontologies/Concept/TITLE\">\n"
				+ "\t<DatatypeProperty xmlns=\"http://www.w3.org/2002/07/owl#\" rdf:resource=\"http://semoss.org/ontologies/Relation/Contains/BOOK/TITLE\"/>\n"
				+ "</rdf:Description>";

		assertFalse(fileContents.contains(concept));
		assertFalse(fileContents.contains(movieDataType));
		assertFalse(fileContents.contains(bookDataType));

		Map<String, String> concepts = engine.getConceptHash();
		assertFalse(concepts.containsKey("TITLE"));
	}

	@Test
	void testRemoveRelation() throws IOException {
		// setup engine
		testAddRelation();

		// remove relation
		engine.removeRelation("AUTHOR", "TITLE", "MOVIE");

		Map<String, String> relations = engine.getRelationHash();
		assertEquals(0, relations.size());

		engine.export(false);

		String fileContents = new String(Files.readAllBytes(rdf), StandardCharsets.UTF_8);

		String val = "<rdf:Description rdf:about=\"http://semoss.org/ontologies/Concept/AUTHOR\">\n"
				+ "\t<DatatypeProperty xmlns=\"http://www.w3.org/2002/07/owl#\" rdf:resource=\"http://semoss.org/ontologies/Relation/Contains/MOVIE\"/>\n"
				+ "</rdf:Description>\n" + "\n"
				+ "<rdf:Description rdf:about=\"http://semoss.org/ontologies/Relation/Contains/MOVIE\">\n"
				+ "\t<Class rdf:resource=\"TYPE:STRING\"/>\n"
				+ "\t<Pixel xmlns=\"http://semoss.org/ontologies/Relation/\" rdf:resource=\"http://semoss.org/ontologies/Relation/Contains/MOVIE/AUTHOR\"/>\n"
				+ "\t<Conceptual xmlns=\"http://semoss.org/ontologies/Relation/\">MOVIE</Conceptual>\n"
				+ "</rdf:Description>\n" + "\n"
				+ "<rdf:Description rdf:about=\"http://semoss.org/ontologies/Relation/MOVIE\">\n"
				+ "\t<subPropertyOf rdf:resource=\"http://semoss.org/ontologies/Relation\"/>\n" + "</rdf:Description>\n"
				+ "\n" + "<rdf:Description rdf:about=\"http://semoss.org/ontologies/Concept/AUTHOR\">\n"
				+ "\t<MOVIE xmlns=\"http://semoss.org/ontologies/Relation/\" rdf:resource=\"http://semoss.org/ontologies/Concept/TITLE\"/>\n"
				+ "</rdf:Description>";

		assertFalse(fileContents.contains(val));
	}

	@Test
	void testRemoveProp() {
		NounMetadata nm = engine.removeProp("TITLE", "TITLE");
		assertTrue((Boolean) nm.getValue());
		assertEquals("Successfully removed property", nm.getAdditionalReturn().get(0).getValue().toString());

		assertEquals(0, engine.getPropHash().size());
	}

	@Test
	void testRenameConcept() throws IOException {
		engine.addConcept("OLD");
		engine.export(false);

		String original = new String(Files.readAllBytes(rdf), StandardCharsets.UTF_8);
		int originalCount = original.split("OLD").length - 1;

		NounMetadata nm = engine.renameConcept("OLD", "NEW", "NEW");
		assertTrue((Boolean) nm.getValue());
		assertEquals("Successfully removed concept and all its dependencies",
				nm.getAdditionalReturn().get(0).getValue().toString());

		engine.export(false);

		String fileContents = new String(Files.readAllBytes(rdf), StandardCharsets.UTF_8);
		int newCount = fileContents.split("NEW").length - 1;

		// make sure same amount of keywords and length of files are equal
		assertEquals(originalCount, newCount);

		// the concept cache is refreshed: old key removed, new key points to the
		// renamed concept
		assertNull(engine.getConceptHash().get("OLD"));
		assertEquals("http://semoss.org/ontologies/Concept/NEW", engine.getConceptHash().get("NEW"));
	}

	@Test
	void testRenameProp() throws IOException {
		engine.addConcept("CONCEPT");
		engine.addProp("CONCEPT", "OLD", "STRING");
		engine.export(false);

		String original = new String(Files.readAllBytes(rdf), StandardCharsets.UTF_8);
		int originalCount = original.split("OLD").length - 1;

		NounMetadata nm = engine.renameProp("CONCEPT", "OLD", "NEW");
		assertTrue((Boolean) nm.getValue());
		// Should change the error message
		assertEquals("Successfully removed property", nm.getAdditionalReturn().get(0).getValue().toString());

		engine.export(false);

		String fileContents = new String(Files.readAllBytes(rdf), StandardCharsets.UTF_8);
		int newCount = fileContents.split("NEW").length - 1;

		assertEquals(5, originalCount);
		assertEquals(2, newCount);

		// the prop cache is refreshed: old key removed, new key points to the renamed
		// property
		assertNull(engine.getPropHash().get("CONCEPT%OLD"));
		assertEquals("http://semoss.org/ontologies/Relation/Contains/NEW/CONCEPT",
				engine.getPropHash().get("CONCEPT%NEW"));
	}

}
