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
package prerna.engine.impl.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.EmbeddedModelEngine;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.engine.impl.remotesemoss.RemoteModelEngine;
import prerna.om.Insight;

public class VectorDatabaseCSVTableUnitTests {
  // used by csv file reader
  public static final String SOURCE = "Source";
  public static final String MODALITY = "Modality";
  public static final String DIVIDER = "Divider";
  public static final String PART = "Part";
  public static final String TOKENS = "Tokens";
  public static final String CONTENT = "Content";

  private final String source = "source";
  private final String modality = "modality";
  private final String divider = "divider";
  private final String part = "part";
  private final int tokens = 10;
  private final String content = "content";
  private List<Double> embeddings;
  private VectorDatabaseCSVTable table;

  @BeforeEach
  void setUp() {
    embeddings = new Vector<>();
    embeddings.add(0.2);
    embeddings.add(0.4);
    embeddings.add(0.6);
    embeddings.add(0.8);
    embeddings.add(1.0);
    table = new VectorDatabaseCSVTable();
  }

  @Test
  void testAddRow() {
    table.addRow(source, modality, divider, part, tokens, content);
    List<VectorDatabaseCSVRow> rows = table.getRows();
    for (VectorDatabaseCSVRow row : rows) {
      assertEquals(source, row.getSource());
      assertEquals(modality, row.getModality());
      assertEquals(divider, row.getDivider());
      assertEquals(part, row.getPart());
      assertEquals(tokens, row.getTokens());
      assertEquals(content, row.getContent());
    }
  }

  @Test
  void testGetContent() {
    table.addRow(source, modality, divider, part, tokens, content);
    table.addRow(source, modality, divider, part, tokens, content);
    table.addRow(source, modality, divider, part, tokens, content);
    table.addRow(source, modality, divider, part, tokens, content);
    List<String> allContent = table.getAllContent();
    assertEquals(4, allContent.size());
    for (String currContent : allContent) {
      assertEquals(content, currContent);
    }
  }

  @Test
  void testSetKeywordEngine() {
    IModelEngine keywordEngine = new EmbeddedModelEngine();
    table.setKeywordEngine(keywordEngine);
    IModelEngine output = table.getKeywordEngine();
    assertEquals(keywordEngine, output);
  }

  @Test
  void testSetKeywordEngineWrongEngineType() {
    IModelEngine keywordEngine = new RemoteModelEngine();
    IllegalArgumentException err =
        assertThrows(IllegalArgumentException.class, () -> table.setKeywordEngine(keywordEngine));
    assertEquals("Keyword Engine must be of type EmbeddedModelEngine", err.getMessage());
  }

  @Test
  void testGenerateAndAssignEmbeddings() {
    table.addRow(source, modality, divider, part, tokens, content);
    EmbeddedModelEngine keywordEngineMock = mock();
    List<List<Double>> allEmbeddings = new Vector<>();
    allEmbeddings.add(embeddings);
    EmbeddingsModelEngineResponse output =
        new EmbeddingsModelEngineResponse(allEmbeddings, tokens, tokens);
    when(keywordEngineMock.embeddings(
            any(List.class), nullable(Insight.class), nullable(Map.class)))
        .thenReturn(output);
    table.generateAndAssignEmbeddings(keywordEngineMock, null);
    List<VectorDatabaseCSVRow> rows = table.getRows();
    assertEquals(1, rows.size());
    for (VectorDatabaseCSVRow row : rows) {
      assertEquals(source, row.getSource());
      assertEquals(modality, row.getModality());
      assertEquals(divider, row.getDivider());
      assertEquals(part, row.getPart());
      assertEquals(tokens, row.getTokens());
      assertEquals(content, row.getContent());
      assertEquals(embeddings, row.getEmbeddings());
    }
  }

  @Test
  void testInitCSVTable(@TempDir Path tempDir) throws Exception {
    String mainDir = tempDir.toString();
    Path mainDirPath = Paths.get(mainDir);
    String fileName = "newFile1.csv";
    Path newFilePath = mainDirPath.resolve(fileName);
    Files.createFile(newFilePath);
    String titleStr =
        String.join(",", Arrays.asList(SOURCE, MODALITY, DIVIDER, PART, TOKENS, CONTENT));
    String contentStr =
        String.join(",", Arrays.asList(source, modality, divider, part, tokens + "", content));
    List<String> lines = Arrays.asList(titleStr, contentStr);
    Files.write(newFilePath, lines);
    File newFile = newFilePath.toFile();
    table = VectorDatabaseCSVTable.initCSVTable(newFile, 1);
    List<VectorDatabaseCSVRow> rows = table.getRows();
    assertEquals(1, rows.size());
    for (VectorDatabaseCSVRow row : rows) {
      assertEquals(source, row.getSource());
      assertEquals(modality, row.getModality());
      assertEquals(divider, row.getDivider());
      assertEquals(part, row.getPart());
      assertEquals(tokens, row.getTokens());
      assertEquals(content, row.getContent());
    }
  }

  @Test
  void testValidateInitTable(@TempDir Path tempDir) throws Exception {
    String mainDir = tempDir.toString();
    Path mainDirPath = Paths.get(mainDir);
    String fileName = "newFile1.csv";
    Path newFilePath = mainDirPath.resolve(fileName);
    Files.createFile(newFilePath);
    String titleStr =
        String.join(",", Arrays.asList(SOURCE, MODALITY, DIVIDER, PART, TOKENS, CONTENT));
    String contentStr =
        String.join(",", Arrays.asList(source, modality, divider, part, tokens + "", content));
    List<String> lines = Arrays.asList(titleStr, contentStr);
    Files.write(newFilePath, lines);
    File newFile = newFilePath.toFile();
    assertTrue(VectorDatabaseCSVTable.validateCSVTable(newFile));

    String fileName2 = "newFile2.csv";
    Path newFilePath2 = mainDirPath.resolve(fileName2);
    Files.createFile(newFilePath2);
    String contentStr2 = String.join(",", Arrays.asList("", "", "", "", "", ""));
    lines = Arrays.asList(titleStr, contentStr2);
    Files.write(newFilePath2, lines);
    File newFile2 = newFilePath2.toFile();
    assertFalse(VectorDatabaseCSVTable.validateCSVTable(newFile2));
  }

  @Test
  void testPullSourceColumn(@TempDir Path tempDir) throws Exception {
    String mainDir = tempDir.toString();
    Path mainDirPath = Paths.get(mainDir);
    String fileName = "newFile1.csv";
    Path newFilePath = mainDirPath.resolve(fileName);
    Files.createFile(newFilePath);
    String titleStr =
        String.join(",", Arrays.asList(SOURCE, MODALITY, DIVIDER, PART, TOKENS, CONTENT));
    String contentStr =
        String.join(",", Arrays.asList(source, modality, divider, part, tokens + "", content));
    List<String> lines = Arrays.asList(titleStr, contentStr);
    Files.write(newFilePath, lines);
    File newFile = newFilePath.toFile();
    Set<String> sources = VectorDatabaseCSVTable.pullSourceColumn(newFile);
    assertEquals(1, sources.size());
    for (String src : sources) {
      assertEquals(src, source);
    }
  }
}
