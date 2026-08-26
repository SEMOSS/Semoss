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
package prerna.util.files;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.apache.pdfbox.pdmodel.font.Standard14Fonts;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * JUnit tests for the SemossParsedFile class.
 *
 * <p>This class provides tests that cover the major functionality including:
 *
 * <ul>
 *   <li>File creation and initialization
 *   <li>Extracted content management
 *   <li>Timestamp-based synchronization
 *   <li>File lifecycle operations (rename, delete)
 * </ul>
 */
public class SemossParsedFileTests {

  private Path tempDir;

  @BeforeEach
  public void setUp() throws IOException {
    tempDir = Files.createTempDirectory("semoss-parsed-file-test");
  }

  @AfterEach
  public void tearDown() {
    deleteDirectoryRecursively(tempDir.toFile());
  }

  @Test
  public void testConstructorWithPathname() throws IOException {
    File testFile = createTestFile("test1.txt", "content");
    SemossParsedFile SemossParsedFile = new SemossParsedFile(testFile.getAbsolutePath());

    assertEquals(testFile.getAbsolutePath(), SemossParsedFile.getAbsolutePath(), "File paths should match");
    assertNotNull(
        SemossParsedFile.getExtractedContentsFilePath(), "Extracted content path should not be null");
  }

  @Test
  public void testConstructorWithFile() throws IOException {
    File testFile = createTestFile("test2.txt", "content");
    SemossParsedFile SemossParsedFile = new SemossParsedFile(testFile);

    assertEquals(testFile.getAbsolutePath(), SemossParsedFile.getAbsolutePath(), "File paths should match");
  }

  @Test
  public void testAutomaticContentExtraction() throws IOException {
    File testFile = createTestFile("test3.txt", "This is the original file content");
    SemossParsedFile SemossParsedFile = new SemossParsedFile(testFile);

    String extractedContent = Objects.requireNonNull(SemossParsedFile.getExtractedContents());
    assertTrue(
        extractedContent.contains("Source,Modality,Divider,Part,Content"),
        "Should contain CSV headers");
    assertTrue(
        extractedContent.contains("original file content"), "Content should contain original text");
    assertNotNull(SemossParsedFile.getExtractionChain(), "Extraction chain should be initialized");
  }

  @Test
  public void testIsSynchronized() throws IOException, InterruptedException {
    File testFile = createTestFile("test4.txt", "original content");
    SemossParsedFile SemossParsedFile = new SemossParsedFile(testFile);

    assertTrue(SemossParsedFile.isSynchronized(), "Should be synchronized after auto-parsing");
    assertTrue(
        SemossParsedFile.isExtractedContentAvailable(), "Content should be available after auto-parsing");

    Thread.sleep(1000);
    Files.writeString(testFile.toPath(), "modified content");

    assertFalse(SemossParsedFile.isSynchronized(), "Should not be synchronized after file modification");
  }

  @Test
  public void testFileWithNoExtension() throws IOException {
    File testFile = createTestFile("document-no-ext", "content without extension");
    SemossParsedFile SemossParsedFile = new SemossParsedFile(testFile);

    String extractedContent = Objects.requireNonNull(SemossParsedFile.getExtractedContents());
    assertTrue(
        extractedContent.contains("Source,Modality,Divider,Part,Content"),
        "Should contain CSV headers");
    assertTrue(
        extractedContent.contains("content without extension"), "Content should match original");

    String extractedPath = SemossParsedFile.getExtractedContentsFilePath();
    assertTrue(
        extractedPath.contains("document-no-ext.txt"),
        "Should handle files without extension correctly");
  }

  @Test
  public void testRename() throws IOException {
    File testFile = createTestFile("original.txt", "original file content");
    SemossParsedFile SemossParsedFile = new SemossParsedFile(testFile);

    assertTrue(SemossParsedFile.isExtractedContentAvailable(), "Content should be auto-extracted");
    String originalContent = Objects.requireNonNull(SemossParsedFile.getExtractedContents());
    assertTrue(
        originalContent.contains("Source,Modality,Divider,Part,Content"),
        "Should contain CSV headers");
    assertTrue(originalContent.contains("original file content"), "Content should match original");

    File newFile = tempDir.resolve("renamed.txt").toFile();
    boolean renamed = SemossParsedFile.renameTo(newFile);

    assertTrue(renamed, "Rename should succeed");
    assertTrue(newFile.exists(), "New file should exist");
    assertFalse(testFile.exists(), "Original file should not exist");
    assertTrue(
        SemossParsedFile.isExtractedContentAvailable(), "Extracted content should still be available");
  }

  @Test
  public void testDelete() throws IOException {
    File testFile = createTestFile("todelete.txt", "content to be deleted");
    SemossParsedFile SemossParsedFile = new SemossParsedFile(testFile);

    File extractedFile = SemossParsedFile.getExtractedContentFile();
    assertTrue(
        SemossParsedFile.isExtractedContentAvailable(), "Content should be auto-extracted before delete");
    assertTrue(extractedFile.exists(), "Extracted file should exist before delete");

    boolean deleted = SemossParsedFile.delete();

    assertTrue(deleted, "Delete should succeed");
    assertFalse(testFile.exists(), "Original file should not exist after delete");
    assertFalse(extractedFile.exists(), "Extracted file should not exist after delete");
  }

  @Test
  public void testEmptyContent() throws IOException {
    File testFile = createTestFile("empty.txt", "");
    SemossParsedFile SemossParsedFile = new SemossParsedFile(testFile);

    String extractedContent = Objects.requireNonNull(SemossParsedFile.getExtractedContents());
    assertTrue(
        extractedContent.contains("Source,Modality,Divider,Part,Content"),
        "Should contain CSV headers");
    assertTrue(extractedContent.contains("empty.txt"), "Should contain filename in metadata");
    assertTrue(extractedContent.contains(",\"\""), "Content field should be empty (empty quotes)");
    assertTrue(SemossParsedFile.isSynchronized(), "Should be synchronized with empty content");
  }

  @Test
  public void testSpecialCharacters() throws IOException {
    String specialContent =
        "Content with special characters: àáâãäåæçèéêë ñóôõöø ùúûü ß 中文 العربية русский";
    File testFile = createTestFile("special.txt", specialContent);
    SemossParsedFile SemossParsedFile = new SemossParsedFile(testFile);

    String extractedContent = Objects.requireNonNull(SemossParsedFile.getExtractedContents());
    assertTrue(
        extractedContent.contains("Source,Modality,Divider,Part,Content"),
        "Should contain CSV headers");
    assertTrue(extractedContent.contains("àáâãäåæçèéêë"), "Should preserve special characters");
    assertTrue(extractedContent.contains("中文"), "Should preserve Unicode characters");
  }

  @Test
  public void testSpecialCharactersInFilename() throws IOException {
    String filename = "test-file_with#special$chars&name (1) [copy].txt";
    String content = "Content of file with special characters in name";
    File testFile = createTestFile(filename, content);
    SemossParsedFile SemossParsedFile = new SemossParsedFile(testFile);

    assertTrue(SemossParsedFile.exists(), "File should exist");
    assertEquals(
        filename, SemossParsedFile.getName(), "Filename should be preserved with special characters");

    String extractedContent = Objects.requireNonNull(SemossParsedFile.getExtractedContents());
    assertTrue(
        extractedContent.contains("Source,Modality,Divider,Part,Content"),
        "Should contain CSV headers");
    assertTrue(
        extractedContent.contains("Content of file with special characters"),
        "Should extract content correctly");
    assertTrue(
        extractedContent.contains(filename),
        "Filename with special chars should appear in metadata");
    assertTrue(SemossParsedFile.isSynchronized(), "Should be synchronized after parsing");

    String extractedPath = SemossParsedFile.getExtractedContentsFilePath();
    assertNotNull(extractedPath, "Extracted content path should be valid");
    assertTrue(extractedPath.length() > 0, "Extracted content path should not be empty");
    assertTrue(
        SemossParsedFile.isExtractedContentAvailable(),
        "Content should be available for file with special chars in name");
  }

  @Test
  public void testAutomaticReparsing() throws IOException, InterruptedException {
    File testFile = createTestFile("reparse.txt", "Original content");
    SemossParsedFile SemossParsedFile = new SemossParsedFile(testFile);

    String initialContent = Objects.requireNonNull(SemossParsedFile.getExtractedContents());
    assertTrue(
        initialContent.contains("Source,Modality,Divider,Part,Content"),
        "Should contain CSV headers");
    assertTrue(initialContent.contains("Original content"), "Initial content should match");

    Thread.sleep(1000);
    Files.writeString(testFile.toPath(), "Modified content");

    String modifiedContent = Objects.requireNonNull(SemossParsedFile.getExtractedContents());
    assertTrue(
        modifiedContent.contains("Source,Modality,Divider,Part,Content"),
        "Should contain CSV headers");
    assertTrue(modifiedContent.contains("Modified content"), "Content should reflect file changes");
  }

  @Test
  public void testPdfExtraction() throws IOException {
    File testPdf = tempDir.resolve("test-document.pdf").toFile();
    try (PDDocument doc = new PDDocument()) {
      PDPage page = new PDPage();
      doc.addPage(page);
      try (PDPageContentStream stream = new PDPageContentStream(doc, page)) {
        stream.beginText();
        stream.setFont(new PDType1Font(Standard14Fonts.FontName.HELVETICA), 12);
        stream.newLineAtOffset(50, 700);
        stream.showText("Lorem ipsum dolor sit amet, consectetur adipiscing elit.");
        stream.endText();
      }
      doc.save(testPdf);
    }

    SemossParsedFile SemossParsedFile = new SemossParsedFile(testPdf);

    String extractedContent = Objects.requireNonNull(SemossParsedFile.getExtractedContents());
    assertTrue(
        extractedContent.contains("Source,Modality,Divider,Part,Content"),
        "Should contain CSV headers");
    assertTrue(
        extractedContent.contains("test-document.pdf"), "Should contain PDF filename in metadata");
    assertTrue(SemossParsedFile.isExtractedContentAvailable(), "Content should be available for PDF");
    assertTrue(SemossParsedFile.isSynchronized(), "Should be synchronized after parsing PDF");
    assertNotNull(SemossParsedFile.getExtractionChain(), "Extraction chain should be initialized for PDF");
  }

  private File createTestFile(String filename, String content) throws IOException {
    File file = tempDir.resolve(filename).toFile();
    Files.writeString(file.toPath(), content);
    return file;
  }

  private void deleteDirectoryRecursively(File directory) {
    if (directory.exists()) {
      File[] files = directory.listFiles();
      if (files != null) {
        for (File file : files) {
          if (file.isDirectory()) {
            deleteDirectoryRecursively(file);
          } else {
            file.delete();
          }
        }
      }
      directory.delete();
    }
  }
}