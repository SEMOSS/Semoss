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

import prerna.engine.impl.vector.VectorDatabaseCSVWriter;
import prerna.reactor.frame.gaas.processors.FileHandlerChain;
import prerna.sablecc2.om.execptions.SemossPixelException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Objects;

/**
 * Represents a "Bring Your Own Document" file that manages the relationship between an original
 * document and its extracted/processed content.
 *
 * <p>This class extends {@link File} to provide additional functionality for:
 *
 * <ul>
 *   <li>Managing extracted content files in a synchronized manner
 *   <li>Tracking metadata and synchronization status between original and extracted files
 *   <li>Handling file lifecycle operations (rename, delete, update)
 * </ul>
 *
 * <p>The extracted content is stored in an "extracted/" subdirectory relative to the original
 * file's parent directory. Metadata is maintained through properties files that contain hashed
 * timestamps to detect when files are out of sync.
 */
public class SemossParsedFile extends File {

  private static final long serialVersionUID = 1L;
  private static final String EXTRACTED_DIR = ".extracted";
  private static final String EXTRACTED_CONTENT_SUFFIX = ".txt";
  private static final FileHandlerChain DEFAULT_EXTRACTION_CHAIN = FileHandlerChain.getCoreHandlerChain();

  private File extractedContentFile;
  private final FileHandlerChain extractionChain;

  /**
   * Creates a new SemossParsedFile instance with automatic parsing.
   *
   * @param pathname the pathname string for the original file
   * @throws IOException if an error occurs during initialization or parsing
   */
  public SemossParsedFile(String pathname) throws IOException {
    this(pathname, DEFAULT_EXTRACTION_CHAIN);
  }

  /**
   * Creates a new SemossParsedFile instance from an existing File with automatic parsing.
   *
   * @param file the existing file to wrap
   * @throws IOException if an error occurs during initialization or parsing
   */
  public SemossParsedFile(File file) throws IOException {
    this(file.getAbsolutePath(), DEFAULT_EXTRACTION_CHAIN);
  }

  /**
   * Creates a new SemossParsedFile instance from an existing File with a custom extraction chain.
   *
   * @param file the existing file to wrap
   * @param handlerChain custom file handler chain for extraction
   * @throws IOException if an error occurs during initialization or parsing
   */
  public SemossParsedFile(File file, FileHandlerChain handlerChain) throws IOException {
    this(file.getAbsolutePath(), handlerChain);
  }

  /**
   * Creates a new SemossParsedFile instance with a custom extraction chain.
   *
   * @param pathname the pathname string for the original file
   * @param handlerChain custom file handler chain for extraction
   * @throws IOException if an error occurs during initialization or parsing
   */
  public SemossParsedFile(String pathname, FileHandlerChain handlerChain)
      throws IOException {
    super(pathname);
    this.extractionChain =
        Objects.requireNonNull(handlerChain, "FileHandlerChain cannot be null");
    initialize();
  }

  /**
   * Initializes the SemossParsedFile by setting up paths, extraction chain, and performing automatic
   * parsing if needed.
   *
   * @throws IOException if an error occurs during initialization or parsing
   */
  private void initialize() throws IOException {
    initializeExtractedFilePaths();

    // Perform automatic parsing if needed
    if (this.exists() && shouldParseFile()) {
      parseFile();
    }
  }

  /** Initializes the paths for extracted content file. */
  private void initializeExtractedFilePaths() {
    File parentDir = this.getParentFile();
    if (parentDir == null) {
      parentDir = new File(".");
    }

    File extractedDir = new File(parentDir, EXTRACTED_DIR);
    String baseName = getBaseName(this.getName());

    this.extractedContentFile = new File(extractedDir, baseName + EXTRACTED_CONTENT_SUFFIX);
  }

  /**
   * Gets the extracted content as a string. If content is not available or out of sync, attempts to
   * parse the file automatically.
   *
   * @return the extracted content, or null if parsing fails
   * @throws IOException if an error occurs reading or parsing the content
   */
  public String getExtractedContents() throws IOException {
    // Use unified predicate for deciding whether to parse
    if (shouldParseFile()) {
      if (this.exists()) {
        parseFile();
      } else {
        return null; // Cannot parse non-existent file
      }
    }

    // After potential parsing, ensure availability
    if (!isExtractedContentAvailable()) {
      return null;
    }

    return Files.readString(extractedContentFile.toPath());
  }

  /**
   * Sets the extracted content and synchronizes the last modified timestamp.
   *
   * @param content the extracted content to save
   * @throws IOException if an error occurs writing the content
   */
  public void setExtractedContents(String content) throws IOException {
    ensureExtractedDirectoryExists();

    // Write the content
    Files.writeString(extractedContentFile.toPath(), content);

    // Synchronize the last modified timestamp
    if (this.exists()) {
      extractedContentFile.setLastModified(this.lastModified());
    }
  }

  /**
   * Gets the path to the extracted content file.
   *
   * @return the absolute path to the extracted content file
   */
  public String getExtractedContentsFilePath() {
    return extractedContentFile.getAbsolutePath();
  }

  /**
   * Gets the extracted content file.
   *
   * @return the extracted content File object
   */
  public File getExtractedContentFile() {
    return extractedContentFile;
  }

  /**
   * Checks if extracted content is available.
   *
   * @return true if the extracted content file exists and is readable
   */
  public boolean isExtractedContentAvailable() {
    return extractedContentFile != null
        && extractedContentFile.exists()
        && extractedContentFile.canRead();
  }

  /**
   * Checks if the original file and extracted content are synchronized.
   *
   * @return true if both files exist and have the same last modified timestamp, false otherwise
   */
  public boolean isSynchronized() {
    if (!this.exists() || !isExtractedContentAvailable()) {
      return false;
    }

    return this.lastModified() == extractedContentFile.lastModified();
  }

  /**
   * Forces synchronization by setting the extracted file's timestamp to match the source file.
   *
   * @throws IOException if an error occurs updating the timestamp
   */
  public void synchronize() throws IOException {
    if (this.exists() && isExtractedContentAvailable()) {
      extractedContentFile.setLastModified(this.lastModified());
    }
  }

  /**
   * Handles file rename operations by updating the extracted file paths and moving files if
   * necessary.
   *
   * @param newFile the new file location
   * @return true if the rename was successful
   */
  public boolean renameTo(File newFile) {
    boolean originalRenamed = super.renameTo(newFile);

    if (originalRenamed) {
      try {
        // Create new SemossParsedFile to get correct paths (without auto-parsing since files
        // are moving)
        File parentDir = newFile.getParentFile();
        if (parentDir == null) {
          parentDir = new File(".");
        }

        File newExtractedDir = new File(parentDir, EXTRACTED_DIR);
        String newBaseName = getBaseName(newFile.getName());

        File newExtractedContentFile =
            new File(newExtractedDir, newBaseName + EXTRACTED_CONTENT_SUFFIX);

        // Move extracted content if it exists
        if (isExtractedContentAvailable()) {
          if (!newExtractedDir.exists()) {
            newExtractedDir.mkdirs();
          }
          Files.move(
              extractedContentFile.toPath(),
              newExtractedContentFile.toPath(),
              StandardCopyOption.REPLACE_EXISTING);
        }

        // Update our internal paths
        this.extractedContentFile = newExtractedContentFile;

      } catch (IOException e) {
        // Log error but don't fail the rename operation
        System.err.println(
            "Warning: Failed to move extracted content during rename: " + e.getMessage());
      }
    }

    return originalRenamed;
  }

  /**
   * Deletes the file and its associated extracted content.
   *
   * @return true if the deletion was successful
   */
  @Override
  public boolean delete() {
    boolean deleted = super.delete();

    if (deleted) {
      // Clean up extracted content
      try {
        if (extractedContentFile.exists()) {
          Files.delete(extractedContentFile.toPath());
        }

        // Try to remove the extracted directory if it's empty
        File extractedDir = extractedContentFile.getParentFile();
        if (extractedDir.exists() && extractedDir.isDirectory()) {
          String[] files = extractedDir.list();
          if (files != null && files.length == 0) {
            extractedDir.delete();
          }
        }
      } catch (IOException e) {
        // Log error but don't fail the delete operation
        System.err.println(
            "Warning: Failed to clean up extracted content during delete: " + e.getMessage());
      }
    }

    return deleted;
  }

  /**
   * Ensures the extracted directory exists.
   *
   * @throws IOException if the directory cannot be created
   */
  private void ensureExtractedDirectoryExists() throws IOException {
    File extractedDir = extractedContentFile.getParentFile();
    if (!extractedDir.exists()) {
      if (!extractedDir.mkdirs()) {
        throw new SemossPixelException(
            new IOException(
                "Failed to create extracted directory: " + extractedDir.getAbsolutePath()));
      }
    }
  }

  /**
   * Determines whether the file should be parsed based on synchronization status.
   *
   * @return true if the file should be parsed (doesn't exist or is out of sync)
   */
  private boolean shouldParseFile() {
    return !isExtractedContentAvailable() || !isSynchronized();
  }

  /**
   * Parses the file using the extraction chain to generate extracted content.
   *
   * @throws IOException if an error occurs during parsing
   */
  private void parseFile() throws IOException {
    if (!this.exists()) {
      throw new SemossPixelException(
          new IOException("Cannot parse non-existent file: " + this.getAbsolutePath()));
    }

    try {
      ensureExtractedDirectoryExists();

      // Create writer in the source file's sibling extracted directory.
      try (VectorDatabaseCSVWriter csvWriter =
          new VectorDatabaseCSVWriter(extractedContentFile.getAbsolutePath())) {
        int rowsProcessed;

        if (extractionChain.supportsFile(this.getAbsoluteFile())) {
          rowsProcessed = extractionChain.process(this, csvWriter);
        } else {
          // Fallback: treat the file as plain text and write its content directly.
          String rawContent = Files.readString(this.toPath());
          csvWriter.writeRow(this.getName(), "1", rawContent);
          rowsProcessed = 1;
        }

        // If processing was successful, synchronize the timestamp
        if (rowsProcessed >= 0 && extractedContentFile.exists()) {
          extractedContentFile.setLastModified(this.lastModified());
        }
      }
    } catch (Exception e) {
      throw new SemossPixelException(
          new IOException("Failed to parse file: " + this.getAbsolutePath(), e));
    }
  }

  /**
   * Forces re-parsing of the file, even if it's already synchronized.
   *
   * @throws IOException if an error occurs during parsing
   */
  public void forceReparse() throws IOException {
    parseFile();
  }

  /**
   * Gets the extraction chain used by this SemossParsedFile.
   *
   * @return the FileHandlerChain
   */
  public FileHandlerChain getExtractionChain() {
    return extractionChain;
  }

  /**
   * Extracts the base name from a filename (without extension).
   *
   * @param fileName the full filename
   * @return the base name without extension
   */
  private String getBaseName(String fileName) {
    int lastDot = fileName.lastIndexOf('.');
    return lastDot > 0 ? fileName.substring(0, lastDot) : fileName;
  }
}