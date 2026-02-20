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
package prerna.cluster.util;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LocalFileWatcherUnitTests {

    private DBSynchronizer mockDbs;
    private LocalFileWatcher watcher;

    @BeforeEach
    void setUp() {
        mockDbs = mock(DBSynchronizer.class);
        watcher = new LocalFileWatcher(mockDbs, "testDb");
    }

    @AfterEach
    void tearDown() throws Exception {
        closeWatchService(watcher);
    }

    @Test
    void testConstructorSetsDbsField() throws Exception {
        Field dbsField = LocalFileWatcher.class.getDeclaredField("dbs");
        dbsField.setAccessible(true);
        assertSame(mockDbs, dbsField.get(watcher));
    }

    @Test
    void testConstructorSetsDbField() throws Exception {
        Field dbField = LocalFileWatcher.class.getDeclaredField("db");
        dbField.setAccessible(true);
        assertEquals("testDb", dbField.get(watcher));
    }

    @Test
    void testConstructorCreatesWatchService() throws Exception {
        Field wsField = LocalFileWatcher.class.getDeclaredField("ws");
        wsField.setAccessible(true);
        assertNotNull(wsField.get(watcher));
    }

    @Test
    void testConstructorWithDifferentDbName() throws Exception {
        LocalFileWatcher w2 = new LocalFileWatcher(mockDbs, "anotherDb");
        try {
            Field dbField = LocalFileWatcher.class.getDeclaredField("db");
            dbField.setAccessible(true);
            assertEquals("anotherDb", dbField.get(w2));
        } finally { closeWatchService(w2); }
    }

    @Test
    void testConstructorWithEmptyDbName() throws Exception {
        LocalFileWatcher w2 = new LocalFileWatcher(mockDbs, "");
        try {
            Field dbField = LocalFileWatcher.class.getDeclaredField("db");
            dbField.setAccessible(true);
            assertEquals("", dbField.get(w2));
        } finally { closeWatchService(w2); }
    }

    @Test
    void testConstructorWithDifferentDbs() throws Exception {
        DBSynchronizer otherMock = mock(DBSynchronizer.class);
        LocalFileWatcher w2 = new LocalFileWatcher(otherMock, "db2");
        try {
            Field dbsField = LocalFileWatcher.class.getDeclaredField("dbs");
            dbsField.setAccessible(true);
            assertSame(otherMock, dbsField.get(w2));
        } finally { closeWatchService(w2); }
    }

    @Test
    void testEachConstructorCreatesUniqueWatchService() throws Exception {
        LocalFileWatcher w2 = new LocalFileWatcher(mockDbs, "db2");
        try {
            Field wsField = LocalFileWatcher.class.getDeclaredField("ws");
            wsField.setAccessible(true);
            assertNotSame(wsField.get(watcher), wsField.get(w2));
        } finally { closeWatchService(w2); }
    }

    @Test
    void testDefaultFileExtensionsValue() throws Exception {
        assertEquals("py;python;r;js;css;mosfet;json;", getFileExtensions());
    }

    @Test
    void testFileExtensionsIncludesPy() throws Exception { assertTrue(getFileExtensions().contains("py;")); }

    @Test
    void testFileExtensionsIncludesPython() throws Exception { assertTrue(getFileExtensions().contains("python;")); }

    @Test
    void testFileExtensionsIncludesR() throws Exception { assertTrue(getFileExtensions().contains("r;")); }

    @Test
    void testFileExtensionsIncludesJs() throws Exception { assertTrue(getFileExtensions().contains("js;")); }

    @Test
    void testFileExtensionsIncludesCss() throws Exception { assertTrue(getFileExtensions().contains("css;")); }

    @Test
    void testFileExtensionsIncludesMosfet() throws Exception { assertTrue(getFileExtensions().contains("mosfet;")); }

    @Test
    void testFileExtensionsIncludesJson() throws Exception { assertTrue(getFileExtensions().contains("json;")); }

    @Test
    void testFileExtensionsDoesNotIncludeJava() throws Exception { assertFalse(getFileExtensions().contains("java")); }

    @Test
    void testFileExtensionsEndsWithSemicolon() throws Exception { assertTrue(getFileExtensions().endsWith(";")); }

    @Test
    void testFileExtensionsHasSevenEntries() throws Exception { assertEquals(7, getFileExtensions().split(";").length); }

    @Test
    void testWatchPathRegistersDirectory(@TempDir Path tempDir) {
        assertDoesNotThrow(() -> watcher.watchPath(tempDir.toString()));
    }

    @Test
    void testWatchPathRegistersSubdirectory(@TempDir Path tempDir) throws Exception {
        Path subDir = tempDir.resolve("subdir");
        Files.createDirectories(subDir);
        assertDoesNotThrow(() -> watcher.watchPath(subDir.toString()));
    }

    @Test
    void testWatchPathWithMultipleDirectories(@TempDir Path tempDir) throws Exception {
        Path d1 = tempDir.resolve("d1"); Path d2 = tempDir.resolve("d2");
        Files.createDirectories(d1); Files.createDirectories(d2);
        assertDoesNotThrow(() -> { watcher.watchPath(d1.toString()); watcher.watchPath(d2.toString()); });
    }

    @Test
    void testWatchPathWithNonExistentDirectory() {
        assertDoesNotThrow(() -> watcher.watchPath("/nonexistent/path/xyz"));
    }

    @Test
    void testWatchFilePathRegistersDirectory(@TempDir Path tempDir) {
        assertDoesNotThrow(() -> watcher.watchFilePath(tempDir.toString()));
    }

    @Test
    void testWatchFilePathRegistersSubdirectory(@TempDir Path tempDir) throws Exception {
        Path subDir = tempDir.resolve("filesub");
        Files.createDirectories(subDir);
        assertDoesNotThrow(() -> watcher.watchFilePath(subDir.toString()));
    }

    @Test
    void testWatchFilePathWithMultipleDirectories(@TempDir Path tempDir) throws Exception {
        Path d1 = tempDir.resolve("fd1"); Path d2 = tempDir.resolve("fd2");
        Files.createDirectories(d1); Files.createDirectories(d2);
        assertDoesNotThrow(() -> { watcher.watchFilePath(d1.toString()); watcher.watchFilePath(d2.toString()); });
    }

    @Test
    void testWatchFilePathWithNonExistentPath() {
        assertDoesNotThrow(() -> watcher.watchFilePath("/nonexistent/path/xyz"));
    }

    @Test
    void testImplementsRunnable() {
        assertInstanceOf(Runnable.class, watcher);
    }

    @Test
    void testWatchPathAndWatchFilePathOnSameDir(@TempDir Path tempDir) throws Exception {
        Path dir = tempDir.resolve("combined"); Files.createDirectories(dir);
        assertDoesNotThrow(() -> { watcher.watchPath(dir.toString()); watcher.watchFilePath(dir.toString()); });
    }

    @Test
    void testWatchPathAndWatchFilePathOnDifferentDirs(@TempDir Path tempDir) throws Exception {
        Path d1 = tempDir.resolve("watchDir"); Path d2 = tempDir.resolve("fileWatchDir");
        Files.createDirectories(d1); Files.createDirectories(d2);
        assertDoesNotThrow(() -> { watcher.watchPath(d1.toString()); watcher.watchFilePath(d2.toString()); });
    }

    private String getFileExtensions() throws Exception {
        Field feField = LocalFileWatcher.class.getDeclaredField("fileExtensions");
        feField.setAccessible(true);
        return (String) feField.get(watcher);
    }

    private void closeWatchService(LocalFileWatcher w) throws Exception {
        Field wsField = LocalFileWatcher.class.getDeclaredField("ws");
        wsField.setAccessible(true);
        WatchService ws = (WatchService) wsField.get(w);
        if (ws != null) { ws.close(); }
    }
}
