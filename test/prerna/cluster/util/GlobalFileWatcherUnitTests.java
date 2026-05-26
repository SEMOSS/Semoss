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
import static org.mockito.Mockito.*;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GlobalFileWatcherUnitTests {

    @TempDir
    Path tempDir;

    private DBSynchronizer dbs;
    private Map<String, Object> pathThreadLock;

    @BeforeEach
    void setUp() throws Exception {
        dbs = mock(DBSynchronizer.class);
        pathThreadLock = new ConcurrentHashMap<>();
        Field ptlField = DBSynchronizer.class.getDeclaredField("pathThreadLock");
        ptlField.setAccessible(true);
        ptlField.set(dbs, pathThreadLock);
    }

    @Test
    void testConstructor_storesSemossHomeAndDbs() {
        GlobalFileWatcher watcher = new GlobalFileWatcher("/some/home", dbs);
        assertEquals("/some/home", watcher.semossHome);
        assertSame(dbs, watcher.dbs);
    }

    @Test
    void testConstructor_nullValues() {
        GlobalFileWatcher watcher = new GlobalFileWatcher(null, null);
        assertNull(watcher.semossHome);
        assertNull(watcher.dbs);
    }

    @Test
    void testProcessNodeChanged_timestampsMatch_doesNotPutToPathThreadLock() throws Exception {
        Path dbDir = tempDir.resolve("db").resolve("myengine");
        Files.createDirectories(dbDir);
        Path testFile = dbDir.resolve("data.mosfet");
        Files.writeString(testFile, "test content");
        BasicFileAttributes attr = Files.readAttributes(testFile, BasicFileAttributes.class);
        String lastAccessTime = attr.lastAccessTime().toString();
        String semossHome = tempDir.toString().replace("\\", "/");
        GlobalFileWatcher watcher = new GlobalFileWatcher(semossHome, dbs);
        watcher.processNodeChanged("/myengine/data.mosfet", lastAccessTime);
        assertTrue(pathThreadLock.isEmpty(), "pathThreadLock should be empty when timestamps match");
    }

    @Test
    void testProcessNodeChanged_timestampsMismatch_pathEndsMosfet_putsParent() throws Exception {
        Path dbDir = tempDir.resolve("db").resolve("engine1");
        Files.createDirectories(dbDir);
        Path testFile = dbDir.resolve("insight.mosfet");
        Files.writeString(testFile, "content");
        String semossHome = tempDir.toString().replace("\\", "/");
        GlobalFileWatcher watcher = new GlobalFileWatcher(semossHome, dbs);
        watcher.processNodeChanged("/engine1/insight.mosfet", "MISMATCHED_TIMESTAMP");
        String folder = semossHome + "/db/engine1/insight.mosfet";
        File file = new File(folder);
        String parent = file.getParent();
        assertTrue(pathThreadLock.containsKey(parent),
                "pathThreadLock should contain the parent path for mosfet files");
        assertFalse(pathThreadLock.containsKey(folder),
                "pathThreadLock should not contain the folder path after removal");
    }

    @Test
    void testProcessNodeChanged_timestampsMismatch_nonMosfet_putsThenRemovesFolder() throws Exception {
        Path dbDir = tempDir.resolve("db").resolve("engine2");
        Files.createDirectories(dbDir);
        Path testFile = dbDir.resolve("data.db");
        Files.writeString(testFile, "db content");
        String semossHome = tempDir.toString().replace("\\", "/");
        GlobalFileWatcher watcher = new GlobalFileWatcher(semossHome, dbs);
        watcher.processNodeChanged("/engine2/data.db", "WRONG_TIMESTAMP");
        String folder = semossHome + "/db/engine2/data.db";
        assertFalse(pathThreadLock.containsKey(folder),
                "folder should be removed after put-then-remove");
    }

    @Test
    void testProcessNodeChanged_fileDoesNotExist_entersElseBranch() throws Exception {
        String semossHome = tempDir.toString().replace("\\", "/");
        GlobalFileWatcher watcher = new GlobalFileWatcher(semossHome, dbs);
        watcher.processNodeChanged("/nonexistent/file.db", "some_payload");
        String folder = semossHome + "/db/nonexistent/file.db";
        assertFalse(pathThreadLock.containsKey(folder),
                "folder removed after else branch");
    }

    @Test
    void testProcessNodeChanged_fileDoesNotExist_mosfetPath_putsParent() throws Exception {
        String semossHome = tempDir.toString().replace("\\", "/");
        GlobalFileWatcher watcher = new GlobalFileWatcher(semossHome, dbs);
        watcher.processNodeChanged("/missing/insight.mosfet", "payload");
        String folder = semossHome + "/db/missing/insight.mosfet";
        File file = new File(folder);
        String parent = file.getParent();
        assertTrue(pathThreadLock.containsKey(parent),
                "parent should be in pathThreadLock for mosfet");
        assertFalse(pathThreadLock.containsKey(folder),
                "folder should not be in pathThreadLock");
    }

    @Test
    void testProcessNodeDeleted_noException() {
        GlobalFileWatcher watcher = new GlobalFileWatcher("/opt/semoss", dbs);
        watcher.processNodeDeleted("/engine/data.db");
        assertNotNull(watcher.semossHome);
    }

    @Test
    void testProcessNodeChanged_nestedStructure_inSync() throws Exception {
        Path dbDir = tempDir.resolve("db").resolve("mydb").resolve("version");
        Files.createDirectories(dbDir);
        Path versionFile = dbDir.resolve("schema.json");
        Files.writeString(versionFile, "{}");
        BasicFileAttributes attr = Files.readAttributes(versionFile, BasicFileAttributes.class);
        String accessTime = attr.lastAccessTime().toString();
        String semossHome = tempDir.toString().replace("\\", "/");
        GlobalFileWatcher watcher = new GlobalFileWatcher(semossHome, dbs);
        watcher.processNodeChanged("/mydb/version/schema.json", accessTime);
        assertTrue(pathThreadLock.isEmpty(), "No entries when in sync");
    }

    @Test
    void testProcessNodeChanged_mosfetPath_parentDistinctFromFolder() throws Exception {
        Path dbDir = tempDir.resolve("db").resolve("deep").resolve("nested");
        Files.createDirectories(dbDir);
        Path mosfetFile = dbDir.resolve("insight.mosfet");
        Files.writeString(mosfetFile, "mosfet data");
        String semossHome = tempDir.toString().replace("\\", "/");
        GlobalFileWatcher watcher = new GlobalFileWatcher(semossHome, dbs);
        watcher.processNodeChanged("/deep/nested/insight.mosfet", "WRONG");
        String folder = semossHome + "/db/deep/nested/insight.mosfet";
        File file = new File(folder);
        String parent = file.getParent();
        assertNotEquals(parent, folder);
        assertTrue(pathThreadLock.containsKey(parent));
        assertFalse(pathThreadLock.containsKey(folder));
    }
}
