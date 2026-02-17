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

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DBSynchronizerUnitTests {

    private DBSynchronizer instance;

    @BeforeEach
    void setUp() throws Exception {
        Constructor<DBSynchronizer> ctor = DBSynchronizer.class.getDeclaredConstructor();
        ctor.setAccessible(true);
        instance = ctor.newInstance();
    }

    @Test
    void testConstructorCreatesNonNullInstance() {
        assertNotNull(instance);
    }

    @Test
    void testExtendsZKClient() {
        assertInstanceOf(ZKClient.class, instance);
    }

    @Test
    void testSuperclassIsZKClient() {
        assertEquals(ZKClient.class, DBSynchronizer.class.getSuperclass());
    }

    @Test
    void testSemossHomeDefaultIsNull() {
        assertNull(instance.semossHome);
    }

    @Test
    void testNamespaceDefaultIsEmptyString() throws Exception {
        Field nsField = DBSynchronizer.class.getDeclaredField("namespace");
        nsField.setAccessible(true);
        assertEquals("", nsField.get(instance));
    }

    @Test
    void testFileExtensionsDefaultValue() throws Exception {
        Field feField = DBSynchronizer.class.getDeclaredField("fileExtensions");
        feField.setAccessible(true);
        assertEquals("py;python;r;js;css;mosfet;json;java;", feField.get(instance));
    }

    @Test
    void testFileExtensionsIncludesJava() throws Exception {
        Field feField = DBSynchronizer.class.getDeclaredField("fileExtensions");
        feField.setAccessible(true);
        assertTrue(((String) feField.get(instance)).contains("java;"));
    }

    @Test
    void testFileExtensionsHasEightEntries() throws Exception {
        Field feField = DBSynchronizer.class.getDeclaredField("fileExtensions");
        feField.setAccessible(true);
        assertEquals(8, ((String) feField.get(instance)).split(";").length);
    }

    @SuppressWarnings("unchecked")
    @Test
    void testDbLockMapIsNonNullAndEmpty() throws Exception {
        Field f = DBSynchronizer.class.getDeclaredField("dbLock");
        f.setAccessible(true);
        Map<String, InterProcessLock> m = (Map<String, InterProcessLock>) f.get(instance);
        assertNotNull(m);
        assertTrue(m.isEmpty());
    }

    @SuppressWarnings("unchecked")
    @Test
    void testPathThreadLockMapIsNonNullAndEmpty() throws Exception {
        Field f = DBSynchronizer.class.getDeclaredField("pathThreadLock");
        f.setAccessible(true);
        Map<String, Object> m = (Map<String, Object>) f.get(instance);
        assertNotNull(m);
        assertTrue(m.isEmpty());
    }

    @SuppressWarnings("unchecked")
    @Test
    void testDbWatcherMapIsNonNullAndEmpty() throws Exception {
        Field f = DBSynchronizer.class.getDeclaredField("dbWatcher");
        f.setAccessible(true);
        Map<String, LocalFileWatcher> m = (Map<String, LocalFileWatcher>) f.get(instance);
        assertNotNull(m);
        assertTrue(m.isEmpty());
    }

    @SuppressWarnings("unchecked")
    @Test
    void testDbListenerMapIsNonNullAndEmpty() throws Exception {
        Field f = DBSynchronizer.class.getDeclaredField("dbListener");
        f.setAccessible(true);
        Map<String, CuratorCacheListener> m = (Map<String, CuratorCacheListener>) f.get(instance);
        assertNotNull(m);
        assertTrue(m.isEmpty());
    }

    @SuppressWarnings("unchecked")
    @Test
    void testDbVersionMapIsNonNullAndEmpty() throws Exception {
        Field f = DBSynchronizer.class.getDeclaredField("dbVersion");
        f.setAccessible(true);
        Map<String, String> m = (Map<String, String>) f.get(instance);
        assertNotNull(m);
        assertTrue(m.isEmpty());
    }

    @Test
    void testGetIntraProcessLockReturnsNullForUnknownPath() {
        assertNull(instance.getIntraProcessLock("unknown/path"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void testGetIntraProcessLockReturnsObjectAfterPut() throws Exception {
        Field f = DBSynchronizer.class.getDeclaredField("pathThreadLock");
        f.setAccessible(true);
        Map<String, Object> ptl = (Map<String, Object>) f.get(instance);
        Object lockObj = new Object();
        ptl.put("my/path", lockObj);
        assertSame(lockObj, instance.getIntraProcessLock("my/path"));
    }

    @Test
    void testGetIntraProcessLockReturnsNullForEmptyString() {
        assertNull(instance.getIntraProcessLock(""));
    }

    @Test
    void testGetIntraProcessLockReturnsNullForNull() {
        assertNull(instance.getIntraProcessLock(null));
    }

    @SuppressWarnings("unchecked")
    @Test
    void testGetIntraProcessLockReturnsDifferentObjects() throws Exception {
        Field f = DBSynchronizer.class.getDeclaredField("pathThreadLock");
        f.setAccessible(true);
        Map<String, Object> ptl = (Map<String, Object>) f.get(instance);
        Object l1 = new Object(); Object l2 = new Object();
        ptl.put("path1", l1); ptl.put("path2", l2);
        assertSame(l1, instance.getIntraProcessLock("path1"));
        assertSame(l2, instance.getIntraProcessLock("path2"));
        assertNotSame(l1, l2);
    }

    @SuppressWarnings("unchecked")
    @Test
    void testGetIntraProcessLockReturnsSameObjectOnRepeatedCalls() throws Exception {
        Field f = DBSynchronizer.class.getDeclaredField("pathThreadLock");
        f.setAccessible(true);
        Map<String, Object> ptl = (Map<String, Object>) f.get(instance);
        ptl.put("stable", new Object());
        assertSame(instance.getIntraProcessLock("stable"), instance.getIntraProcessLock("stable"));
    }

    private boolean invokeIsDirectoryAllowed(File dir) throws Exception {
        Method m = DBSynchronizer.class.getDeclaredMethod("isDirectoryAllowed", File.class);
        m.setAccessible(true);
        return (boolean) m.invoke(instance, dir);
    }

    @Test void testIsDirectoryAllowed_Temp() throws Exception { assertFalse(invokeIsDirectoryAllowed(new File("Temp"))); }
    @Test void testIsDirectoryAllowed_temp() throws Exception { assertFalse(invokeIsDirectoryAllowed(new File("temp"))); }
    @Test void testIsDirectoryAllowed_TEMP() throws Exception { assertFalse(invokeIsDirectoryAllowed(new File("TEMP"))); }
    @Test void testIsDirectoryAllowed_tEmP() throws Exception { assertFalse(invokeIsDirectoryAllowed(new File("tEmP"))); }
    @Test void testIsDirectoryAllowed_DotGit() throws Exception { assertFalse(invokeIsDirectoryAllowed(new File(".git"))); }
    @Test void testIsDirectoryAllowed_DotGitignore() throws Exception { assertFalse(invokeIsDirectoryAllowed(new File(".gitignore"))); }
    @Test void testIsDirectoryAllowed_DotGitmodules() throws Exception { assertFalse(invokeIsDirectoryAllowed(new File(".gitmodules"))); }
    @Test void testIsDirectoryAllowed_DotGitattributes() throws Exception { assertFalse(invokeIsDirectoryAllowed(new File(".gitattributes"))); }
    @Test void testIsDirectoryAllowed_Classes() throws Exception { assertFalse(invokeIsDirectoryAllowed(new File("classes"))); }
    @Test void testIsDirectoryAllowed_ClassesBin() throws Exception { assertFalse(invokeIsDirectoryAllowed(new File("classesBin"))); }
    @Test void testIsDirectoryAllowed_ClassesOutput() throws Exception { assertFalse(invokeIsDirectoryAllowed(new File("classesOutput"))); }
    @Test void testIsDirectoryAllowed_Src() throws Exception { assertTrue(invokeIsDirectoryAllowed(new File("src"))); }
    @Test void testIsDirectoryAllowed_NormalDir() throws Exception { assertTrue(invokeIsDirectoryAllowed(new File("normalDir"))); }
    @Test void testIsDirectoryAllowed_Data() throws Exception { assertTrue(invokeIsDirectoryAllowed(new File("data"))); }
    @Test void testIsDirectoryAllowed_Lib() throws Exception { assertTrue(invokeIsDirectoryAllowed(new File("lib"))); }
    @Test void testIsDirectoryAllowed_Scripts() throws Exception { assertTrue(invokeIsDirectoryAllowed(new File("scripts"))); }
    @Test void testIsDirectoryAllowed_DotVscode() throws Exception { assertTrue(invokeIsDirectoryAllowed(new File(".vscode"))); }
    @Test void testIsDirectoryAllowed_DotEclipse() throws Exception { assertTrue(invokeIsDirectoryAllowed(new File(".eclipse"))); }
    @Test void testIsDirectoryAllowed_TempFiles() throws Exception { assertTrue(invokeIsDirectoryAllowed(new File("TempFiles"))); }
    @Test void testIsDirectoryAllowed_Temporary() throws Exception { assertTrue(invokeIsDirectoryAllowed(new File("temporary"))); }

    private String invokeGetNodeName(String input) throws Exception {
        Method m = DBSynchronizer.class.getDeclaredMethod("getNodeName", String.class);
        m.setAccessible(true);
        return (String) m.invoke(instance, input);
    }

    @Test
    void testGetNodeNameReplacesBackslashes() throws Exception {
        instance.semossHome = "c:/workspace/Semoss_Dev";
        assertEquals("/mydb/file.txt", invokeGetNodeName("c:\\workspace\\Semoss_Dev\\db\\mydb\\file.txt"));
    }

    @Test
    void testGetNodeNameWithForwardSlashes() throws Exception {
        instance.semossHome = "c:/workspace/Semoss_Dev";
        assertEquals("/mydb/file.txt", invokeGetNodeName("c:/workspace/Semoss_Dev/db/mydb/file.txt"));
    }

    @Test
    void testGetNodeNameWithDeepPath() throws Exception {
        instance.semossHome = "/opt/semoss";
        assertEquals("/engine/v1/scripts/main.py", invokeGetNodeName("/opt/semoss/db/engine/v1/scripts/main.py"));
    }

    @Test
    void testGetNodeNameWithMixedSlashes() throws Exception {
        instance.semossHome = "c:/workspace/Semoss_Dev";
        assertEquals("/mydb/subdir/file.r", invokeGetNodeName("c:\\workspace\\Semoss_Dev/db/mydb\\subdir/file.r"));
    }

    private String invokeGetLastModifiedTime(String path) throws Exception {
        Method m = DBSynchronizer.class.getDeclaredMethod("getLastModifiedTime", String.class);
        m.setAccessible(true);
        return (String) m.invoke(instance, path);
    }

    @Test
    void testGetLastModifiedTimeWithRealFile(@TempDir Path tempDir) throws Exception {
        Path file = tempDir.resolve("testfile.txt");
        Files.writeString(file, "content");
        String result = invokeGetLastModifiedTime(file.toString());
        assertNotNull(result);
    }

    @Test
    void testGetLastModifiedTimeReturnsIsoFormattedString(@TempDir Path tempDir) throws Exception {
        Path file = tempDir.resolve("check.txt");
        Files.writeString(file, "data");
        String result = invokeGetLastModifiedTime(file.toString());
        assertNotNull(result);
        assertTrue(result.contains("T"), "Result should be ISO-8601 formatted");
    }

    @Test
    void testGetLastModifiedTimeWithDirectory(@TempDir Path tempDir) throws Exception {
        Path dir = tempDir.resolve("subdir");
        Files.createDirectories(dir);
        assertNotNull(invokeGetLastModifiedTime(dir.toString()));
    }

    @Test
    void testGetLastModifiedTimeReturnsNullForNonexistentFile() throws Exception {
        assertNull(invokeGetLastModifiedTime("/nonexistent/path/file.txt"));
    }

    @Test
    void testGetLastModifiedTimeForEmptyFile(@TempDir Path tempDir) throws Exception {
        Path file = tempDir.resolve("empty.txt");
        Files.writeString(file, "");
        assertNotNull(invokeGetLastModifiedTime(file.toString()));
    }

    @Test
    void testUnregisterDBMethodExists() throws Exception {
        Method m = DBSynchronizer.class.getDeclaredMethod("unregisterDB", String.class);
        assertNotNull(m);
    }

    @Test
    void testUnregisterDBDoesNotThrowWithoutLock() {
        assertDoesNotThrow(() -> instance.unregisterDB("testDb"));
    }

    @Test
    void testModifyNodeDoesNotThrowWithoutLock() {
        assertDoesNotThrow(() -> instance.modifyNode("db1", "file.txt"));
    }

    @Test
    void testCreateNodeDoesNotThrowWithoutLock() {
        assertDoesNotThrow(() -> instance.createNode("db1", "file.txt"));
    }

    @Test
    void testDeleteNodeDoesNotThrowWithoutLock() {
        assertDoesNotThrow(() -> instance.deleteNode("db1", "file.txt"));
    }

    @Test
    void testSemossHomeCanBeSetDirectly() {
        instance.semossHome = "/custom/path";
        assertEquals("/custom/path", instance.semossHome);
    }

    @Test
    void testSemossHomeCanBeSetToNull() {
        instance.semossHome = "something";
        instance.semossHome = null;
        assertNull(instance.semossHome);
    }

    @Test
    void testConstructorIsPrivate() throws Exception {
        Constructor<DBSynchronizer> ctor = DBSynchronizer.class.getDeclaredConstructor();
        assertFalse(ctor.canAccess(null));
    }
}
