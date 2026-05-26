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
package prerna;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import prerna.util.Constants;
import prerna.util.DIHelper;

import java.beans.PropertyEditor;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public class SemossUnitTest {


    protected static Path tempDir;

    protected static Path semossDir;
    protected static Path insightCacheDir;
    protected static Path projectDir;
    protected static Path vectorDir;

    private static boolean first = false;

    @TempDir
    static Path firstIsTempDirTheRestAreImpostersToNotBeUsed;

    @BeforeAll
    static void setupSemossUnitTest() throws IOException, NoSuchFieldException, IllegalAccessException {
        if (!first) {
            System.out.println("Run Once");
            tempDir = firstIsTempDirTheRestAreImpostersToNotBeUsed;
            first = true;
            // Pointing Semoss to Use this temp directory
        }

        System.out.println("Temp dir: " + tempDir);
        if (!Files.exists(tempDir)) {
            Files.createDirectories(tempDir);
        }
        Properties prop = setupProperties(tempDir);
        resetProperties(prop);
    }

    private static Properties setupProperties(Path tempDir) throws IOException {
        Properties prop = new Properties();

        semossDir = tempDir.resolve("semoss");

        insightCacheDir = semossDir.resolve("insightCache");
        Files.createDirectories(insightCacheDir);

        projectDir = semossDir.resolve("project");
        Files.createDirectories(projectDir);

        vectorDir = semossDir.resolve("vector");
        Files.createDirectories(vectorDir);

        prop.setProperty(Constants.BASE_FOLDER, semossDir.toString());
        prop.setProperty(Constants.INSIGHT_CACHE_DIR, insightCacheDir.toString());
        prop.setProperty(Constants.PROJECT_FOLDER, projectDir.toString());
        prop.setProperty(Constants.VECTOR_FOLDER, vectorDir.toString());

        return prop;
    }

    private static void resetProperties(Properties coreProp) throws IllegalAccessException, NoSuchFieldException {
        Field f = DIHelper.class.getDeclaredField("helper");
        f.setAccessible(true);
        f.set(null, null);

        DIHelper.getInstance().setCoreProp(coreProp);
    }

    @AfterAll
    static void tearDown() throws IOException {
        if (Files.exists(tempDir)) {
            FileUtils.cleanDirectory(tempDir.toFile());
        }
    }

}
