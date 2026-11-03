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
package prerna.reactor.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Properties;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import prerna.SemossUnitTest;
import prerna.util.sql.RdbmsTypeEnum;

/**
 * Runs setUpQuartzProperties against the project quartz.properties, copied into
 * the test base folder the way semosshome.xml packages it for deployment.
 */
class SchedulerFactorySingletonUnitTests extends SemossUnitTest {

	@BeforeAll
	static void copyQuartzProperties() throws IOException {
		Path projectRoot = Paths.get("").toAbsolutePath();
		Path source = projectRoot.resolve("quartz.properties");
		Files.copy(source, semossDir.resolve("quartz.properties"), StandardCopyOption.REPLACE_EXISTING);
	}

	@Test
	void realConnectionDetailsOverwriteThePlaceholders() {
		Properties result = SchedulerFactorySingleton.setUpQuartzProperties(null, "jdbc:h2:mem:realtest", "realuser",
				"realpass", RdbmsTypeEnum.H2_DB);

		assertEquals("jdbc:h2:mem:realtest", result.getProperty("org.quartz.dataSource.myDS.URL"));
		assertEquals("org.h2.Driver", result.getProperty("org.quartz.dataSource.myDS.driver"));
		assertEquals("realuser", result.getProperty("org.quartz.dataSource.myDS.user"));
		assertEquals("realpass", result.getProperty("org.quartz.dataSource.myDS.password"));
		assertEquals("org.quartz.impl.jdbcjobstore.StdJDBCDelegate",
				result.getProperty("org.quartz.jobStore.driverDelegateClass"));
	}

	@Test
	void nonConnectionSettingsStillLoadFromTheFile() {
		Properties result = SchedulerFactorySingleton.setUpQuartzProperties(null, "jdbc:h2:mem:realtest", "realuser",
				"realpass", RdbmsTypeEnum.H2_DB);

		assertEquals("org.quartz.impl.jdbcjobstore.JobStoreTX", result.getProperty("org.quartz.jobStore.class"));
		assertEquals("QRTZ_", result.getProperty("org.quartz.jobStore.tablePrefix"));
	}
}
