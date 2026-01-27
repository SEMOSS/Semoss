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
package prerna.junit.pixel;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class PixelUnitTestsUser extends PixelUnit {

	protected static final String USER_TESTS_CSV = Paths.get(TEST_RESOURCES_DIRECTORY, "user_tests.csv").toAbsolutePath().toString();
	
	// Needed for parameterized tests
	private String name;
	private String pixel;
	private String expectedJson;
	private boolean compareAll;
	private List<String> excludePaths;
	private boolean ignoreOrder;
	private boolean ignoreAddedDictionary;
	private boolean ignoreAddedIterable;
	private List<String> cleanTestDatabases;
	private boolean ignoreFailure;
	
	public PixelUnitTestsUser(String name, String pixel, String expectedJson, boolean compareAll, List<String> excludePaths, boolean ignoreOrder, boolean ignoreAddedDictionary, boolean ignoreAddedIterable, List<String> cleanTestDatabases, boolean ignoreFailure) {
		this.name = name;
		this.pixel = pixel;
		this.expectedJson = expectedJson;
		this.compareAll = compareAll;
		this.excludePaths = excludePaths;
		this.ignoreOrder = ignoreOrder;
		this.ignoreAddedDictionary = ignoreAddedDictionary;
		this.ignoreAddedIterable = ignoreAddedIterable;
		this.cleanTestDatabases = cleanTestDatabases;
		this.ignoreFailure = ignoreFailure;
	}
		
	@Parameters(name = "{index}: (user defined) test {0}")
	public static Collection<Object[]> getTestParams() {
		return PixelUnitTests.getTestParams(USER_TESTS_CSV);
	}
	
	@Test
	public void runTest() throws IOException {
		
		PixelUnitTests.runTest(this, name, pixel, expectedJson, compareAll, excludePaths, ignoreOrder, ignoreAddedDictionary, ignoreAddedIterable, cleanTestDatabases, ignoreFailure);
	}

}
