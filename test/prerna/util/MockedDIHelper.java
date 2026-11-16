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
package prerna.util;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * Replaces the {@link DIHelper} singleton with a Mockito mock for the length of
 * a try-with-resources block, so tests outside {@code prerna.util} can set the
 * RDF_Map settings and registered ids the code under test reads. Every key
 * returns null until a test sets it. Closing it puts the real singleton back.
 */
public final class MockedDIHelper implements AutoCloseable {

	private final MockedStatic<DIHelper> mockedStatic;
	private final DIHelper helper;

	private MockedDIHelper() {
		this.helper = Mockito.mock(DIHelper.class);
		this.mockedStatic = Mockito.mockStatic(DIHelper.class);
		this.mockedStatic.when(DIHelper::getInstance).thenReturn(this.helper);
	}

	/**
	 * Starts mocking DIHelper with no settings and no registered engines or
	 * projects.
	 *
	 * @return the mock, to close when the test is done
	 */
	public static MockedDIHelper start() {
		return new MockedDIHelper();
	}

	/**
	 * Sets the RDF_Map setting that {@link Utility#getDIHelperProperty(String)}
	 * returns for the key.
	 *
	 * @param key   the RDF_Map key
	 * @param value the value to return
	 * @return this mock
	 */
	public MockedDIHelper withProperty(String key, String value) {
		when(this.helper.getProperty(key)).thenReturn(value);
		return this;
	}

	/**
	 * Sets the ";"-joined ids that {@link InternalUtility#getEngineIds()}
	 * returns.
	 *
	 * @param engineIds the registered engine ids
	 * @return this mock
	 */
	public MockedDIHelper withEngineIds(String engineIds) {
		when(this.helper.getEngineProperty(Constants.ENGINES)).thenReturn(engineIds);
		return this;
	}

	/**
	 * Sets the ";"-joined ids that {@link InternalUtility#getProjectIds()}
	 * returns.
	 *
	 * @param projectIds the registered project ids
	 * @return this mock
	 */
	public MockedDIHelper withProjectIds(String projectIds) {
		when(this.helper.getProjectProperty(Constants.PROJECTS)).thenReturn(projectIds);
		return this;
	}

	/**
	 * Checks that the code under test took the loaded engine out of DIHelper
	 * exactly once.
	 *
	 * @param engineId the engine id
	 */
	public void verifyLoadedEngineRemoved(String engineId) {
		verify(this.helper).removeEngineProperty(engineId);
	}

	/**
	 * Checks that the code under test took the loaded project out of DIHelper
	 * exactly once.
	 *
	 * @param projectId the project id
	 */
	public void verifyLoadedProjectRemoved(String projectId) {
		verify(this.helper).removeProjectProperty(projectId);
	}

	/**
	 * Stops mocking DIHelper and puts the real singleton back.
	 */
	@Override
	public void close() {
		this.mockedStatic.close();
	}
}
