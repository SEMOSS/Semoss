/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.engine.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.project.api.IProject;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class SaveInsightIntoWorkspaceUnitTests {

	private SaveInsightIntoWorkspace workspace;

	@Mock
	private IProject project;

	@Mock
	private RDBMSNativeEngine insightEngine;

	@Mock
	private AbstractSqlQueryUtil queryUtil;

	@Mock
	InsightAdministrator insightAdministrator;

	@BeforeEach
	public void setup() {
		MockitoAnnotations.openMocks(this);
		String userWorkspaceId = "uwid";

		when(project.getInsightDatabase()).thenReturn(insightEngine);
		try (MockedStatic<Utility> utilityMockedStatic = Mockito.mockStatic(Utility.class);
				MockedConstruction<InsightAdministrator> ia = Mockito.mockConstruction(InsightAdministrator.class,
						(mock, context) -> {
							assertEquals(insightEngine, context.arguments().get(0));
						})) {
			utilityMockedStatic.when(() -> Utility.getProject(userWorkspaceId)).thenReturn(project);

			workspace = new SaveInsightIntoWorkspace(userWorkspaceId, "rdbmsId", "testName", false);

			assertEquals(1, ia.constructed().size());
			insightAdministrator = ia.constructed().get(0);
		}
	}

	@AfterEach
	public void teardown() {
		workspace.killThread();
	}

	@Test
	void testAddToQueue() {
		List<String> steps = new ArrayList<>();
		steps.add("step1");
		steps.add("step2");

		workspace.addToQueue(steps);
		// cannot really test anything here.
	}

	@Test
	void testDropWorkspaceCache() throws SQLException {
		try (MockedStatic<SecurityInsightUtils> siu = Mockito.mockStatic(SecurityInsightUtils.class)) {
			when(project.getProjectId()).thenReturn("pid");

			// test method call
			workspace.dropWorkspaceCache();

			// verifications
			ArgumentCaptor<String> am = ArgumentCaptor.forClass(String.class);
			verify(insightAdministrator, times(1)).dropInsight(am.capture());
			String captured = am.getValue();
			assertEquals(5, captured.split("-").length);
			assertEquals(36, captured.length());
			siu.verify(() -> SecurityInsightUtils.deleteInsight("pid", captured), times(1));
		}
	}

	@Test
	void setInsightName() {
		workspace.setInsightName("test");
		// no good to way to verify this happends
	}

	@Test
	void isCacheUserWorkspace() {
		try (MockedStatic<Utility> utilityMockedStatic = Mockito.mockStatic(Utility.class)) {
			utilityMockedStatic.when(() -> Utility.getDIHelperProperty(Constants.USER_WORKSPACE)).thenReturn("true");

			boolean val = SaveInsightIntoWorkspace.isCacheUserWorkspace();
			assertTrue(val);
		}
	}
}
