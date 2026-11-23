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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.owl.AbstractOWLEngine;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

class SchedulerOwlCreatorUnitTests {

	private static final String JOB_NAME_JOIN = "QRTZ_TRIGGERS.JOB_NAME.QRTZ_JOB_DETAILS.JOB_NAME";
	private static final String JOB_GROUP_JOIN = "QRTZ_TRIGGERS.JOB_GROUP.QRTZ_JOB_DETAILS.JOB_GROUP";

	private final SchedulerOwlCreator creator = new SchedulerOwlCreator(
			SqlQueryUtilFactory.initialize(RdbmsTypeEnum.H2_DB));

	@Test
	void aTriggerJoinsTheJobItFiresByJobNameAndGroup() throws Exception {
		WriteOWLEngine owler = mock(WriteOWLEngine.class);

		creator.writeRelations(owler);

		verify(owler).addRelation("QRTZ_TRIGGERS", "QRTZ_JOB_DETAILS",
				"QRTZ_TRIGGERS.SCHED_NAME.QRTZ_JOB_DETAILS.SCHED_NAME");
		verify(owler).addRelation("QRTZ_TRIGGERS", "QRTZ_JOB_DETAILS", JOB_NAME_JOIN);
		verify(owler).addRelation("QRTZ_TRIGGERS", "QRTZ_JOB_DETAILS", JOB_GROUP_JOIN);
		verify(owler, never()).addRelation(eq("QRTZ_TRIGGERS"), eq("QRTZ_JOB_DETAILS"), contains("TRIGGER_"));
	}

	@Test
	void anOwlJoiningATriggerToItsJobByTriggerNameIsMadeAgain() {
		IDatabaseEngine engine = owlWith("QRTZ_TRIGGERS.TRIGGER_NAME.QRTZ_JOB_DETAILS.TRIGGER_NAME",
				"QRTZ_TRIGGERS.TRIGGER_GROUP.QRTZ_JOB_DETAILS.TRIGGER_GROUP");

		assertTrue(creator.additionalRemakeChecks(engine));
	}

	@Test
	void anOwlMissingOneOfTheJobJoinsIsMadeAgain() {
		assertTrue(creator.additionalRemakeChecks(owlWith(JOB_NAME_JOIN)));
		assertTrue(creator.additionalRemakeChecks(owlWith(JOB_GROUP_JOIN)));
	}

	@Test
	void anOwlWithBothJobJoinsIsKept() {
		assertFalse(creator.additionalRemakeChecks(owlWith(JOB_NAME_JOIN, JOB_GROUP_JOIN)));
	}

	private static IDatabaseEngine owlWith(String... triggerToJobRelations) {
		IDatabaseEngine engine = mock(IDatabaseEngine.class);
		List<String[]> relations = new ArrayList<>();
		for (String relation : triggerToJobRelations) {
			relations.add(new String[] { AbstractOWLEngine.BASE_NODE_URI + "/QRTZ_TRIGGERS",
					AbstractOWLEngine.BASE_NODE_URI + "/QRTZ_JOB_DETAILS",
					AbstractOWLEngine.BASE_RELATION_URI + "/" + relation });
		}
		when(engine.getPhysicalRelationships()).thenReturn(relations);
		return engine;
	}
}
