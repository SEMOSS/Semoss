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
package prerna.masterdatabase.utility;

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

class LocalMasterOwlCreatorUnitTests {

	private static final String SOURCE_JOIN = "ENGINECONCEPT.PHYSICALNAMEID.ENGINERELATION.SOURCECONCEPTID";
	private static final String TARGET_JOIN = "ENGINECONCEPT.PHYSICALNAMEID.ENGINERELATION.TARGETCONCEPTID";

	private final LocalMasterOwlCreator creator = new LocalMasterOwlCreator(
			SqlQueryUtilFactory.initialize(RdbmsTypeEnum.H2_DB));

	@Test
	void aDatabaseJoinJoinsItsTablesAndColumnsByPhysicalNameId() throws Exception {
		WriteOWLEngine owler = mock(WriteOWLEngine.class);

		creator.writeRelations(owler);

		verify(owler).addRelation("ENGINECONCEPT", "ENGINERELATION", SOURCE_JOIN);
		verify(owler).addRelation("ENGINECONCEPT", "ENGINERELATION", TARGET_JOIN);
		verify(owler, never()).addRelation(eq("ENGINECONCEPT"), eq("ENGINERELATION"), contains("LOCALCONCEPTID"));
	}

	@Test
	void anOwlJoiningByLocalConceptIdIsMadeAgain() {
		IDatabaseEngine engine = owlWith("ENGINECONCEPT.LOCALCONCEPTID.ENGINERELATION.SOURCECONCEPTID",
				"ENGINECONCEPT.LOCALCONCEPTID.ENGINERELATION.TARGETCONCEPTID");

		assertTrue(creator.additionalRemakeChecks(engine));
	}

	@Test
	void anOwlMissingOneOfTheJoinsIsMadeAgain() {
		assertTrue(creator.additionalRemakeChecks(owlWith(SOURCE_JOIN)));
		assertTrue(creator.additionalRemakeChecks(owlWith(TARGET_JOIN)));
	}

	@Test
	void anOwlWithBothJoinsIsKept() {
		assertFalse(creator.additionalRemakeChecks(owlWith(SOURCE_JOIN, TARGET_JOIN)));
	}

	private static IDatabaseEngine owlWith(String... conceptToRelationJoins) {
		IDatabaseEngine engine = mock(IDatabaseEngine.class);
		List<String[]> relations = new ArrayList<>();
		for (String relation : conceptToRelationJoins) {
			relations.add(new String[] { AbstractOWLEngine.BASE_NODE_URI + "/ENGINECONCEPT",
					AbstractOWLEngine.BASE_NODE_URI + "/ENGINERELATION",
					AbstractOWLEngine.BASE_RELATION_URI + "/" + relation });
		}
		when(engine.getPhysicalRelationships()).thenReturn(relations);
		return engine;
	}
}
