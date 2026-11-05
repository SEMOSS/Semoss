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
package prerna.auth.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import prerna.auth.User;
import prerna.auth.access.PermissionBatch;
import prerna.auth.access.PermissionBatchException;
import prerna.auth.access.PermissionChange;
import prerna.auth.access.PermissionGrant;
import prerna.auth.access.PermissionSubject;
import prerna.auth.access.PermissionTargetResource;
import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.engine.impl.usage.UsageLimit;
import prerna.engine.impl.usage.UsageLimitKey;
import prerna.engine.impl.usage.UsageLimitKey.Source;
import prerna.engine.impl.usage.UsageRule;
import prerna.engine.impl.usage.UsageRule.Counts;
import prerna.engine.impl.usage.UsageRule.Metric;
import prerna.engine.impl.usage.UsageWindows.Window;
import prerna.util.SystemEngineRegistry;

class PermissionBatchEngineUsageLimitUnitTests extends AbstractSecurityUtilsUnitTestsSetup {
	private User owner;
	private User editor;
	private List<String> tables = new ArrayList<>();

	@BeforeEach
	void setup() throws Exception {
		owner = UnitTestSecurityAuthUtils.createUser("owner", false);
		editor = UnitTestSecurityAuthUtils.createUser("editor", false);
		UnitTestSecurityAuthUtils.createUser("reader", false);
		UnitTestSecurityAuthUtils.createEngine("db", "database", CATALOG_TYPE.DATABASE, owner);
		UnitTestSecurityAuthUtils.addPermissionsToUserForEngine(owner, "editorid", "db", "EDIT");
	}

	@AfterEach
	void cleanup() throws Exception {
		tables = UnitTestSecurityAuthUtils.clearSecurityDB(SystemEngineRegistry.getSecurityDb(), tables);
	}

	private static UsageLimit rows(long count, Counts counts) {
		return new UsageLimit(false, List.of(new UsageRule(Metric.ROWS, count, Window.DAY, null, counts)));
	}

	private static UsageLimitKey direct() {
		return new UsageLimitKey(CATALOG_TYPE.DATABASE, Source.DIRECT, "db", "readerid", "NATIVE");
	}

	private static PermissionSubject reader() {
		return PermissionSubject.ofUser("readerid", "NATIVE");
	}

	private static void save(User actor, PermissionChange... changes) throws Exception {
		SecurityAccessPermissionBatchUtils.saveBatches(actor,
				List.of(new PermissionBatch(PermissionTargetResource.engine("db"), List.of(changes))));
	}

	@Test
	void ownerSavesGrantAndPolicyTogetherAndRemovalDeletesBoth() throws Exception {
		save(owner, PermissionChange.add(reader(),
				new PermissionGrant("READ_ONLY", null).withEngineUsageLimit(rows(5, Counts.EACH_PERSON))));
		assertEquals(5, SecurityEngineUsageLimitUtils.getLimit(direct()).rules().get(0).amount());
		save(owner, PermissionChange.remove(reader()));
		assertNull(SecurityEngineUsageLimitUtils.getLimit(direct()));
	}

	@Test
	void editorCanChangeLevelButCannotLiftTheLimit() throws Exception {
		save(owner, PermissionChange.add(reader(),
				new PermissionGrant("READ_ONLY", null).withEngineUsageLimit(rows(5, Counts.EACH_PERSON))));
		save(editor, PermissionChange.change(reader(), new PermissionGrant("EDIT", null)));
		assertEquals(5, SecurityEngineUsageLimitUtils.getLimit(direct()).rules().get(0).amount());
		assertThrows(PermissionBatchException.class, () -> save(editor, PermissionChange.remove(reader())));
		assertThrows(PermissionBatchException.class, () -> save(editor,
				PermissionChange.change(reader(), new PermissionGrant("EDIT", null).withEngineUsageLimit(null))));
		assertThrows(PermissionBatchException.class, () -> save(editor,
				PermissionChange.change(reader(), new PermissionGrant("EDIT", "2099-01-01T00:00:00Z"))));
		assertNotNull(SecurityEngineUsageLimitUtils.getLimit(direct()));
	}

	@Test
	void omittedJsonPolicyIsPreservedAndExplicitNullClearsIt() throws Exception {
		save(owner, PermissionChange.add(reader(),
				new PermissionGrant("READ_ONLY", null).withEngineUsageLimit(rows(5, Counts.EACH_PERSON))));
		String prefix = "[{\"target\":{\"resourceType\":\"ENGINE\",\"id\":\"db\"},\"changes\":[{\"action\":\"CHANGE\",\"subject\":{\"category\":\"USER\",\"id\":\"readerid\",\"type\":\"NATIVE\"},\"to\":{\"level\":\"READ_ONLY\"";
		SecurityAccessPermissionBatchUtils.saveBatches(owner,
				SecurityAccessPermissionBatchUtils.readBatches(prefix + "}}]}]"));
		assertNotNull(SecurityEngineUsageLimitUtils.getLimit(direct()));
		SecurityAccessPermissionBatchUtils.saveBatches(owner,
				SecurityAccessPermissionBatchUtils.readBatches(prefix + ",\"engineUsageLimit\":null}}]}]"));
		assertNull(SecurityEngineUsageLimitUtils.getLimit(direct()));
	}

	@Test
	void settingsUseTheSameBatchAndAreOwnerOnly() throws Exception {
		var total = new PermissionSubject(PermissionSubject.SubjectCategory.ENGINE, null, null);
		save(owner,
				PermissionChange.change(total, new PermissionGrant().withEngineUsageLimit(rows(100, Counts.SHARED))));
		assertThrows(PermissionBatchException.class, () -> save(editor,
				PermissionChange.change(total, new PermissionGrant().withEngineUsageLimit(rows(10, Counts.SHARED)))));
		var permissions = SecurityAccessPermissionBatchUtils.getPermissions(owner,
				PermissionTargetResource.engine("db"));
		assertTrue(permissions.containsKey("engineUsage"));
		assertFalse(permissions.containsKey("modelTotal"));
	}

	@Test
	void oneInvalidPolicyPreventsTheWholeBatchFromBeingSaved() throws Exception {
		assertThrows(PermissionBatchException.class, () -> save(owner,
				PermissionChange.add(reader(),
						new PermissionGrant("READ_ONLY", null).withEngineUsageLimit(rows(5, Counts.EACH_PERSON))),
				PermissionChange.change(new PermissionSubject(PermissionSubject.SubjectCategory.ENGINE, null, null),
						new PermissionGrant().withEngineUsageLimit(rows(10, Counts.EACH_PERSON)))));
		assertNull(SecurityEngineUsageLimitUtils.getLimit(direct()));
	}

	@Test
	void expirationCleanupRemovesThePolicyWithTheGrant() throws Exception {
		save(owner, PermissionChange.add(reader(),
				new PermissionGrant("READ_ONLY", null).withEngineUsageLimit(rows(5, Counts.EACH_PERSON))));
		SecurityEngineUtils.removeExpiredEngineUser("readerid", "db");
		assertNull(SecurityEngineUsageLimitUtils.getLimit(direct()));
	}

	@Test
	void teamExpirationCleanupRemovesThePolicyWithTheGrant() throws Exception {
		User admin = UnitTestSecurityAuthUtils.createUser("admin", true);
		AdminSecurityGroupUtils.getInstance(admin).addGroup(admin, "team", "CUSTOM", "team");
		var team = new PermissionSubject(PermissionSubject.SubjectCategory.TEAM, "team", "CUSTOM");
		save(owner, PermissionChange.add(team,
				new PermissionGrant("READ_ONLY", null).withEngineUsageLimit(rows(5, Counts.SHARED))));
		var key = new UsageLimitKey(CATALOG_TYPE.DATABASE, Source.TEAM, "db", "team", "CUSTOM");
		assertNotNull(SecurityEngineUsageLimitUtils.getLimit(key));
		SecurityGroupEngineUtils.removeExpiredEngineGroupPermission("team", "CUSTOM", "db");
		assertNull(SecurityEngineUsageLimitUtils.getLimit(key));
	}

	@Test
	void standaloneCompatibilitySetterCannotCreateAnOrphanPolicy() {
		assertThrows(IllegalArgumentException.class,
				() -> SecurityEngineUsageLimitUtils.setLimit(owner, direct(), rows(5, Counts.EACH_PERSON)));
	}
}
