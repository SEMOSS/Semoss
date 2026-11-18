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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.function.Supplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.objectweb.asm.Opcodes;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminExternalConnectorsUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.StoredAdmin;
import prerna.reactor.utils.GetShareAuthTokenReactor;
import prerna.reactor.utils.GetShareSessionTokenReactor;
import prerna.util.insight.InsightUtility;

/**
 * A custom reactor works with what its user holds on the request it serves. It
 * gains nothing that lets it, or anyone it tells, act as the user elsewhere: an
 * admin's rights, a token for the session, or the session id. Each test names
 * the rule it guards; the rules are listed in
 * docs/platform_services/project_reactor_sandbox.md.
 */
class SandboxActsAsTheUserUnitTests {

	private static final String SESSION_ID = "0123456789ABCDEF0123456789ABCDEF";

	@TempDir
	Path folder;

	@AfterEach
	void clearHeldObject() {
		SandboxProbe.heldObject = null;
	}

	@Test
	void anAdminIsNotAnAdminWhileACustomReactorIsActing() throws Throwable {
		User admin = new User();
		Class<?> probe = platformWorkRunByACustomReactor();

		StoredAdmin.run(admin, () -> {
			assertEquals(Boolean.TRUE, SecurityAdminUtils.userIsAdmin(admin));
			assertNotNull(SecurityAdminUtils.getInstance(admin));
			assertNotNull(SecurityAdminExternalConnectorsUtils.getInstance(admin));

			SandboxProbe.heldObject = supplier(() -> SecurityAdminUtils.userIsAdmin(admin));
			assertEquals(Boolean.FALSE, SandboxProbe.run(probe, "run"));
			SandboxProbe.heldObject = supplier(() -> SecurityAdminUtils.getInstance(admin));
			assertNull(SandboxProbe.run(probe, "run"));
			SandboxProbe.heldObject = supplier(() -> SecurityAdminExternalConnectorsUtils.getInstance(admin));
			assertNull(SandboxProbe.run(probe, "run"));
		});
	}

	@Test
	void aCustomReactorCannotHaveAShareTokenMadeForTheUsersSession() throws Throwable {
		Class<?> probe = platformWorkRunByACustomReactor();

		SandboxProbe.heldObject = supplier(() -> new GetShareSessionTokenReactor().execute());
		SecurityException refused = assertThrows(SecurityException.class, () -> SandboxProbe.run(probe, "run"));
		assertEquals("A share token is created on the user's own request, not by a custom reactor",
				refused.getMessage());
		SandboxProbe.heldObject = supplier(() -> new GetShareAuthTokenReactor().execute());
		assertThrows(SecurityException.class, () -> SandboxProbe.run(probe, "run"));
	}

	@Test
	void aSessionsFolderNameDoesNotTellTheSessionId() {
		String name = InsightUtility.getFolderDirSessionId(SESSION_ID);

		assertTrue(name.matches("[0-9a-f]{32}"), name);
		assertNotEquals(SESSION_ID.toLowerCase(), name);
		assertEquals(name, InsightUtility.getFolderDirSessionId(SESSION_ID));
		assertNotEquals(name, InsightUtility.getFolderDirSessionId(SESSION_ID + "0"));
		assertEquals("UNKNOWN_SESSION_ID", InsightUtility.getFolderDirSessionId(null));
	}

	/**
	 * A class of a project whose {@code run} method has the platform run the work
	 * the test holds, the way a custom reactor runs a pixel or calls a helper.
	 *
	 * @return the sandboxed class
	 * @throws Exception if the class cannot be loaded
	 */
	private Class<?> platformWorkRunByACustomReactor() throws Exception {
		return SandboxProbe.load(this.folder, new SandboxProbe().method("run", mv -> {
			mv.visitFieldInsn(Opcodes.GETSTATIC, "prerna/util/SandboxProbe", "heldObject", "Ljava/lang/Object;");
			mv.visitTypeInsn(Opcodes.CHECKCAST, "java/util/function/Supplier");
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, "prerna/util/SandboxProbe", "runAsPlatform",
					"(Ljava/util/function/Supplier;)Ljava/lang/Object;", false);
		}).bytes());
	}

	private static Supplier<Object> supplier(Supplier<Object> work) {
		return work;
	}
}
