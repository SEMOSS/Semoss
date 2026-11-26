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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;

import prerna.auth.CustomReactorSecurity;

/**
 * The lines the compile output adds about calls a custom reactor makes to the
 * security utilities and the other classes that work on a system database.
 */
class SandboxCompileNoticesUnitTests {

	private static final String SOURCE = SandboxProbe.INTERNAL_NAME + ".java";

	@TempDir
	Path folder;

	@Test
	void aSecurityQuestionAskedDirectlyIsDeprecatedAndNamesItsReplacement() {
		List<SemossClassLoader.CompileNotice> notices = noticesForCall(39, "prerna/auth/utils/SecurityInsightUtils",
				"userCanEditInsight", "(Lprerna/auth/User;Ljava/lang/String;Ljava/lang/String;)Z");

		assertEquals(List.of(new SemossClassLoader.CompileNotice(SOURCE, 39, false,
				"SecurityInsightUtils.userCanEditInsight is deprecated in custom reactors; "
						+ "call CustomReactorSecurity.userCanEditInsight instead")),
				notices);
	}

	@Test
	void aQuestionNamedThroughASubclassFindsItsReplacement() {
		List<SemossClassLoader.CompileNotice> notices = noticesForCall(7, "prerna/auth/utils/SecurityQueryUtils",
				"adminSetExporter", "()Z");

		assertEquals(1, notices.size());
		assertTrue(notices.get(0).message().endsWith("call CustomReactorSecurity.adminSetExporter instead"));
	}

	@Test
	void aRefusedCallSaysItThrowsAndNamesItsReplacement() {
		List<SemossClassLoader.CompileNotice> notices = noticesForCall(12, "prerna/auth/utils/SecurityQueryUtils",
				"searchForUser", "(Ljava/lang/String;)Ljava/util/List;");

		assertEquals(List.of(new SemossClassLoader.CompileNotice(SOURCE, 12, true,
				"SecurityQueryUtils.searchForUser is not available to custom reactors and throws when it runs; "
						+ "call CustomReactorSecurity.searchUsers instead")),
				notices);
	}

	@Test
	void aRefusedChangePointsToThePlatformsReactors() {
		List<SemossClassLoader.CompileNotice> notices = noticesForCall(20, "prerna/auth/utils/SecurityEngineUtils",
				"addEngineUser", "(Ljava/lang/String;)V");

		assertEquals(1, notices.size());
		assertTrue(notices.get(0).refused());
		assertTrue(notices.get(0).message().contains("the platform's reactors, run through Pixel"));
	}

	@Test
	void callsOutsideTheSystemDatabasesGetNoNotice() {
		assertTrue(noticesForCall(3, "prerna/auth/CustomReactorSecurity", "userCanViewEngine",
				"(Lprerna/auth/User;Ljava/lang/String;)Z").isEmpty());
		assertTrue(noticesForCall(4, "java/lang/Math", "abs", "(I)I").isEmpty());
	}

	@Test
	void everyReplacementReplacesARealMethodWithAMethodOfCustomReactorSecurity() throws Exception {
		List<String> approved = Stream.of(CustomReactorSecurity.class.getDeclaredMethods())
				.filter(method -> Modifier.isPublic(method.getModifiers())).map(Method::getName).toList();

		assertFalse(SemossClassLoader.CUSTOM_REACTOR_SECURITY_REPLACEMENTS.isEmpty());
		for (Map.Entry<String, String> replacement : SemossClassLoader.CUSTOM_REACTOR_SECURITY_REPLACEMENTS
				.entrySet()) {
			int dot = replacement.getKey().lastIndexOf('.');
			Class<?> owner = Class.forName(replacement.getKey().substring(0, dot).replace('/', '.'), false,
					getClass().getClassLoader());
			String name = replacement.getKey().substring(dot + 1);

			assertTrue(Stream.of(owner.getDeclaredMethods()).anyMatch(method -> method.getName().equals(name)),
					replacement.getKey());
			assertTrue(approved.contains(replacement.getValue()), replacement.getValue());
		}
	}

	@Test
	void theCompileOutputListsTheDeprecatedAndRefusedCalls() throws Exception {
		Path source = this.folder.resolve("java/custom/notice/CheckReactor.java");
		Files.createDirectories(source.getParent());
		Files.writeString(source, String.join("\n", "package custom.notice;", "",
				"import prerna.auth.AuthProvider;", "import prerna.auth.User;",
				"import prerna.auth.utils.SecurityQueryUtils;", "", "public class CheckReactor {", "",
				"	public boolean canExport(User user) {", "		return SecurityQueryUtils.userIsExporter(user);",
				"	}", "", "	public Object[] lock(String userId) {",
				"		return SecurityQueryUtils.getUserLockAndLastLoginAndLastPassReset(userId, AuthProvider.NATIVE);",
				"	}", "}", ""), StandardCharsets.UTF_8);
		String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));

		int status = InternalUtility.compileJava(this.folder.toString(), classpath);
		List<String> output = Files.readAllLines(this.folder.resolve("classes/compileerror.out"));

		assertEquals(0, status, String.join("\n", output));
		assertTrue(output.stream().anyMatch(line -> line.startsWith("[WARNING] ")
				&& line.endsWith("CheckReactor.java:10 - SecurityQueryUtils.userIsExporter is deprecated in custom "
						+ "reactors; call CustomReactorSecurity.userIsExporter instead")),
				String.join("\n", output));
		assertTrue(output.stream()
				.anyMatch(line -> line.startsWith("[ERROR] ") && line.contains("CheckReactor.java:14 - "
						+ "SecurityQueryUtils.getUserLockAndLastLoginAndLastPassReset is not available to custom "
						+ "reactors and throws when it runs")),
				String.join("\n", output));
	}

	private static List<SemossClassLoader.CompileNotice> noticesForCall(int line, String owner, String name,
			String descriptor) {
		byte[] probe = new SandboxProbe().method("run", mv -> {
			Label start = new Label();
			mv.visitLabel(start);
			mv.visitLineNumber(line, start);
			SandboxProbe.call(Opcodes.INVOKESTATIC, owner, name, descriptor).accept(mv);
		}).bytes();
		return SemossClassLoader.compileNotices(probe);
	}
}
