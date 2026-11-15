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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import com.sun.net.httpserver.HttpServer;

import prerna.security.HttpHelperUtility;

/**
 * One test per rule on the HTTP requests a project's custom reactors make
 * through {@link HttpHelperUtility}. Each loads a small class through the
 * sandbox, so the helper sees a project class as its caller. The sandbox page
 * in the docs lists each rule with the test here that guards it.
 */
class SandboxHttpUnitTests {

	private static final String HELPER = "prerna/security/HttpHelperUtility";

	@TempDir
	Path folder;

	private HttpServer server;
	private String localhost;
	private Properties savedCoreProp;

	@BeforeEach
	void startServer() throws IOException {
		this.savedCoreProp = DIHelper.getInstance().getCoreProp();
		DIHelper.getInstance().setCoreProp(new Properties());
		this.server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
		this.server.createContext("/hello", exchange -> {
			byte[] body = "hello".getBytes(StandardCharsets.UTF_8);
			exchange.sendResponseHeaders(200, body.length);
			try (OutputStream out = exchange.getResponseBody()) {
				out.write(body);
			}
		});
		this.server.start();
		this.localhost = "http://127.0.0.1:" + this.server.getAddress().getPort() + "/hello";
	}

	@AfterEach
	void stopServer() {
		this.server.stop(0);
		SandboxedJavaExecution.clearSandboxRootForCurrentThread();
		DIHelper.getInstance().setCoreProp(this.savedCoreProp);
	}

	@Test
	void aProjectReactorCallsLocalhostUnlessTheAdminBlacklistsIt() throws Throwable {
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe().method("run", get(this.localhost)).bytes());

		assertEquals("hello", SandboxProbe.run(probe, "run"));

		DIHelper.getInstance().getCoreProp().setProperty(Constants.BLACK_LIST_DOMAINS, "localhost, 127.0.0.0/8, ::1");
		assertThrows(IllegalArgumentException.class, () -> SandboxProbe.run(probe, "run"));
		assertThrows(IllegalArgumentException.class, () -> HttpHelperUtility.getRequest(this.localhost, null, null));
	}

	@Test
	void aProjectReactorCannotCallTheCloudMetadataService() throws Exception {
		String metadata = "http://169.254.169.254/latest/meta-data/";
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe().method("run", get(metadata)).bytes());

		IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
				() -> SandboxProbe.run(probe, "run"));

		assertEquals("Project reactors are not allowed to make requests to the host: 169.254.169.254",
				refused.getMessage());
	}

	@ParameterizedTest
	@ValueSource(strings = { "getCustomClient", "getSchedulerClient" })
	void aProjectReactorCannotBuildItsOwnClient(String method) throws Exception {
		Class<?> probe = SandboxProbe.load(this.folder,
				new SandboxProbe()
						.method("run",
								SandboxProbe.call(Opcodes.INVOKESTATIC, HELPER, method,
										"(Lorg/apache/hc/client5/http/cookie/CookieStore;Ljava/lang/String;"
												+ "Ljava/lang/String;Ljava/lang/String;)"
												+ "Lorg/apache/hc/client5/http/impl/classic/CloseableHttpClient;"))
						.bytes());

		SecurityException refused = assertThrows(SecurityException.class, () -> SandboxProbe.run(probe, "run"));
		assertTrue(refused.getMessage().contains(method + " is not permitted in project reactors"));
		HttpHelperUtility.getCustomClient(null, null, null, null).close();
	}

	@Test
	void withAUserRootADownloadIsSavedInsideIt() throws Throwable {
		Path root = Files.createDirectories(this.folder.resolve("user-root")).toRealPath();
		String outside = this.folder.toRealPath().resolve("downloads").toString();
		String url = this.localhost;
		Class<?> probe = SandboxProbe.load(this.folder, new SandboxProbe().method("run", mv -> {
			mv.visitLdcInsn(url);
			mv.visitInsn(Opcodes.ACONST_NULL);
			mv.visitInsn(Opcodes.ACONST_NULL);
			mv.visitInsn(Opcodes.ACONST_NULL);
			mv.visitInsn(Opcodes.ACONST_NULL);
			mv.visitLdcInsn(outside);
			mv.visitLdcInsn("hello.txt");
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, HELPER, "getRequestFileDownload",
					"(Ljava/lang/String;Ljava/util/Map;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;"
							+ "Ljava/lang/String;Ljava/lang/String;)Ljava/io/File;",
					false);
		}).bytes());
		SandboxedJavaExecution.setSandboxRootForCurrentThread(root.toString());

		File saved = (File) SandboxProbe.run(probe, "run");

		assertTrue(saved.toPath().toRealPath().startsWith(root), saved.toString());
		assertEquals("hello", Files.readString(saved.toPath()));
		assertTrue(Files.notExists(Path.of(outside)));
	}

	/**
	 * A method body that sends a GET through the helper with no headers and no
	 * timeout.
	 */
	private static Consumer<MethodVisitor> get(String url) {
		return mv -> {
			mv.visitLdcInsn(url);
			mv.visitInsn(Opcodes.ACONST_NULL);
			mv.visitInsn(Opcodes.ACONST_NULL);
			mv.visitMethodInsn(Opcodes.INVOKESTATIC, HELPER, "getRequest",
					"(Ljava/lang/String;Ljava/util/Map;Ljava/time/Duration;)Ljava/lang/String;", false);
		};
	}
}
