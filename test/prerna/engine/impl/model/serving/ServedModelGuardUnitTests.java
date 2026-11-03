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
package prerna.engine.impl.model.serving;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import prerna.engine.impl.model.responses.AskErrorModelEngineResponse;

class ServedModelGuardUnitTests {

	private static final String ENDPOINT = "http://10.43.0.90/v1";
	private static final String CLIENT_SAID = " The client's error was: Connection error.";

	private static Properties engine() {
		Properties properties = new Properties();
		properties.setProperty(ServedModelGuard.SERVED_MODEL_PROPERTY, "olmo-2-1b");
		properties.setProperty(ServedModelGuard.NAMESPACE_PROPERTY, "semoss");
		properties.setProperty("ENDPOINT", ENDPOINT);
		return properties;
	}

	private static AskErrorModelEngineResponse connectionError() {
		return new AskErrorModelEngineResponse("Connection error.", "APIConnectionError", 500, "openai", "olmo-2-1b",
				"Traceback (most recent call last): ...");
	}

	private static ServedModelStatus status(ServedModelState state, String message) {
		ServedModelStatus status = new ServedModelStatus();
		status.servedModel = "olmo-2-1b";
		status.state = state;
		status.message = message;
		status.endpoint = ENDPOINT;
		return status;
	}

	private static AskErrorModelEngineResponse explain(ServedModelStatus status) {
		return ServedModelGuard.explain(engine(), connectionError(), (namespace, name) -> status);
	}

	@Test
	void aStoppedModelSaysHowToStartIt() {
		AskErrorModelEngineResponse explained = explain(status(ServedModelState.STOPPED, "Stopped"));

		assertEquals("olmo-2-1b is stopped. An admin can start it with StartServedModel(servedModel=[\"olmo-2-1b\"])."
				+ CLIENT_SAID, explained.getStringResponse());
		assertEquals(ServedModelGuard.NOT_RUNNING, explained.getReason());
		assertEquals("STOPPED", explained.getReasonDetail());
		assertEquals("openai", explained.getClient());
		assertEquals(500, explained.getCode());
		assertEquals("Traceback (most recent call last): ...", explained.getTraceback());
	}

	@Test
	void aStartingModelSaysHowFarItIs() {
		ServedModelStatus starting = status(ServedModelState.STARTING, "Downloading the model weights from Hugging Face");
		starting.progress = 0.4;
		starting.etaSeconds = 130L;

		assertEquals("olmo-2-1b is still starting: Downloading the model weights from Hugging Face (40%, about 2 min "
				+ "left)." + CLIENT_SAID, explain(starting).getStringResponse());
	}

	@Test
	void aRunningModelKeepsItsOwnError() {
		AskErrorModelEngineResponse error = connectionError();
		assertSame(error, ServedModelGuard.explain(engine(), error,
				(namespace, name) -> status(ServedModelState.READY, "Ready")));

		ServedModelStatus slash = status(ServedModelState.READY, "Ready");
		slash.endpoint = ENDPOINT + "/";
		assertSame(error, ServedModelGuard.explain(engine(), error, (namespace, name) -> slash), "the same endpoint");
	}

	@Test
	void aRunningModelElsewhereOrUnreachableSaysSo() {
		ServedModelStatus moved = status(ServedModelState.READY, "Ready");
		moved.endpoint = "http://localhost:8001/v1";
		assertEquals("olmo-2-1b runs at http://localhost:8001/v1, but this engine calls " + ENDPOINT
				+ "; an admin can change the engine's ENDPOINT." + CLIENT_SAID, explain(moved).getStringResponse());

		ServedModelStatus unreachable = status(ServedModelState.READY, "Ready");
		unreachable.warnings.add("SEMOSS cannot reach the model at " + ENDPOINT + ": ConnectException");
		assertEquals("olmo-2-1b is running, but SEMOSS cannot reach the model at " + ENDPOINT + ": ConnectException."
				+ CLIENT_SAID, explain(unreachable).getStringResponse());
	}

	@Test
	void aRemovedModelIsNamed() {
		AskErrorModelEngineResponse explained = ServedModelGuard.explain(engine(), connectionError(),
				(namespace, name) -> {
					throw new IllegalArgumentException("No served model named olmo-2-1b in namespace semoss");
				});

		assertEquals("olmo-2-1b is no longer served in namespace semoss; an admin can define it again, or delete "
				+ "this engine." + CLIENT_SAID, explained.getStringResponse());
		assertEquals("GONE", explained.getReasonDetail());
	}

	@Test
	void whatIsNotAboutTheModelIsLeftAlone() {
		AtomicInteger lookups = new AtomicInteger();
		ServedModelGuard.Lookup counting = (namespace, name) -> {
			lookups.incrementAndGet();
			return status(ServedModelState.STOPPED, "Stopped");
		};
		AskErrorModelEngineResponse error = connectionError();

		Properties otherEngine = new Properties();
		otherEngine.setProperty("ENDPOINT", "https://api.openai.com/v1");
		assertSame(error, ServedModelGuard.explain(otherEngine, error, counting), "not a served model's engine");
		assertSame(error, ServedModelGuard.explain(otherEngine, error), "nor through the public entry");

		AskErrorModelEngineResponse overflow = connectionError();
		overflow.setReason(AskErrorModelEngineResponse.REASON_CONTEXT_OVERFLOW);
		assertSame(overflow, ServedModelGuard.explain(engine(), overflow, counting), "about the call, not the model");
		assertEquals(0, lookups.get());

		assertSame(error, ServedModelGuard.explain(engine(), error, (namespace, name) -> null), "state unknown");
		assertSame(error, ServedModelGuard.explain(engine(), error, (namespace, name) -> {
			throw new IllegalArgumentException("Invalid served model name");
		}));
	}

	@Test
	void everyStateButReadyHasAWord() {
		for (ServedModelState state : ServedModelState.values()) {
			if (state != ServedModelState.READY) {
				String why = ServedModelGuard.whyNot(status(state, "Some message"), ENDPOINT);
				assertTrue(why != null && why.startsWith("olmo-2-1b "), state + ": " + why);
			}
		}
	}
}
