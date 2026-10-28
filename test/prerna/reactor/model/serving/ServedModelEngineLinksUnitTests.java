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
package prerna.reactor.model.serving;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import prerna.engine.impl.model.serving.ServedModelStatus;
import prerna.reactor.model.serving.ServedModelEngineLinks.ModelEngine;

class ServedModelEngineLinksUnitTests {

	private static final String SAMPLE = "olmo-2-1b-instruct";

	/** An engine made by hand: no link, calling the sample's LoadBalancer port */
	private static final String HAND_MADE = """
			#Base Properties
			ENGINE	hand-1
			ENGINE_ALIAS	OLMo by hand
			ENGINE_TYPE	prerna.engine.impl.model.TextGenerationEngine
			MODEL_TYPE	TEXT_GENERATION
			MODEL	olmo-2-1b-instruct
			ENDPOINT	http://localhost:8000/v1
			OPEN_AI_KEY	secret-stays
			""";

	private static ServedModelStatus model(String name, String endpoint, String inClusterUrl) {
		ServedModelStatus status = mock(ServedModelStatus.class);
		when(status.getServedModel()).thenReturn(name);
		when(status.getEndpoint()).thenReturn(endpoint);
		when(status.getInClusterUrl()).thenReturn(inClusterUrl);
		return status;
	}

	private static ModelEngine engine(String model, String endpoint) {
		return new ModelEngine("e-1", "Some engine", endpoint, model, null, null, Path.of("unused.smss"));
	}

	@Test
	void anEngineIsReadFromItsSmss(@TempDir Path dir) throws Exception {
		Path linked = dir.resolve("linked.smss");
		Files.writeString(linked, """
				ENGINE	e-1
				ENGINE_ALIAS	kserve test sample
				ENGINE_DISPLAY_NAME	KServe test sample
				MODEL	olmo-2-1b-instruct
				ENDPOINT	http://localhost:8000/v1
				KSERVE_NAMESPACE	semoss
				kserve_served_model	olmo-2-1b-instruct
				""");
		Path byHand = dir.resolve("hand.smss");
		Files.writeString(byHand, HAND_MADE);

		ModelEngine one = ServedModelEngineLinks.read("e-1", linked);
		ModelEngine two = ServedModelEngineLinks.read("hand-1", byHand);

		assertEquals("KServe test sample", one.engineName(), "the display name first");
		assertEquals(SAMPLE, one.servedModel(), "keys compare without case");
		assertEquals("semoss", one.namespace());
		assertTrue(one.isLinked());
		assertEquals("OLMo by hand", two.engineName(), "else the alias");
		assertEquals("http://localhost:8000/v1", two.endpoint());
		assertFalse(two.isLinked());
		assertNull(two.namespace());
	}

	@Test
	void anEngineCallsAModelByItsNameOrEitherOfItsUrls() {
		List<ServedModelStatus> models = List.of(
				model(SAMPLE, "http://localhost:8000/v1", "http://olmo-2-1b-instruct-predictor.semoss.svc/v1"),
				model("olmo-2-1b", null, "http://olmo-2-1b-predictor.semoss.svc/v1"));

		assertEquals(List.of(SAMPLE), ServedModelEngineLinks.calls(engine(SAMPLE, null), models));
		assertEquals(List.of(SAMPLE),
				ServedModelEngineLinks.calls(engine("served-as-another-name", " HTTP://LOCALHOST:8000/v1/ "), models),
				"without case, blanks or a trailing slash");
		assertEquals(List.of("olmo-2-1b"),
				ServedModelEngineLinks.calls(engine(null, "http://olmo-2-1b-predictor.semoss.svc/v1"), models));
		assertEquals(List.of(), ServedModelEngineLinks.calls(engine("gpt-4o", "https://api.example.com/v1"), models));
		assertEquals(List.of(), ServedModelEngineLinks.calls(engine(null, null), models));
	}

	@Test
	void aMismatchSaysWhatToChange() {
		String text = ServedModelEngineLinks.mismatch(engine("gpt-4o", "https://api.example.com/v1"),
				model(SAMPLE, "http://localhost:8000/v1", null));

		assertEquals("Some engine calls the model gpt-4o at https://api.example.com/v1, not olmo-2-1b-instruct. "
				+ "Set its MODEL to olmo-2-1b-instruct and its ENDPOINT to http://localhost:8000/v1 "
				+ "(its smss, in Model Settings), then link it.", text);
	}

	@Test
	void linkingAddsTwoLinesAndKeepsTheRest() {
		String linked = ServedModelEngineLinks.withLink(HAND_MADE, "semoss", SAMPLE);

		assertEquals(HAND_MADE + "KSERVE_NAMESPACE\tsemoss\nKSERVE_SERVED_MODEL\tolmo-2-1b-instruct\n", linked);
		assertEquals(linked, ServedModelEngineLinks.withLink(linked, "semoss", SAMPLE), "linking again changes nothing");
		assertEquals(HAND_MADE, ServedModelEngineLinks.withLink(linked, null, null), "unlinking restores the file");
	}

	@Test
	void relinkingReplacesTheOldLinkWhereverItWas() {
		String smss = "ENGINE\te-1\r\nkserve_served_model = old-model\r\nKSERVE_NAMESPACE:other\r\n"
				+ "# KSERVE_SERVED_MODEL stays: a comment\r\nKSERVE_NAMESPACE_NOTE\tstays: another key\r\nMODEL\tx\r\n";

		String linked = ServedModelEngineLinks.withLink(smss, "semoss", SAMPLE);

		assertEquals("ENGINE\te-1\r\n# KSERVE_SERVED_MODEL stays: a comment\r\nKSERVE_NAMESPACE_NOTE\tstays: another key"
				+ "\r\nMODEL\tx\r\nKSERVE_NAMESPACE\tsemoss\r\nKSERVE_SERVED_MODEL\tolmo-2-1b-instruct\r\n", linked,
				"the file's line endings are kept");
	}

	@Test
	void unlinkingWhatIsNotLinkedLeavesTheFileAlone() {
		String mixed = "ENGINE\te-1\r\nMODEL\tx\nENDPOINT\ty";

		assertEquals(mixed, ServedModelEngineLinks.withLink(mixed, null, null));
		assertEquals("ENGINE\te-1\r\nMODEL\tx\r\nENDPOINT\ty\r\nKSERVE_NAMESPACE\tsemoss\r\n"
				+ "KSERVE_SERVED_MODEL\tolmo-2-1b-instruct\r\n", ServedModelEngineLinks.withLink(mixed, "semoss", SAMPLE));
	}

	@Test
	void aLinkCannotCarryAnotherProperty() {
		assertThrows(IllegalArgumentException.class,
				() -> ServedModelEngineLinks.withLink(HAND_MADE, "semoss", "olmo\nENDPOINT\thttp://elsewhere"));
		assertThrows(IllegalArgumentException.class, () -> ServedModelEngineLinks.withLink(HAND_MADE, null, SAMPLE));
	}

	@Test
	void theFileIsWrittenOnlyWhenTheLinkChanges(@TempDir Path dir) throws Exception {
		Path smss = dir.resolve("engine.smss");
		// a byte outside ASCII survives the round trip
		byte[] original = (HAND_MADE + "DESCRIPTION\tcafé\n").getBytes(StandardCharsets.ISO_8859_1);
		Files.write(smss, original);

		assertTrue(ServedModelEngineLinks.writeLink(smss, "semoss", SAMPLE));
		assertFalse(ServedModelEngineLinks.writeLink(smss, "semoss", SAMPLE));
		assertEquals(SAMPLE, ServedModelEngineLinks.read("hand-1", smss).servedModel());
		assertTrue(ServedModelEngineLinks.writeLink(smss, null, null));
		assertEquals(new String(original, StandardCharsets.ISO_8859_1),
				new String(Files.readAllBytes(smss), StandardCharsets.ISO_8859_1));
		assertFalse(ServedModelEngineLinks.writeLink(smss, null, null));
	}
}
