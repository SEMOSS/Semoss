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

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.util.k8s.KubeJson;

/**
 * Objects recorded from the DGX Spark's cluster with kserve-verify.sh while the
 * sample model olmo-2-1b-instruct was stopped and started (2026-10-26), and a
 * second InferenceService that waited for the GPU. Times (UTC): stop requested
 * 23:56:52, start 23:57:07, download 23:57:07-23:57:45, vLLM 23:57:46, Ready
 * 23:59:07; the GPU wait began 00:00:42.
 */
final class Fixtures {

	private Fixtures() {
	}

	static String text(String name) {
		try (InputStream in = Fixtures.class.getResourceAsStream("kserve/" + name)) {
			if (in == null) {
				throw new IllegalStateException("Missing fixture " + name);
			}
			return new String(in.readAllBytes(), StandardCharsets.UTF_8);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	/** A fresh copy each call, so tests can edit it. */
	static JsonObject json(String name) {
		return JsonParser.parseString(text(name)).getAsJsonObject();
	}

	static List<JsonObject> items(String name) {
		return new ArrayList<>(KubeJson.objects(json(name), "items"));
	}

	/** vLLM's startup log, up to and including the first line containing the marker. */
	static List<String> vllmLogUntil(String marker) {
		List<String> lines = new ArrayList<>();
		for (String line : text("vllm-startup.log").split("\n")) {
			lines.add(line);
			if (line.contains(marker)) {
				return lines;
			}
		}
		throw new IllegalStateException("No line with " + marker);
	}
}
