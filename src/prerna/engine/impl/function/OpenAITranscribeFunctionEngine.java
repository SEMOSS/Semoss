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
package prerna.engine.impl.function;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import prerna.engine.api.FunctionTypeEnum;
import prerna.om.Insight;
import prerna.util.Utility;

public class OpenAITranscribeFunctionEngine extends AbstractFunctionEngine {

	private static final ObjectMapper MAPPER = new ObjectMapper();

	public static final String URL = "URL";
	public static final String API_KEY = "API_KEY";
	public static final String MODEL = "MODEL";

	private String url;
	private String apiKey;
	private String model;
	private String modelType;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.apiKey = smssProp.getProperty(API_KEY);
		this.url = smssProp.getProperty(URL);
		this.model = smssProp.getProperty(MODEL);
		this.modelType = smssProp.getProperty("MODEL_TYPE");

		if (this.apiKey == null || (this.apiKey.isEmpty())) {
			throw new RuntimeException("Must set API key. Use EMPTY if none.");
		}
		if (this.url == null || this.url.isEmpty()) {
			throw new RuntimeException("Must set URL");
		}
	}

	@Override
	public Object execute(Map<String, Object> parameterValues) {
		Boolean healthCheck = performHealthCheck();
		if (!healthCheck) {
			throw new RuntimeException("Model " + this.model + "is offline.");
		}

		Insight insight = (Insight) parameterValues.getOrDefault("INSIGHT", null);
		String instanceDir = Utility.normalizePath(insight.getInsightFolder());

		String filePath = (String) parameterValues.getOrDefault("filePath", null);
		if (filePath == null || filePath.isEmpty()) {
			throw new IllegalArgumentException("Parameter 'filePath' is required.");
		}

		String fileLocation = instanceDir + "/" + filePath;

		try {
			return transcribe(new File(fileLocation));
		} catch (IOException | InterruptedException e) {
			throw new RuntimeException("Transcription failed: " + e.getMessage(), e);
		}
	}

	private String transcribe(File audioFile) throws IOException, InterruptedException {
		if (!audioFile.exists()) {
			throw new IOException("File does not exist: " + audioFile.getAbsolutePath());
		}

		String modelUrl = this.url.endsWith("/") ? this.url + "audio/transcriptions"
				: this.url + "/audio/transcriptions";

		String boundary = "----JavaBoundary" + UUID.randomUUID();
		byte[] body = buildMultipartBody(boundary, this.model, audioFile);

		HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20)).build();

		HttpRequest request = HttpRequest.newBuilder().uri(URI.create(modelUrl)).timeout(Duration.ofMinutes(5))
				.header("Authorization", "Bearer " + this.apiKey)
				.header("Content-Type", "multipart/form-data; boundary=" + boundary)
				.POST(HttpRequest.BodyPublishers.ofByteArray(body)).build();

		HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
		int status = response.statusCode();
		String respBody = response.body();

		if (status / 100 != 2) {
			throw new IOException("Non-2xx response (" + status + "): " + respBody);
		}
		return extractTextField(respBody);
	}

	private static byte[] buildMultipartBody(String boundary, String model, File audioFile) throws IOException {
		String lineBreak = "\r\n";
		ByteArrayOutputStream bos = new ByteArrayOutputStream();

		bos.write(("--" + boundary + lineBreak).getBytes(StandardCharsets.UTF_8));
		bos.write(("Content-Disposition: form-data; name=\"model\"" + lineBreak).getBytes(StandardCharsets.UTF_8));
		bos.write(("Content-Type: text/plain; charset=UTF-8" + lineBreak + lineBreak).getBytes(StandardCharsets.UTF_8));
		bos.write((model + lineBreak).getBytes(StandardCharsets.UTF_8));

		String fileName = audioFile.getName();
		String contentType = guessAudioContentType(fileName);

		bos.write(("--" + boundary + lineBreak).getBytes(StandardCharsets.UTF_8));
		bos.write(("Content-Disposition: form-data; name=\"file\"; filename=\"" + fileName + "\"" + lineBreak)
				.getBytes(StandardCharsets.UTF_8));
		bos.write(("Content-Type: " + contentType + lineBreak + lineBreak).getBytes(StandardCharsets.UTF_8));

		try (FileInputStream fis = new FileInputStream(audioFile)) {
			fis.transferTo(bos);
		}
		bos.write(lineBreak.getBytes(StandardCharsets.UTF_8));

		bos.write(("--" + boundary + "--" + lineBreak).getBytes(StandardCharsets.UTF_8));

		return bos.toByteArray();
	}

	private static String guessAudioContentType(String fileName) {
		String lower = fileName.toLowerCase();
		if (lower.endsWith(".wav")) {
			return "audio/wav";
		}
		if (lower.endsWith(".mp3")) {
			return "audio/mpeg";
		}
		if (lower.endsWith(".m4a")) {
			return "audio/mp4";
		}
		if (lower.endsWith(".flac")) {
			return "audio/flac";
		}
		if (lower.endsWith(".ogg")) {
			return "audio/ogg";
		}
		return "application/octet-stream";
	}

	private static String extractTextField(String json) {
		String key = "\"text\":";
		int idx = json.indexOf(key);
		if (idx == -1) {
			return json;
		}

		int start = json.indexOf('"', idx + key.length());
		int end = json.indexOf('"', start + 1);
		if (start != -1 && end != -1) {
			return json.substring(start + 1, end);
		}
		return json;
	}

	private static String buildHealthUrl(String rawUrl) {
		if (rawUrl == null || rawUrl.isEmpty()) {
			throw new IllegalArgumentException("URL is required for health check");
		}

		String base = rawUrl.endsWith("/") ? rawUrl.substring(0, rawUrl.length() - 1) : rawUrl;

		base = base.replaceFirst("/v1/?$", "");

		return base + "/v2/health/live";
	}

	private Boolean performHealthCheck() {
		if (this.modelType == null || !this.modelType.equalsIgnoreCase("kserve")) {
			return true;
		}

		try {
			String healthUrl = buildHealthUrl(this.url);

			HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

			HttpRequest request = HttpRequest.newBuilder().uri(URI.create(healthUrl)).timeout(Duration.ofSeconds(10))
					.GET().build();

			HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

			if (response.statusCode() / 100 != 2) {
				throw new RuntimeException("Health check failed: HTTP " + response.statusCode());
			}

			JsonNode node = MAPPER.readTree(response.body());
			if (node.has("live") && node.get("live").asBoolean()) {
				return true;
			} else {
				throw new RuntimeException("Model not online at " + healthUrl + ". Response: " + response.body());
			}
		} catch (Exception e) {
			throw new RuntimeException("Health check failed: " + e.getMessage(), e);
		}
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return FunctionTypeEnum.OPENAI_TRANSCRIBE.name();
	}
}
