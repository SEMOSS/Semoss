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
package prerna.collaboration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.reactor.agent.mcp.MCPUtility;

/**
 * Room-local snapshots keep the reviewed email separate from later file edits.
 */
public final class EmailAttachmentFiles {

	public static final String DIRECTORY = ".email-attachments";
	// Inline Graph attachments require small files; leave room for base64 and the
	// body.
	public static final int MAX_BYTES = 2_500_000;
	public static final int MAX_FILES = 10;

	private EmailAttachmentFiles() {
	}

	public static byte[] read(Path path) throws IOException {
		try (var input = Files.newInputStream(path)) {
			byte[] bytes = input.readNBytes(MAX_BYTES + 1);
			if (bytes.length > MAX_BYTES) {
				throw new IllegalArgumentException("Email attachments must total at most 2.5 MB.");
			}
			return bytes;
		}
	}

	public static String digest(byte[] bytes) {
		try {
			return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException(e);
		}
	}

	public static List<Map<String, Object>> snapshot(String rootFolder, List<String> paths) throws IOException {
		if (paths.size() > MAX_FILES) {
			throw new IllegalArgumentException("Attach at most 10 files per ComposeEmail call.");
		}
		// Validate and read every input before creating anything.
		List<Path> sources = new ArrayList<>();
		List<byte[]> contents = new ArrayList<>();
		int total = 0;
		for (String reference : paths) {
			Path source = MCPUtility.resolveContainedMcpFile(rootFolder, reference);
			byte[] bytes = read(source);
			total += bytes.length;
			if (total > MAX_BYTES) {
				throw new IllegalArgumentException("Email attachments must total at most 2.5 MB.");
			}
			sources.add(source);
			contents.add(bytes);
		}
		List<Map<String, Object>> descriptors = new ArrayList<>();
		if (paths.isEmpty()) {
			return descriptors;
		}
		Path root = Path.of(rootFolder).toRealPath();
		Path directory = Files.createDirectories(root.resolve(DIRECTORY)).toRealPath();
		if (!directory.startsWith(root)) {
			throw new IllegalArgumentException("Invalid email attachment directory.");
		}
		for (int i = 0; i < sources.size(); i++) {
			Path copy = Files.createTempDirectory(directory, "file-").resolve(sources.get(i).getFileName());
			Files.write(copy, contents.get(i));
			Map<String, Object> descriptor = new LinkedHashMap<>();
			descriptor.put("path", root.relativize(copy).toString().replace('\\', '/'));
			descriptor.put("name", copy.getFileName().toString());
			descriptor.put("size", contents.get(i).length);
			descriptor.put("sha256", digest(contents.get(i)));
			descriptors.add(descriptor);
		}
		return descriptors;
	}
}
