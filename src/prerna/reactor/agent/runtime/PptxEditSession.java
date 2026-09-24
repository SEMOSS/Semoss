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
package prerna.reactor.agent.runtime;

import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.zip.ZipFile;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;

import org.json.JSONArray;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

/**
 * Original input and edit scope belong to the run, not to the generated
 * program.
 */
final class PptxEditSession {

	static final String TOOL = "PreparePptxEdit";
	private static final String P = "http://schemas.openxmlformats.org/presentationml/2006/main";
	private static final String A = "http://schemas.openxmlformats.org/drawingml/2006/main";
	private static final String R = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
	private static final String REL = "http://schemas.openxmlformats.org/package/2006/relationships";
	private static final int MAX_PART = 64 * 1024 * 1024;
	private final Path root, directory;
	private JSONObject inputs = new JSONObject();
	private JSONObject contract;

	PptxEditSession(Path root, Path stateDirectory) {
		this.root = root.toAbsolutePath().normalize();
		this.directory = stateDirectory.resolve("inputs");
	}

	/**
	 * Capture before the first author tool can overwrite, rename or remove an
	 * input.
	 */
	void capture() throws Exception {
		Files.createDirectories(directory);
		try (var paths = Files.walk(root)) {
			for (Path path : paths.filter(Files::isRegularFile).filter(p -> p.toString().endsWith(".pptx"))
					.filter(p -> !p.startsWith(root.resolve(".semoss")) && !p.startsWith(root.resolve(".claude")))
					.toList()) {
				if (!path.toRealPath().startsWith(root.toRealPath())) {
					throw new IllegalArgumentException("PPTX input escapes the working directory");
				}
				String name = root.relativize(path).toString();
				String hash = PptxWorkflow.hash(path);
				Files.copy(path, directory.resolve(hash + ".pptx"), StandardCopyOption.REPLACE_EXISTING);
				inputs.put(name, hash);
			}
		}
		Files.writeString(directory.resolve("manifest.json"), inputs.toString(2));
	}

	void restore(JSONObject savedContract) throws Exception {
		Path manifest = directory.resolve("manifest.json");
		if (Files.exists(manifest)) {
			inputs = new JSONObject(Files.readString(manifest));
		}
		contract = savedContract;
		if (contract != null) {
			original(contract.getString("sourceFilePath"));
		}
	}

	JSONObject contract() {
		return contract;
	}

	boolean active() {
		return contract != null;
	}

	boolean isInput(String file) {
		return inputs.has(file);
	}

	private Path original(String file) throws Exception {
		if (!inputs.has(file)) {
			throw new IllegalArgumentException("Input was not present when this run started: " + file);
		}
		String hash = inputs.getString(file);
		Path source = directory.resolve(hash + ".pptx");
		if (!hash.equals(PptxWorkflow.hash(source))) {
			throw new IllegalStateException("Original PPTX snapshot hash mismatch");
		}
		return source;
	}

	JSONObject prepare(String source, String output, Map<String, Object> args) throws Exception {
		Map<String, byte[]> parts = parts(original(source));
		List<String> ordered = orderedSlides(parts);
		Set<Integer> selected = new TreeSet<>();
		Object raw = args.get("slides");
		if (!(raw instanceof List<?> values) || values.isEmpty()) {
			throw new IllegalArgumentException("slides must list the requested original 1-based slide numbers");
		}
		for (Object item : values) {
			if (!(item instanceof Number n) || n.doubleValue() != n.intValue() || n.intValue() < 1
					|| n.intValue() > ordered.size() || !selected.add(n.intValue())) {
				throw new IllegalArgumentException("slides contains an invalid or duplicate slide number");
			}
		}
		String mode = String.valueOf(args.getOrDefault("editType", "text"));
		if (!Set.of("text", "slides").contains(mode)) {
			throw new IllegalArgumentException("editType must be text or slides");
		}
		Set<String> allowed = new TreeSet<>();
		for (int number : selected) {
			allowed.add(ordered.get(number - 1));
		}
		Object extra = args.getOrDefault("additionalParts", List.of());
		if (!(extra instanceof List<?> extras)) {
			throw new IllegalArgumentException("additionalParts must be an array");
		}
		Map<String, Set<Integer>> owners = owners(parts, ordered);
		for (Object item : extras) {
			if (!(item instanceof String)) throw new IllegalArgumentException("additionalParts must contain strings");
		}
		JSONObject next = new JSONObject().put("sourceFilePath", source).put("filePath", output).put("editType", mode)
				.put("slides", new JSONArray(selected)).put("allowedParts", new JSONArray(allowed))
				.put("slideCount", ordered.size()).put("sourceHash", inputs.getString(source));
		if (contract != null && (!source.equals(contract.getString("sourceFilePath"))
				|| !output.equals(contract.getString("filePath")))) {
			throw new IllegalArgumentException(
					"Keep the original edit source and output filename");
		}
		if (inputs.has(output) && !source.equals(output)
				&& !inputs.getString(output).equals(inputs.getString(source))) {
			throw new IllegalArgumentException(
					"Output already contains a different presentation; use the authoritative source or a new output filename");
		}
		JSONArray inspected = new JSONArray();
		int remaining = 16000;
		for (int number : selected) {
			String name = ordered.get(number - 1);
			Document document = xml(parts.get(name));
			var nodes = document.getElementsByTagNameNS(A, "t");
			JSONArray texts = new JSONArray();
			for (int i = 0; i < nodes.getLength(); i++) {
				String text = nodes.item(i).getTextContent();
				if (text.length() > remaining) {
					throw new IllegalArgumentException(
							"Selected text exceeds inspection limit; inspect fewer slides per edit");
				}
				remaining -= text.length();
				texts.put(new JSONObject().put("index", i).put("text", text));
			}
			JSONArray linked = new JSONArray();
			for (var entry : new TreeMap<>(owners).entrySet()) {
				if (entry.getValue().contains(number) && !entry.getKey().equals(name)) {
					linked.put(new JSONObject().put("part", entry.getKey()).put("usedBySlides", entry.getValue()));
				}
			}
			inspected.put(PptxSlideInspector.inspect(document, texts)
					.put("slide", number).put("part", name).put("texts", texts).put("linkedParts", linked));
		}
		contract = next;
		return new JSONObject().put("status", "prepared").put("edit", next)
				.put("inputSnapshot", root.relativize(original(source)).toString()).put("slides", inspected)
				.put("instructions",
						"Edit the protected inputSnapshot and preserve unrelated content. ApplyPptxEdits is an optional helper for text and colors; other edits use BuildPptx. Linked parts are discovered automatically, not a whitelist. Check usedBySlides before changing a shared resource. Changes outside the requested slides or uncertain changes are saved as a separate proposal. Consider foreground readability when changing backgrounds. Preparation may be corrected before the first build.");
	}

	void requirePrepared(String output, int slides) throws Exception {
		if (contract == null) {
			if (inputs.has(output)) {
				contract = new JSONObject().put("sourceFilePath", output).put("filePath", output)
						.put("sourceHash", inputs.getString(output)).put("slideCount", orderedSlides(parts(original(output))).size())
						.put("slides", new JSONArray()).put("allowedParts", new JSONArray())
						.put("editType", "slides").put("scopeUnspecified", true);
			}
		} else if (!output.equals(contract.getString("filePath"))) {
			throw new IllegalArgumentException("Keep the prepared output filename");
		}
	}

	JSONObject verify(Path output) throws Exception {
		if (contract == null) {
			return null;
		}
		if (!contract.getString("sourceFilePath").equals(contract.getString("filePath"))
				&& !contract.getString("sourceHash")
						.equals(PptxWorkflow.hash(root.resolve(contract.getString("sourceFilePath"))))) {
			throw new IllegalArgumentException("Edit preservation failed: separate source presentation was modified");
		}
		Map<String, byte[]> before = parts(original(contract.getString("sourceFilePath"))), after = parts(output);
		Set<String> broken = missingTargets(after);
		broken.removeAll(missingTargets(before));
		if (!broken.isEmpty()) throw new IllegalArgumentException("Edited package has missing linked files: " + broken);
		List<String> beforeSlides = orderedSlides(before), afterSlides = orderedSlides(after);
		Map<String, Set<Integer>> beforeOwners = owners(before, beforeSlides), afterOwners = owners(after, afterSlides);
		Set<Integer> selected = new TreeSet<>(), affected = new TreeSet<>();
		contract.getJSONArray("slides").forEach(value -> selected.add(((Number) value).intValue()));
		Set<String> names = new TreeSet<>(before.keySet()); names.addAll(after.keySet());
		List<String> changed = new ArrayList<>(), meaningful = new ArrayList<>(), unassigned = new ArrayList<>();
		List<String> added = new ArrayList<>(), removed = new ArrayList<>(), formatting = new ArrayList<>();
		for (String part : names) {
			byte[] a = before.get(part), b = after.get(part);
			if (Arrays.equals(a, b)) continue;
			changed.add(part);
			if (a == null) added.add(part);
			if (b == null) removed.add(part);
			boolean isXml = part.endsWith(".xml") || part.endsWith(".rels");
			if (a != null && b != null && isXml && canonical(xml(a)).equals(canonical(xml(b)))) continue;
			meaningful.add(part);
			Set<Integer> users = new TreeSet<>(beforeOwners.getOrDefault(part, Set.of()));
			users.addAll(afterOwners.getOrDefault(part, Set.of()));
			affected.addAll(users);
			if (users.isEmpty()) unassigned.add(part);
			if ("text".equals(contract.getString("editType")) && beforeSlides.contains(part)
					&& a != null && b != null && !sameExceptText(a, b)) formatting.add(part);
		}
		if (meaningful.isEmpty()) {
			throw new IllegalArgumentException("No requested edit was saved; presentation content is unchanged");
		}
		Set<Integer> outside = new TreeSet<>(affected); outside.removeAll(selected);
		boolean orderChanged = !beforeSlides.equals(afterSlides);
		List<String> warnings = new ArrayList<>();
		if (contract.optBoolean("scopeUnspecified")) warnings.add("The requested slide scope was not recorded.");
		else if (!outside.isEmpty()) warnings.add("Changes also affect slides outside the requested scope: " + outside + ".");
		if (orderChanged) warnings.add("Slide order or slide count changed.");
		if (!unassigned.isEmpty()) warnings.add("Some changed package parts could not be attributed to individual slides: " + unassigned + ".");
		if (!formatting.isEmpty()) warnings.add("A wording edit also changed formatting or objects: " + formatting + ".");
		Set<Integer> review = new TreeSet<>(selected); review.addAll(affected);
		if (orderChanged || !unassigned.isEmpty() || contract.optBoolean("scopeUnspecified")) {
			review.clear(); for (int i = 1; i <= afterSlides.size(); i++) review.add(i);
		}
		boolean proposal = !warnings.isEmpty();
		return new JSONObject().put("status", proposal ? "attention_required" : "passed")
				.put("disposition", proposal ? "proposal" : "revision").put("sourceHash", contract.getString("sourceHash"))
				.put("editType", contract.getString("editType")).put("slides", contract.getJSONArray("slides"))
				.put("affectedSlides", affected).put("outsideRequestedSlides", outside).put("reviewSlides", review)
				.put("changedParts", changed).put("contentChangedParts", meaningful).put("unassignedParts", unassigned)
				.put("addedParts", added).put("removedParts", removed).put("warnings", warnings)
				.put("unchangedParts", before.size() - changed.size() + added.size());
	}

	void recoverOriginal() throws Exception {
		if (contract != null) {
			for (String name : new TreeSet<>(
					List.of(contract.getString("sourceFilePath"), contract.getString("filePath")))) {
				recover(contract.getString("sourceFilePath"), name);
			}
		}
	}

	void recoverSeparateSource() throws Exception {
		if (contract != null && !contract.getString("sourceFilePath").equals(contract.getString("filePath"))) {
			recover(contract.getString("sourceFilePath"), contract.getString("sourceFilePath"));
		}
	}

	private void recover(String source, String name) throws Exception {
		Path output = root.resolve(name), ancestor = output.getParent();
		while (!Files.exists(ancestor)) {
			ancestor = ancestor.getParent();
		}
		if (!ancestor.toRealPath().startsWith(root.toRealPath())) {
			throw new IllegalStateException("Recovery destination escapes the working directory");
		}
		Files.createDirectories(output.getParent());
		Files.copy(original(source), output, StandardCopyOption.REPLACE_EXISTING);
	}

	static Map<String, byte[]> parts(Path source) throws Exception {
		Map<String, byte[]> result = new TreeMap<>();
		long total = 0;
		try (ZipFile zip = new ZipFile(source.toFile())) {
			var entries = zip.entries();
			while (entries.hasMoreElements()) {
				var entry = entries.nextElement();
				if (entry.isDirectory()) {
					continue;
				}
				byte[] bytes;
				try (var in = zip.getInputStream(entry)) {
					bytes = in.readNBytes(MAX_PART + 1);
				}
				if (bytes.length > MAX_PART || (total += bytes.length) > 500L * 1024 * 1024) {
					throw new IllegalArgumentException("PPTX package exceeds inspection limits");
				}
				if (result.put(entry.getName(), bytes) != null) {
					throw new IllegalArgumentException("Duplicate PPTX package entry: " + entry.getName());
				}
			}
		}
		return result;
	}

	static List<String> orderedSlides(Map<String, byte[]> parts) throws Exception {
		Map<String, String> targets = relationships(parts, "ppt/presentation.xml");
		var ids = xml(parts.get("ppt/presentation.xml")).getElementsByTagNameNS(P, "sldId");
		List<String> result = new ArrayList<>();
		for (int i = 0; i < ids.getLength(); i++) {
			String target = targets.get(((Element) ids.item(i)).getAttributeNS(R, "id"));
			if (target == null || !parts.containsKey(target) || result.contains(target)) {
				throw new IllegalArgumentException("Invalid presentation slide order");
			}
			result.add(target);
		}
		if (result.isEmpty() || result.size() > 100) {
			throw new IllegalArgumentException("Expected 1 to 100 slides");
		}
		return result;
	}

	private static String rels(String part) {
		int slash = part.lastIndexOf('/');
		return part.substring(0, slash + 1) + "_rels/" + part.substring(slash + 1) + ".rels";
	}

	private static Map<String, String> relationships(Map<String, byte[]> parts, String part) throws Exception {
		Map<String, String> result = new HashMap<>();
		if (!parts.containsKey(rels(part))) {
			return result;
		}
		var links = xml(parts.get(rels(part))).getElementsByTagNameNS(REL, "Relationship");
		for (int i = 0; i < links.getLength(); i++) {
			Element link = (Element) links.item(i);
			if ("External".equals(link.getAttribute("TargetMode"))) {
				continue;
			}
			String target = link.getAttribute("Target");
			java.net.URI uri = java.net.URI.create(target.replace(" ", "%20"));
			if (uri.isAbsolute() || uri.getRawAuthority() != null) {
				throw new IllegalArgumentException("Internal package relationship must target a package part");
			}
			target = uri.getPath();
			if (target == null || target.isEmpty()) target = "/" + part;
			Path parent = Path.of(part).getParent();
			Path resolved = (target.startsWith("/") ? Path.of(target.substring(1))
					: (parent == null ? Path.of("") : parent).resolve(target)).normalize();
			if (resolved.startsWith("..") || resolved.isAbsolute()) {
				throw new IllegalArgumentException("Invalid package relationship");
			}
			result.put(link.getAttribute("Id"), resolved.toString());
		}
		return result;
	}

	private static Map<String, Set<Integer>> owners(Map<String, byte[]> parts, List<String> ordered) throws Exception {
		Map<String, Set<Integer>> owners = new HashMap<>();
		for (int i = 0; i < ordered.size(); i++) {
			List<String> pending = new ArrayList<>(List.of(ordered.get(i)));
			Set<String> seen = new TreeSet<>();
			for (int j = 0; j < pending.size(); j++) {
				String part = pending.get(j);
				if (!seen.add(part)) {
					continue;
				}
				owners.computeIfAbsent(part, key -> new TreeSet<>()).add(i + 1);
				if (parts.containsKey(rels(part))) {
					owners.computeIfAbsent(rels(part), key -> new TreeSet<>()).add(i + 1);
				}
				pending.addAll(relationships(parts, part).values());
			}
		}
		return owners;
	}

	private static Set<String> missingTargets(Map<String, byte[]> parts) throws Exception {
		Set<String> missing = new TreeSet<>();
		for (String name : parts.keySet()) {
			if (!name.endsWith(".rels")) continue;
			int marker = name.lastIndexOf("_rels/");
			if (marker < 0) continue;
			String source = name.substring(0, marker) + name.substring(marker + 6, name.length() - 5);
			for (var link : relationships(parts, source).entrySet()) {
				if (!parts.containsKey(link.getValue())) missing.add(name + "#" + link.getKey() + " -> " + link.getValue());
			}
		}
		return missing;
	}

	private static Document xml(byte[] bytes) throws Exception {
		if (bytes == null) {
			throw new IllegalArgumentException("Required PPTX XML part is missing");
		}
		DocumentBuilderFactory factory = DocumentBuilderFactory.newDefaultInstance();
		factory.setNamespaceAware(true);
		factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
		factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
		factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
		factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
		factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
		return factory.newDocumentBuilder().parse(new ByteArrayInputStream(bytes));
	}

	private static boolean sameExceptText(byte[] before, byte[] after) throws Exception {
		Document a = xml(before), b = xml(after);
		for (Document doc : List.of(a, b)) {
			var texts = doc.getElementsByTagNameNS(A, "t");
			for (int i = 0; i < texts.getLength(); i++) {
				Node node = texts.item(i);
				for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {
					if (child.getNodeType() != Node.TEXT_NODE && child.getNodeType() != Node.CDATA_SECTION_NODE) {
						return false;
					}
				}
				node.setTextContent("TEXT");
			}
		}
		return canonical(a).equals(canonical(b));
	}
	/** Compare XML content without treating namespace prefixes, attribute order or indentation as edits. */
	private static String canonical(Node node) {
		StringBuilder result = new StringBuilder();
		canonical(node, result, false);
		return result.toString();
	}

	private static void token(StringBuilder out, String value) {
		String text = value == null ? "" : value;
		out.append(text.length()).append(':').append(text);
	}

	private static void canonical(Node node, StringBuilder out, boolean preserveSpace) {
		if (node instanceof Element element) {
			out.append('<'); token(out, element.getNamespaceURI()); token(out, element.getLocalName());
			Map<String, String> attributes = new TreeMap<>();
			var attrs = element.getAttributes();
			for (int i = 0; i < attrs.getLength(); i++) {
				Node attr = attrs.item(i);
				if (!XMLConstants.XMLNS_ATTRIBUTE_NS_URI.equals(attr.getNamespaceURI()))
					attributes.put(String.valueOf(attr.getNamespaceURI()) + "|" + attr.getLocalName(), attr.getNodeValue());
			}
			attributes.forEach((key, value) -> { token(out, key); token(out, value); });
			out.append('>');
			String space = element.getAttributeNS(XMLConstants.XML_NS_URI, "space");
			if (!space.isEmpty()) preserveSpace = space.equals("preserve");
		}
		boolean elements = false;
		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling())
			if (child instanceof Element) elements = true;
		for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling()) {
			if (child.getNodeType() == Node.TEXT_NODE || child.getNodeType() == Node.CDATA_SECTION_NODE) {
				if (!elements || preserveSpace || !child.getNodeValue().isBlank()) { out.append('T'); token(out, child.getNodeValue()); }
			} else if (child instanceof Element) canonical(child, out, preserveSpace);
		}
		if (node instanceof Element) out.append('/');
	}

}
