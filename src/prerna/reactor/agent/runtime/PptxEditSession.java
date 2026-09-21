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

/** Original input and edit scope belong to the run, not to the generated program. */
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

    /** Capture before the first author tool can overwrite, rename or remove an input. */
    void capture() throws Exception {
        Files.createDirectories(directory);
        try (var paths = Files.walk(root)) {
            for (Path path : paths.filter(Files::isRegularFile).filter(p -> p.toString().endsWith(".pptx"))
                    .filter(p -> !p.startsWith(root.resolve(".semoss")) && !p.startsWith(root.resolve(".claude"))).toList()) {
                if (!path.toRealPath().startsWith(root.toRealPath())) throw new IllegalArgumentException("PPTX input escapes the working directory");
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
        if (Files.exists(manifest)) inputs = new JSONObject(Files.readString(manifest));
        contract = savedContract;
        if (contract != null) original(contract.getString("sourceFilePath"));
    }

    JSONObject contract() { return contract; }
    boolean active() { return contract != null; }
    boolean isInput(String file) { return inputs.has(file); }

    private Path original(String file) throws Exception {
        if (!inputs.has(file)) throw new IllegalArgumentException("Input was not present when this run started: " + file);
        String hash = inputs.getString(file);
        Path source = directory.resolve(hash + ".pptx");
        if (!hash.equals(PptxWorkflow.hash(source))) throw new IllegalStateException("Original PPTX snapshot hash mismatch");
        return source;
    }

    JSONObject prepare(String source, String output, Map<String, Object> args) throws Exception {
        Map<String, byte[]> parts = parts(original(source));
        List<String> ordered = orderedSlides(parts);
        Set<Integer> selected = new TreeSet<>();
        Object raw = args.get("slides");
        if (!(raw instanceof List<?> values) || values.isEmpty()) throw new IllegalArgumentException("slides must list the requested original 1-based slide numbers");
        for (Object item : values) {
            if (!(item instanceof Number n) || n.doubleValue() != n.intValue() || n.intValue() < 1 || n.intValue() > ordered.size()
                    || !selected.add(n.intValue())) throw new IllegalArgumentException("slides contains an invalid or duplicate slide number");
        }
        String mode = String.valueOf(args.getOrDefault("editType", "text"));
        if (!Set.of("text", "slides").contains(mode)) throw new IllegalArgumentException("editType must be text or slides");
        Set<String> allowed = new TreeSet<>();
        for (int number : selected) allowed.add(ordered.get(number - 1));
        Object extra = args.getOrDefault("additionalParts", List.of());
        if (!(extra instanceof List<?> extras)) throw new IllegalArgumentException("additionalParts must be an array");
        if (mode.equals("text") && !extras.isEmpty()) throw new IllegalArgumentException("Text edits may only change text in selected slides");
        Map<String, Set<Integer>> owners = owners(parts, ordered);
        for (Object item : extras) {
            if (!(item instanceof String name) || !parts.containsKey(name) || !owners.containsKey(name)
                    || !selected.containsAll(owners.get(name)))
                throw new IllegalArgumentException("Additional part must already belong exclusively to the selected slides: " + item);
            allowed.add((String) item);
        }
        JSONObject next = new JSONObject().put("sourceFilePath", source).put("filePath", output).put("editType", mode)
                .put("slides", new JSONArray(selected)).put("allowedParts", new JSONArray(allowed)).put("slideCount", ordered.size())
                .put("sourceHash", inputs.getString(source));
        if (contract != null && !contract.similar(next)) throw new IllegalArgumentException("Keep the original edit source, output and scope; do not broaden scope during repairs");
        if (inputs.has(output) && !source.equals(output) && !inputs.getString(output).equals(inputs.getString(source)))
            throw new IllegalArgumentException("Output already contains a different presentation; use the authoritative source or a new output filename");
        JSONArray inspected = new JSONArray();
        int remaining = 16000;
        for (int number : selected) {
            String name = ordered.get(number - 1);
            var nodes = xml(parts.get(name)).getElementsByTagNameNS(A, "t");
            JSONArray texts = new JSONArray();
            for (int i = 0; i < nodes.getLength(); i++) {
                String text = nodes.item(i).getTextContent();
                if (text.length() > remaining) throw new IllegalArgumentException("Selected text exceeds inspection limit; inspect fewer slides per edit");
                remaining -= text.length();
                texts.put(new JSONObject().put("index", i).put("text", text));
            }
            inspected.put(new JSONObject().put("slide", number).put("part", name).put("texts", texts));
        }
        contract = next;
        return new JSONObject().put("status", "prepared").put("edit", next)
                .put("inputSnapshot", root.relativize(original(source)).toString()).put("slides", inspected)
                .put("instructions", "Read pptx/references/editing.md. Use the returned inputSnapshot with scripts/edit.js; save the editing IIFE as build-deck.js, then call BuildPptx. Never reconstruct this deck with PptxGenJS. Unlisted package parts and text-mode formatting must remain identical to the original.");
    }

    void requirePrepared(String output, int slides) {
        if (contract == null) {
            if (inputs.has(output)) {
                try { recover(output, output); }
                catch (Exception e) { throw new IllegalStateException("Could not restore unprepared input", e); }
                throw new IllegalArgumentException("Existing presentation: call PreparePptxEdit before BuildPptx. Rebuilding the deck would discard existing edits.");
            }
        } else if (!output.equals(contract.getString("filePath")) || slides != contract.getInt("slideCount")) {
            throw new IllegalArgumentException("Keep the prepared output filename and original slide count");
        }
    }

    JSONObject verify(Path output) throws Exception {
        if (contract == null) return null;
        if (!contract.getString("sourceFilePath").equals(contract.getString("filePath"))
                && !contract.getString("sourceHash").equals(PptxWorkflow.hash(root.resolve(contract.getString("sourceFilePath")))))
            throw new IllegalArgumentException("Edit preservation failed: separate source presentation was modified");
        Map<String, byte[]> before = parts(original(contract.getString("sourceFilePath"))), after = parts(output);
        if (!before.keySet().equals(after.keySet())) throw new IllegalArgumentException("Edit preservation failed: package parts were added or removed");
        Set<String> allowed = new TreeSet<>();
        contract.getJSONArray("allowedParts").forEach(value -> allowed.add((String) value));
        List<String> changed = new ArrayList<>();
        for (String part : before.keySet()) {
            if (Arrays.equals(before.get(part), after.get(part))) continue;
            if (!allowed.contains(part)) throw new IllegalArgumentException("Edit preservation failed: unrelated part changed: " + part);
            if (contract.getString("editType").equals("text") && !sameExceptText(before.get(part), after.get(part)))
                throw new IllegalArgumentException("Edit preservation failed: formatting, objects or layout changed in " + part + "; text edits may change only existing text nodes");
            changed.add(part);
        }
        if (changed.isEmpty()) throw new IllegalArgumentException("No requested edit was saved; presentation is identical to the original");
        return new JSONObject().put("status", "passed").put("sourceHash", contract.getString("sourceHash"))
                .put("editType", contract.getString("editType")).put("slides", contract.getJSONArray("slides"))
                .put("changedParts", changed).put("unchangedParts", before.size() - changed.size());
    }

    void recoverOriginal() throws Exception {
        if (contract != null) {
            for (String name : new TreeSet<>(List.of(contract.getString("sourceFilePath"), contract.getString("filePath"))))
                recover(contract.getString("sourceFilePath"), name);
        }
    }

    void recoverSeparateSource() throws Exception {
        if (contract != null && !contract.getString("sourceFilePath").equals(contract.getString("filePath")))
            recover(contract.getString("sourceFilePath"), contract.getString("sourceFilePath"));
    }

    private void recover(String source, String name) throws Exception {
        Path output = root.resolve(name), ancestor = output.getParent();
        while (!Files.exists(ancestor)) ancestor = ancestor.getParent();
        if (!ancestor.toRealPath().startsWith(root.toRealPath())) throw new IllegalStateException("Recovery destination escapes the working directory");
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
                if (entry.isDirectory()) continue;
                byte[] bytes;
                try (var in = zip.getInputStream(entry)) { bytes = in.readNBytes(MAX_PART + 1); }
                if (bytes.length > MAX_PART || (total += bytes.length) > 500L * 1024 * 1024) throw new IllegalArgumentException("PPTX package exceeds inspection limits");
                if (result.put(entry.getName(), bytes) != null) throw new IllegalArgumentException("Duplicate PPTX package entry: " + entry.getName());
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
            if (target == null || !parts.containsKey(target) || result.contains(target)) throw new IllegalArgumentException("Invalid presentation slide order");
            result.add(target);
        }
        if (result.isEmpty() || result.size() > 100) throw new IllegalArgumentException("Expected 1 to 100 slides");
        return result;
    }

    private static String rels(String part) {
        int slash = part.lastIndexOf('/');
        return part.substring(0, slash + 1) + "_rels/" + part.substring(slash + 1) + ".rels";
    }

    private static Map<String, String> relationships(Map<String, byte[]> parts, String part) throws Exception {
        Map<String, String> result = new HashMap<>();
        if (!parts.containsKey(rels(part))) return result;
        var links = xml(parts.get(rels(part))).getElementsByTagNameNS(REL, "Relationship");
        for (int i = 0; i < links.getLength(); i++) {
            Element link = (Element) links.item(i);
            if ("External".equals(link.getAttribute("TargetMode"))) continue;
            String target = link.getAttribute("Target");
            Path parent = Path.of(part).getParent();
            Path resolved = (target.startsWith("/") ? Path.of(target.substring(1)) : (parent == null ? Path.of("") : parent).resolve(target)).normalize();
            if (resolved.startsWith("..") || resolved.isAbsolute()) throw new IllegalArgumentException("Invalid package relationship");
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
                if (!seen.add(part)) continue;
                owners.computeIfAbsent(part, key -> new TreeSet<>()).add(i + 1);
                if (parts.containsKey(rels(part))) owners.computeIfAbsent(rels(part), key -> new TreeSet<>()).add(i + 1);
                pending.addAll(relationships(parts, part).values());
            }
        }
        return owners;
    }

    private static Document xml(byte[] bytes) throws Exception {
        if (bytes == null) throw new IllegalArgumentException("Required PPTX XML part is missing");
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
                for (Node child = node.getFirstChild(); child != null; child = child.getNextSibling())
                    if (child.getNodeType() != Node.TEXT_NODE && child.getNodeType() != Node.CDATA_SECTION_NODE) return false;
                node.setTextContent("TEXT");
            }
        }
        return a.isEqualNode(b);
    }
}
