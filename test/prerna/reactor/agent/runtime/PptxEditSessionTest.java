package prerna.reactor.agent.runtime;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PptxEditSessionTest {
    @TempDir Path root;
    PptxEditSession edit;
    Map<String, byte[]> original;
    static final String FIRST = "ppt/slides/slide7.xml";
    static final String SECOND = "ppt/slides/slide2.xml";
    static final String REL = "http://schemas.openxmlformats.org/package/2006/relationships";
    static final String SLIDE = "<p:sld xmlns:p=\"http://schemas.openxmlformats.org/presentationml/2006/main\" xmlns:a=\"http://schemas.openxmlformats.org/drawingml/2006/main\"><p:cSld><p:spTree><p:sp><p:spPr><a:xfrm><a:off x=\"12345\" y=\"6789\"/></a:xfrm></p:spPr><p:txBody><a:p><a:r><a:rPr sz=\"3100\" b=\"1\"/><a:t>DOGS</a:t></a:r><a:r><a:rPr i=\"1\"/><a:t>Manual formatting</a:t></a:r></a:p></p:txBody></p:sp></p:spTree></p:cSld></p:sld>";

    @BeforeEach void setup() throws Exception {
        original = new HashMap<>();
        put("ppt/presentation.xml", "<p:presentation xmlns:p=\"http://schemas.openxmlformats.org/presentationml/2006/main\" xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\"><p:sldIdLst><p:sldId id=\"1\" r:id=\"r7\"/><p:sldId id=\"2\" r:id=\"r2\"/></p:sldIdLst></p:presentation>");
        put("ppt/_rels/presentation.xml.rels", "<Relationships xmlns=\"" + REL + "\"><Relationship Id=\"r7\" Target=\"slides/slide7.xml\"/><Relationship Id=\"r2\" Target=\"slides/slide2.xml\"/></Relationships>");
        put(FIRST, SLIDE); put(SECOND, SLIDE.replace("DOGS", "Second slide"));
        put("ppt/slides/_rels/slide7.xml.rels", "<Relationships xmlns=\"" + REL + "\"><Relationship Id=\"media\" Target=\"../media/shared.png\"/></Relationships>");
        put("ppt/slides/_rels/slide2.xml.rels", "<Relationships xmlns=\"" + REL + "\"><Relationship Id=\"media\" Target=\"../media/shared.png\"/></Relationships>");
        put("ppt/media/shared.png", "unchanged image bytes"); put("ppt/notesSlides/notesSlide7.xml", "manual speaker notes");
        put("ppt/charts/chart1.xml", "chart cache"); put("ppt/embeddings/data.xlsx", "embedded workbook bytes");
        save(root.resolve("deck.pptx"), original);
        edit = new PptxEditSession(root, root.resolve(".semoss/pptx-workflow/test")); edit.capture();
    }

    void put(String name, String value) { original.put(name, value.getBytes(java.nio.charset.StandardCharsets.UTF_8)); }
    static void save(Path file, Map<String, byte[]> parts) throws Exception {
        try (var zip = new ZipOutputStream(Files.newOutputStream(file))) {
            for (var part : parts.entrySet()) { zip.putNextEntry(new ZipEntry(part.getKey())); zip.write(part.getValue()); zip.closeEntry(); }
        }
    }
    JSONObject prepare() throws Exception { return edit.prepare("deck.pptx", "deck.pptx", Map.of("slides", List.of(1))); }
    Map<String, byte[]> changed(String part, String content) {
        Map<String, byte[]> parts = new HashMap<>(original); parts.put(part, content.getBytes(java.nio.charset.StandardCharsets.UTF_8)); return parts;
    }

    @Test void inspectionUsesPresentationOrderAndRunStartSnapshot() throws Exception {
        save(root.resolve("deck.pptx"), changed(FIRST, SLIDE.replace("DOGS", "overwritten early")));
        JSONObject report = prepare();
        assertEquals(FIRST, report.getJSONArray("slides").getJSONObject(0).getString("part"));
        assertEquals("DOGS", report.getJSONArray("slides").getJSONObject(0).getJSONArray("texts").getJSONObject(0).getString("text"));
        assertTrue(Files.isRegularFile(root.resolve(report.getString("inputSnapshot"))));
    }

    @Test void textEditPreservesManualFormattingAndEveryOtherPart() throws Exception {
        prepare(); save(root.resolve("deck.pptx"), changed(FIRST, SLIDE.replace("DOGS", "CATS")));
        var report = edit.verify(root.resolve("deck.pptx"));
        assertEquals(List.of(FIRST), report.getJSONArray("changedParts").toList());
        assertEquals(original.size() - 1, report.getInt("unchangedParts"));
    }

    @Test void textModeRejectsGeometryFontAndShapeChangesOnSelectedSlide() throws Exception {
        prepare();
        for (String xml : List.of(SLIDE.replace("12345", "88888"), SLIDE.replace("3100", "3200"), SLIDE.replace("<a:rPr i=\"1\"/>", ""))) {
            save(root.resolve("deck.pptx"), changed(FIRST, xml.replace("DOGS", "CATS")));
            assertTrue(assertThrows(IllegalArgumentException.class, () -> edit.verify(root.resolve("deck.pptx"))).getMessage().contains("formatting"));
        }
    }

    @Test void unrelatedSlideNotesMediaWorkbookAndChartChangesAreRejected() throws Exception {
        prepare();
        for (String part : List.of(SECOND, "ppt/media/shared.png", "ppt/notesSlides/notesSlide7.xml", "ppt/charts/chart1.xml", "ppt/embeddings/data.xlsx")) {
            save(root.resolve("deck.pptx"), changed(part, "unrequested change"));
            assertTrue(assertThrows(IllegalArgumentException.class, () -> edit.verify(root.resolve("deck.pptx"))).getMessage().contains(part));
        }
    }

    @Test void entryAdditionRemovalAndUnchangedOutputAreRejected() throws Exception {
        prepare();
        assertThrows(IllegalArgumentException.class, () -> edit.verify(root.resolve("deck.pptx")));
        var added = changed("extra.xml", "extra"); save(root.resolve("deck.pptx"), added);
        assertThrows(IllegalArgumentException.class, () -> edit.verify(root.resolve("deck.pptx")));
        var removed = new HashMap<>(original); removed.remove("ppt/media/shared.png"); save(root.resolve("deck.pptx"), removed);
        assertThrows(IllegalArgumentException.class, () -> edit.verify(root.resolve("deck.pptx")));
    }

    @Test void scopeIsImmutableAndSharedAssetsRequireAllAffectedSlides() throws Exception {
        prepare();
        assertThrows(IllegalArgumentException.class, () -> edit.prepare("deck.pptx", "deck.pptx", Map.of("slides", List.of(1, 2))));
        var shared = new PptxEditSession(root, root.resolve(".semoss/pptx-workflow/shared")); shared.capture();
        assertThrows(IllegalArgumentException.class, () -> shared.prepare("deck.pptx", "deck.pptx", Map.of("slides", List.of(1), "editType", "slides", "additionalParts", List.of("ppt/media/shared.png"))));
        shared.prepare("deck.pptx", "deck.pptx", Map.of("slides", List.of(1, 2), "editType", "slides", "additionalParts", List.of("ppt/media/shared.png")));
        save(root.resolve("deck.pptx"), changed("ppt/media/shared.png", "requested updated image"));
        assertEquals("passed", shared.verify(root.resolve("deck.pptx")).getString("status"));
    }

    @Test void layoutModePermitsOnlyScopedSlideChanges() throws Exception {
        edit.prepare("deck.pptx", "deck.pptx", Map.of("slides", List.of(1), "editType", "slides"));
        save(root.resolve("deck.pptx"), changed(FIRST, SLIDE.replace("12345", "55555")));
        assertEquals("passed", edit.verify(root.resolve("deck.pptx")).getString("status"));
    }

    @Test void recoveryAndResumeRetainTheOriginalBaseline() throws Exception {
        prepare(); var contract = new JSONObject(edit.contract().toString());
        save(root.resolve("deck.pptx"), changed(FIRST, SLIDE.replace("DOGS", "CATS")));
        var resumed = new PptxEditSession(root, root.resolve(".semoss/pptx-workflow/test")); resumed.restore(contract);
        assertEquals("passed", resumed.verify(root.resolve("deck.pptx")).getString("status"));
        resumed.recoverOriginal();
        for (var part : PptxEditSession.parts(root.resolve("deck.pptx")).entrySet()) assertArrayEquals(original.get(part.getKey()), part.getValue());
    }

    @Test void snapshotTamperingAndXxeAreRejected() throws Exception {
        var prepared = prepare(); Files.writeString(root.resolve(prepared.getString("inputSnapshot")), "tampered");
        assertThrows(IllegalStateException.class, () -> edit.verify(root.resolve("deck.pptx")));
        var bad = new HashMap<>(original); bad.put("ppt/presentation.xml", "<!DOCTYPE x [<!ENTITY e SYSTEM 'file:///etc/passwd'>]><x>&e;</x>".getBytes());
        assertThrows(Exception.class, () -> PptxEditSession.orderedSlides(bad));
    }

    @Test void separateOutputPreservesAndRecoversTheSource() throws Exception {
        edit.prepare("deck.pptx", "copy.pptx", Map.of("slides", List.of(1)));
        save(root.resolve("copy.pptx"), changed(FIRST, SLIDE.replace("DOGS", "CATS")));
        assertEquals("passed", edit.verify(root.resolve("copy.pptx")).getString("status"));
        Files.writeString(root.resolve("deck.pptx"), "accidentally overwritten source");
        assertTrue(assertThrows(IllegalArgumentException.class, () -> edit.verify(root.resolve("copy.pptx"))).getMessage().contains("separate source"));
        edit.recoverOriginal();
        assertEquals(PptxWorkflow.hash(root.resolve("deck.pptx")), PptxWorkflow.hash(root.resolve("copy.pptx")));
        assertArrayEquals(original.get(FIRST), PptxEditSession.parts(root.resolve("deck.pptx")).get(FIRST));
    }

    @Test void resumeBeforeFirstToolKeepsTheCapturedInputs() throws Exception {
        var operations = new FakeOperations(); var workflow = workflow(operations);
        var resumed = new PptxWorkflow(root, root.resolve(".semoss/pptx-workflow/controller"), 6, operations);
        var restore = PptxWorkflow.class.getDeclaredMethod("restore"); restore.setAccessible(true); restore.invoke(resumed);
        assertTrue(resumed.build(buildArgs(), 1).getString("buildError").contains("PreparePptxEdit"));
        assertEquals(0, operations.builds);
    }

    @Test void workflowBlocksUnpreparedRebuildBeforeExecutingCode() throws Exception {
        var operations = new FakeOperations(); var workflow = workflow(operations);
        String originalHash = PptxWorkflow.hash(root.resolve("deck.pptx"));
        Files.writeString(root.resolve("deck.pptx"), "accidental direct write before preparation");
        var result = workflow.build(buildArgs(), 1);
        assertEquals(0, operations.builds); assertTrue(result.getString("buildError").contains("PreparePptxEdit"));
        assertEquals(originalHash, PptxWorkflow.hash(root.resolve("deck.pptx")));
        workflow.prepareEdit(Map.of("filePath", "deck.pptx", "slides", List.of(1)));
        workflow.build(buildArgs(), 3);
        assertNull(workflow.completionError()); assertEquals(1, operations.builds);
        assertEquals(List.of(1), operations.scope);
        assertEquals("passed", workflow.snapshot().getJSONObject("validation").getJSONObject("preservation").getString("status"));
    }

    @Test void workflowRejectsReconstructionAndRestoresOriginalWithoutReview() throws Exception {
        var operations = new FakeOperations(); operations.rebuild = true; var workflow = workflow(operations);
        String hash = PptxWorkflow.hash(root.resolve("deck.pptx"));
        workflow.prepareEdit(Map.of("filePath", "deck.pptx", "slides", List.of(1)));
        var result = workflow.build(buildArgs(), 2);
        assertTrue(result.getString("buildError").contains("preservation failed"));
        assertEquals(hash, PptxWorkflow.hash(root.resolve("deck.pptx"))); assertEquals(0, operations.reviews);
        workflow.build(buildArgs(), 4);
        assertNotNull(workflow.completionError()); assertEquals("unavailable", workflow.snapshot().getJSONObject("artifact").getString("status"));
    }

    @Test void cancellationRestoresOriginalWhilePreservingCancelledStatus() throws Exception {
        var operations = new FakeOperations(); operations.cancel = true; var workflow = workflow(operations);
        String hash = PptxWorkflow.hash(root.resolve("deck.pptx"));
        workflow.prepareEdit(Map.of("filePath", "deck.pptx", "slides", List.of(1)));
        assertThrows(prerna.reactor.agent.exceptions.AgentCancelledException.class, () -> workflow.build(buildArgs(), 2));
        assertEquals(hash, PptxWorkflow.hash(root.resolve("deck.pptx"))); assertFalse(workflow.isTerminal());
    }

    PptxWorkflow workflow(FakeOperations operations) throws Exception {
        Files.writeString(root.resolve("build-deck.js"), "test generator");
        var workflow = new PptxWorkflow(root, root.resolve(".semoss/pptx-workflow/controller"), 6, operations);
        workflow.captureInputs(); return workflow;
    }
    Map<String, Object> buildArgs() { return Map.of("filePath", "deck.pptx", "generator", "build-deck.js", "expectedSlides", 2); }
    final class FakeOperations implements PptxWorkflow.Operations {
        int builds, reviews; boolean rebuild, cancel; List<Integer> scope;
        public JSONObject build(Map<String, Object> args) throws Exception {
            builds++; assertTrue(args.containsKey("inputSnapshot"));
            var next = changed(FIRST, SLIDE.replace("DOGS", "CATS"));
            if (rebuild) next.put(SECOND, SLIDE.replace("3100", "9900").getBytes());
            save(root.resolve("deck.pptx"), next);
            if (cancel) throw new prerna.reactor.agent.exceptions.AgentCancelledException();
            return new JSONObject().put("ok", true).put("slides", 2).put("warnings", new JSONArray())
                    .put("sourceHash", PptxWorkflow.hash(root.resolve("deck.pptx"))).put("generatorHash", PptxWorkflow.hash(root.resolve("build-deck.js")));
        }
        public JSONObject review(String file, List<Integer> slides, String instructions, String engine) throws Exception {
            reviews++; scope = slides;
            return new JSONObject().put("filePath", file).put("sourceHash", PptxWorkflow.hash(root.resolve(file))).put("slideCount", 2)
                    .put("status", "complete").put("verdict", "pass").put("sourceChanged", false).put("requestedSlides", slides)
                    .put("reviewedSlides", slides).put("unreviewedSlides", new JSONArray()).put("issues", new JSONArray());
        }
    }
}
