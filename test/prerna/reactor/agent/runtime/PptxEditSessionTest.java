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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

	@TempDir
	Path root;
	PptxEditSession edit;
	Map<String, byte[]> original;
	static final String FIRST = "ppt/slides/slide7.xml";
	static final String SECOND = "ppt/slides/slide2.xml";
	static final String REL = "http://schemas.openxmlformats.org/package/2006/relationships";
	static final String SLIDE = "<p:sld xmlns:p=\"http://schemas.openxmlformats.org/presentationml/2006/main\" xmlns:a=\"http://schemas.openxmlformats.org/drawingml/2006/main\"><p:cSld><p:spTree><p:sp><p:spPr><a:xfrm><a:off x=\"12345\" y=\"6789\"/></a:xfrm></p:spPr><p:txBody><a:p><a:r><a:rPr sz=\"3100\" b=\"1\"/><a:t>DOGS</a:t></a:r><a:r><a:rPr i=\"1\"/><a:t>Manual formatting</a:t></a:r></a:p></p:txBody></p:sp></p:spTree></p:cSld></p:sld>";

	@BeforeEach
	void setup() throws Exception {
		original = new HashMap<>();
		put("ppt/presentation.xml",
				"<p:presentation xmlns:p=\"http://schemas.openxmlformats.org/presentationml/2006/main\" xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\"><p:sldIdLst><p:sldId id=\"1\" r:id=\"r7\"/><p:sldId id=\"2\" r:id=\"r2\"/></p:sldIdLst></p:presentation>");
		put("ppt/_rels/presentation.xml.rels", "<Relationships xmlns=\"" + REL
				+ "\"><Relationship Id=\"r7\" Target=\"slides/slide7.xml\"/><Relationship Id=\"r2\" Target=\"slides/slide2.xml\"/></Relationships>");
		put(FIRST, SLIDE);
		put(SECOND, SLIDE.replace("DOGS", "Second slide"));
		put("ppt/slides/_rels/slide7.xml.rels", "<Relationships xmlns=\"" + REL
				+ "\"><Relationship Id=\"media\" Target=\"../media/shared.png\"/></Relationships>");
		put("ppt/slides/_rels/slide2.xml.rels", "<Relationships xmlns=\"" + REL
				+ "\"><Relationship Id=\"media\" Target=\"../media/shared.png\"/></Relationships>");
		put("ppt/media/shared.png", "unchanged image bytes");
		put("ppt/notesSlides/notesSlide7.xml", "<notes>Manual speaker notes</notes>");
		put("ppt/charts/chart1.xml", "<chart>Chart cache</chart>");
		put("ppt/embeddings/data.xlsx", "embedded workbook bytes");
		save(root.resolve("deck.pptx"), original);
		edit = new PptxEditSession(root, root.resolve(".semoss/pptx-workflow/test"));
		edit.capture();
	}

	void put(String name, String value) {
		original.put(name, value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
	}

	static void save(Path file, Map<String, byte[]> parts) throws Exception {
		try (var zip = new ZipOutputStream(Files.newOutputStream(file))) {
			for (var part : parts.entrySet()) {
				zip.putNextEntry(new ZipEntry(part.getKey()));
				zip.write(part.getValue());
				zip.closeEntry();
			}
		}
	}

	JSONObject prepare() throws Exception {
		return edit.prepare("deck.pptx", "deck.pptx", Map.of("slides", List.of(1)));
	}

	Map<String, byte[]> changed(String part, String content) {
		Map<String, byte[]> parts = new HashMap<>(original);
		parts.put(part, content.getBytes(java.nio.charset.StandardCharsets.UTF_8));
		return parts;
	}

	@Test
	void inspectionUsesPresentationOrderAndRunStartSnapshot() throws Exception {
		save(root.resolve("deck.pptx"), changed(FIRST, SLIDE.replace("DOGS", "overwritten early")));
		JSONObject report = prepare();
		assertEquals(FIRST, report.getJSONArray("slides").getJSONObject(0).getString("part"));
		assertEquals("DOGS", report.getJSONArray("slides").getJSONObject(0).getJSONArray("texts").getJSONObject(0)
				.getString("text"));
		assertTrue(Files.isRegularFile(root.resolve(report.getString("inputSnapshot"))));
	}

	@Test
	void textEditPreservesManualFormattingAndEveryOtherPart() throws Exception {
		prepare();
		save(root.resolve("deck.pptx"), changed(FIRST, SLIDE.replace("DOGS", "CATS")));
		var report = edit.verify(root.resolve("deck.pptx"));
		assertEquals(List.of(FIRST), report.getJSONArray("changedParts").toList());
		assertEquals(original.size() - 1, report.getInt("unchangedParts"));
	}

	@Test
	void textModeFormattingChangesAreProposalsRatherThanBuildFailures() throws Exception {
		prepare();
		for (String xml : List.of(SLIDE.replace("12345", "88888"), SLIDE.replace("3100", "3200"),
				SLIDE.replace("<a:rPr i=\"1\"/>", ""))) {
			save(root.resolve("deck.pptx"), changed(FIRST, xml.replace("DOGS", "CATS")));
			assertEquals("proposal", edit.verify(root.resolve("deck.pptx")).getString("disposition"));
		}
	}

	@Test
	void unrelatedAndUnassignedChangesAreReportedWithoutDiscardingTheEdit() throws Exception {
		prepare();
		for (String part : List.of(SECOND, "ppt/media/shared.png", "ppt/notesSlides/notesSlide7.xml",
				"ppt/charts/chart1.xml", "ppt/embeddings/data.xlsx")) {
			save(root.resolve("deck.pptx"), changed(part, part.equals(SECOND) ? SLIDE : "<changed/>"));
			var result = edit.verify(root.resolve("deck.pptx"));
			assertEquals("proposal", result.getString("disposition"));
			assertTrue(result.getJSONArray("changedParts").toList().contains(part));
		}
	}

	@Test
	void unchangedOutputAndNewBrokenLinksRemainFailures() throws Exception {
		prepare();
		assertThrows(IllegalArgumentException.class, () -> edit.verify(root.resolve("deck.pptx")));
		save(root.resolve("deck.pptx"), changed("extra.xml", "<extra/>"));
		assertEquals("proposal", edit.verify(root.resolve("deck.pptx")).getString("disposition"));
		var removed = new HashMap<>(original); removed.remove("ppt/media/shared.png");
		save(root.resolve("deck.pptx"), removed);
		assertTrue(assertThrows(IllegalArgumentException.class, () -> edit.verify(root.resolve("deck.pptx")))
				.getMessage().contains("missing linked files"));
	}

	@Test
	void preparationCanBeCorrectedAndSharedAssetsReportAffectedSlides() throws Exception {
		prepare();
		edit.prepare("deck.pptx", "deck.pptx", Map.of("slides", List.of(1), "editType", "slides",
				"additionalParts", List.of("ppt/media/shared.png")));
		save(root.resolve("deck.pptx"), changed("ppt/media/shared.png", "requested updated image"));
		var report = edit.verify(root.resolve("deck.pptx"));
		assertEquals(List.of(2), report.getJSONArray("outsideRequestedSlides").toList());
		assertEquals("proposal", report.getString("disposition"));
		edit.prepare("deck.pptx", "deck.pptx", Map.of("slides", List.of(1, 2), "editType", "slides"));
		assertEquals("passed", edit.verify(root.resolve("deck.pptx")).getString("status"));
	}

	@Test
	void linkedChartsWorkWithoutAnAdditionalPartsWhitelist() throws Exception {
		put("ppt/slides/_rels/slide7.xml.rels", "<Relationships xmlns=\"" + REL
				+ "\"><Relationship Id=\"chart\" Target=\"/ppt/charts/chart1.xml\"/></Relationships>");
		save(root.resolve("deck.pptx"), original); edit.capture();
		var inspection = edit.prepare("deck.pptx", "deck.pptx", Map.of("slides", List.of(1), "editType", "slides"));
		assertTrue(inspection.getJSONArray("slides").getJSONObject(0).getJSONArray("linkedParts").toString().contains("chart1.xml"));
		save(root.resolve("deck.pptx"), changed("ppt/charts/chart1.xml", "<chart>Red</chart>"));
		var report = edit.verify(root.resolve("deck.pptx"));
		assertEquals("revision", report.getString("disposition"));
		assertEquals(List.of(1), report.getJSONArray("affectedSlides").toList());
	}

	@Test
	void newMediaAndRemovedUnusedPartsAreAssessedAgainstBothGraphs() throws Exception {
		edit.prepare("deck.pptx", "deck.pptx", Map.of("slides", List.of(1), "editType", "slides"));
		var next = changed("ppt/slides/_rels/slide7.xml.rels", "<Relationships xmlns=\"" + REL
				+ "\"><Relationship Id=\"media\" Target=\"../media/new.png\"/></Relationships>");
		next.put("ppt/media/new.png", new byte[] { 1, 2, 3 });
		save(root.resolve("deck.pptx"), next);
		var report = edit.verify(root.resolve("deck.pptx"));
		assertEquals("revision", report.getString("disposition"));
		assertEquals(List.of("ppt/media/new.png"), report.getJSONArray("addedParts").toList());
	}

	@Test
	void packageRelationshipUrisSupportEncodedSpacesAndIgnoreExternalLinks() throws Exception {
		edit.prepare("deck.pptx", "deck.pptx", Map.of("slides", List.of(1), "editType", "slides"));
		var next = changed("ppt/slides/_rels/slide7.xml.rels", "<Relationships xmlns=\"" + REL
				+ "\"><Relationship Id=\"media\" Target=\"../media/new%20image.png\"/>"
				+ "<Relationship Id=\"web\" TargetMode=\"External\" Target=\"https://example.com/page\"/></Relationships>");
		next.put("ppt/media/new image.png", new byte[] { 3, 2, 1 });
		save(root.resolve("deck.pptx"), next);
		assertEquals("revision", edit.verify(root.resolve("deck.pptx")).getString("disposition"));
	}

	@Test
	void xmlReserializationDoesNotCreateFalseOutsideSlideChangesOrSuccessfulNoOps() throws Exception {
		prepare();
		var next = changed(SECOND, new String(original.get(SECOND)).replace("<a:off x=\"12345\" y=\"6789\"/>", "<a:off y=\"6789\" x=\"12345\"/>").replace("><", ">\n<"));
		save(root.resolve("deck.pptx"), next);
		assertThrows(IllegalArgumentException.class, () -> edit.verify(root.resolve("deck.pptx")));
		next.put(FIRST, SLIDE.replace("DOGS", "CATS").getBytes());
		save(root.resolve("deck.pptx"), next);
		var report = edit.verify(root.resolve("deck.pptx"));
		assertEquals("revision", report.getString("disposition"));
		assertEquals(List.of(FIRST), report.getJSONArray("contentChangedParts").toList());
	}

	@Test
	void layoutModePermitsOnlyScopedSlideChanges() throws Exception {
		edit.prepare("deck.pptx", "deck.pptx", Map.of("slides", List.of(1), "editType", "slides"));
		save(root.resolve("deck.pptx"), changed(FIRST, SLIDE.replace("12345", "55555")));
		assertEquals("passed", edit.verify(root.resolve("deck.pptx")).getString("status"));
	}

	@Test
	void recoveryAndResumeRetainTheOriginalBaseline() throws Exception {
		prepare();
		var contract = new JSONObject(edit.contract().toString());
		save(root.resolve("deck.pptx"), changed(FIRST, SLIDE.replace("DOGS", "CATS")));
		var resumed = new PptxEditSession(root, root.resolve(".semoss/pptx-workflow/test"));
		resumed.restore(contract);
		assertEquals("passed", resumed.verify(root.resolve("deck.pptx")).getString("status"));
		resumed.recoverOriginal();
		for (var part : PptxEditSession.parts(root.resolve("deck.pptx")).entrySet()) {
			assertArrayEquals(original.get(part.getKey()), part.getValue());
		}
	}

	@Test
	void snapshotTamperingAndXxeAreRejected() throws Exception {
		var prepared = prepare();
		Files.writeString(root.resolve(prepared.getString("inputSnapshot")), "tampered");
		assertThrows(IllegalStateException.class, () -> edit.verify(root.resolve("deck.pptx")));
		var bad = new HashMap<>(original);
		bad.put("ppt/presentation.xml", "<!DOCTYPE x [<!ENTITY e SYSTEM 'file:///etc/passwd'>]><x>&e;</x>".getBytes());
		assertThrows(Exception.class, () -> PptxEditSession.orderedSlides(bad));
	}

	@Test
	void separateOutputPreservesAndRecoversTheSource() throws Exception {
		edit.prepare("deck.pptx", "copy.pptx", Map.of("slides", List.of(1)));
		save(root.resolve("copy.pptx"), changed(FIRST, SLIDE.replace("DOGS", "CATS")));
		assertEquals("passed", edit.verify(root.resolve("copy.pptx")).getString("status"));
		Files.writeString(root.resolve("deck.pptx"), "accidentally overwritten source");
		assertTrue(assertThrows(IllegalArgumentException.class, () -> edit.verify(root.resolve("copy.pptx")))
				.getMessage().contains("separate source"));
		edit.recoverOriginal();
		assertEquals(PptxWorkflow.hash(root.resolve("deck.pptx")), PptxWorkflow.hash(root.resolve("copy.pptx")));
		assertArrayEquals(original.get(FIRST), PptxEditSession.parts(root.resolve("deck.pptx")).get(FIRST));
	}

	@Test
	void resumeBeforeFirstToolRetainsBaselineAndCanDeliverAnUnpreparedProposal() throws Exception {
		var operations = new FakeOperations(); workflow(operations);
		var resumed = new PptxWorkflow(root, root.resolve(".semoss/pptx-workflow/controller"), 6, operations);
		var restore = PptxWorkflow.class.getDeclaredMethod("restore"); restore.setAccessible(true); restore.invoke(resumed);
		resumed.build(buildArgs(), 1);
		assertNull(resumed.completionError());
		assertEquals("proposal", resumed.snapshot().getJSONObject("artifact").getString("disposition"));
		assertEquals(List.of(1, 2), operations.scope);
	}

	@Test
	void workflowDeliversBroaderChangesAsProposalAndPersistsAssessmentOnResume() throws Exception {
		var operations = new FakeOperations(); operations.rebuild = true;
		var workflow = workflow(operations);
		workflow.prepareEdit(Map.of("filePath", "deck.pptx", "slides", List.of(1)));
		workflow.build(buildArgs(), 2);
		assertNull(workflow.completionError());
		assertEquals(List.of(1, 2), operations.scope);
		assertEquals("available", workflow.snapshot().getJSONObject("artifact").getString("status"));
		assertEquals("proposal", workflow.snapshot().getJSONObject("artifact").getString("disposition"));
		assertTrue(workflow.completionWarning().contains("separate proposed revision"));
		assertFalse(java.util.Arrays.equals(original.get(SECOND), PptxEditSession.parts(root.resolve("deck.pptx")).get(SECOND)));
		var resumed = new PptxWorkflow(root, root.resolve(".semoss/pptx-workflow/controller"), 6, operations);
		var restore = PptxWorkflow.class.getDeclaredMethod("restore"); restore.setAccessible(true); restore.invoke(resumed);
		assertEquals(workflow.snapshot().getJSONObject("artifact").toString(), resumed.snapshot().getJSONObject("artifact").toString());
	}

	@Test
	void oldOfficePagesCannotSilentlyImportProposalsOverTheAcceptedDeck() throws Exception {
		var operations = new FakeOperations(); operations.rebuild = true;
		var workflow = workflow(operations);
		workflow.configureDelivery(Map.of("pptx_edit_file", "deck.pptx"));
		workflow.prepareEdit(Map.of("filePath", "deck.pptx", "slides", List.of(1)));
		workflow.build(buildArgs(), 2);
		assertTrue(workflow.completionError().contains("Refresh the Microsoft Office app"));
		assertEquals("available", workflow.snapshot().getJSONObject("artifact").getString("status"));
		assertEquals("proposal", workflow.snapshot().getJSONObject("artifact").getString("disposition"));
	}

	@Test
	void updatedOfficeCanImportProposalsSeparately() throws Exception {
		var operations = new FakeOperations(); operations.rebuild = true;
		var workflow = workflow(operations);
		workflow.configureDelivery(Map.of("pptx_edit_file", "deck.pptx", "pptx_edit_proposals", true));
		workflow.prepareEdit(Map.of("filePath", "deck.pptx", "slides", List.of(1)));
		workflow.build(buildArgs(), 2);
		assertNull(workflow.completionError());
		assertEquals("proposal", workflow.snapshot().getJSONObject("artifact").getString("disposition"));
	}

	@Test
	void unresolvedSignificantVisualFindingsKeepTheEditAsAProposal() throws Exception {
		var operations = new FakeOperations(); operations.major = true;
		var workflow = workflow(operations);
		workflow.prepareEdit(Map.of("filePath", "deck.pptx", "slides", List.of(1)));
		workflow.build(buildArgs(), 2);
		assertFalse(workflow.isTerminal());
		workflow.modelStopped("Stopped repairing");
		assertNull(workflow.completionError());
		assertEquals("proposal", workflow.snapshot().getJSONObject("artifact").getString("disposition"));
		assertTrue(workflow.completionWarning().contains("separate proposed revision"));
	}

	@Test
	void cancellationRestoresOriginalWhilePreservingCancelledStatus() throws Exception {
		var operations = new FakeOperations();
		operations.cancel = true;
		var workflow = workflow(operations);
		String hash = PptxWorkflow.hash(root.resolve("deck.pptx"));
		workflow.prepareEdit(Map.of("filePath", "deck.pptx", "slides", List.of(1)));
		assertThrows(prerna.reactor.agent.exceptions.AgentCancelledException.class,
				() -> workflow.build(buildArgs(), 2));
		assertEquals(hash, PptxWorkflow.hash(root.resolve("deck.pptx")));
		assertFalse(workflow.isTerminal());
	}

	@Test
	void structuredInspectionReturnsObjectIdentityGeometryAndRunStyles() throws Exception {
		String xml = SLIDE.replace("<p:spPr>", "<p:nvSpPr><p:cNvPr id=\"9\" name=\"Title\"/></p:nvSpPr><p:spPr>");
		save(root.resolve("deck.pptx"), changed(FIRST, xml));
		edit.capture();
		JSONObject slide = prepare().getJSONArray("slides").getJSONObject(0);
		JSONObject object = slide.getJSONArray("objects").getJSONObject(0);
		assertEquals("9", object.getString("objectId"));
		assertEquals("Title", object.getString("name"));
		assertEquals("12345", object.getJSONObject("geometry").getString("x"));
		assertEquals(List.of(0, 1), object.getJSONArray("textIndexes").toList());
		assertEquals("9", slide.getJSONArray("texts").getJSONObject(0).getString("objectId"));
		assertEquals("3100", slide.getJSONArray("texts").getJSONObject(0).getJSONObject("formatting").getString("sz"));
		assertEquals("inherited", slide.getJSONObject("background").getString("kind"));
		assertEquals(1, prepare().getJSONArray("slides").length());
	}

	Map<String, Object> textOperation() {
		return Map.of("type", "replaceText", "part", FIRST, "objectId", "9", "index", 0, "oldText", "DOGS", "newText", "CATS");
	}

	@Test
	void structuredPlanRejectsUnpreparedOutOfScopeAndWrongModeWithoutBuilding() throws Exception {
		var operations = new FakeOperations();
		var workflow = workflow(operations);
		assertThrows(IllegalArgumentException.class, () -> workflow.applyEdits(Map.of("operations", List.of(textOperation())), 1));
		workflow.prepareEdit(Map.of("filePath", "deck.pptx", "slides", List.of(1)));
		String originalHash = PptxWorkflow.hash(root.resolve("deck.pptx"));
		for (var op : List.of(Map.of("type", "setBackground", "part", FIRST, "color", "000000"),
				Map.of("type", "setBackground", "part", SECOND, "color", "000000")))
			assertThrows(IllegalArgumentException.class, () -> workflow.applyEdits(Map.of("operations", List.of(op)), 2));
		assertEquals(0, operations.builds);
		assertEquals(originalHash, PptxWorkflow.hash(root.resolve("deck.pptx")));
	}

	@Test
	void structuredPlanUsesExistingBuildPreservationAndSelectedSlideReview() throws Exception {
		var operations = new FakeOperations();
		var workflow = workflow(operations);
		workflow.prepareEdit(Map.of("filePath", "deck.pptx", "slides", List.of(1)));
		workflow.applyEdits(Map.of("operations", List.of(textOperation())), 2);
		assertNull(workflow.completionError());
		assertTrue(workflow.isTerminal());
		assertEquals(List.of(1), operations.scope);
		assertEquals("passed", workflow.snapshot().getJSONObject("validation").getJSONObject("preservation").getString("status"));
		assertTrue(Files.readString(root.resolve("pptx-edit-controller.js")).contains("structured-edit.js"));
	}

	@Test
	void structuredPlanAlsoDeliversUnexpectedChangesAsASeparateProposal() throws Exception {
		var operations = new FakeOperations(); operations.rebuild = true;
		var workflow = workflow(operations);
		workflow.prepareEdit(Map.of("filePath", "deck.pptx", "slides", List.of(1)));
		JSONObject result = workflow.applyEdits(Map.of("operations", List.of(textOperation())), 2);
		assertFalse(result.has("buildError"));
		assertEquals("proposal", result.getJSONObject("artifact").getString("disposition"));
		assertEquals(1, operations.reviews);
	}

	PptxWorkflow workflow(FakeOperations operations) throws Exception {
		Files.writeString(root.resolve("build-deck.js"), "test generator");
		var workflow = new PptxWorkflow(root, root.resolve(".semoss/pptx-workflow/controller"), 6, operations);
		workflow.captureInputs();
		return workflow;
	}

	Map<String, Object> buildArgs() {
		return Map.of("filePath", "deck.pptx", "generator", "build-deck.js", "expectedSlides", 2);
	}

	final class FakeOperations implements PptxWorkflow.Operations {
		int builds, reviews;
		boolean rebuild, cancel, major;
		List<Integer> scope;

		@Override
		public JSONObject build(Map<String, Object> args) throws Exception {
			builds++;
			assertTrue(args.containsKey("inputSnapshot"));
			var next = changed(FIRST, SLIDE.replace("DOGS", "CATS"));
			if (rebuild) {
				next.put(SECOND, SLIDE.replace("3100", "9900").getBytes());
			}
			save(root.resolve("deck.pptx"), next);
			if (cancel) {
				throw new prerna.reactor.agent.exceptions.AgentCancelledException();
			}
			return new JSONObject().put("ok", true).put("slides", 2).put("warnings", new JSONArray())
					.put("sourceHash", PptxWorkflow.hash(root.resolve("deck.pptx")))
					.put("generatorHash", PptxWorkflow.hash(root.resolve((String) args.get("generator"))));
		}

		@Override
		public JSONObject review(String file, List<Integer> slides, String instructions, String engine)
				throws Exception {
			reviews++;
			scope = slides;
			return new JSONObject().put("filePath", file).put("sourceHash", PptxWorkflow.hash(root.resolve(file)))
					.put("slideCount", 2).put("status", "complete").put("verdict", "pass").put("sourceChanged", false)
					.put("requestedSlides", slides).put("reviewedSlides", slides)
					.put("unreviewedSlides", new JSONArray()).put("issues", major
							? new JSONArray().put(new JSONObject().put("slide", 1).put("severity", "major").put("evidence", "Text is clipped"))
							: new JSONArray());
		}
	}
}
