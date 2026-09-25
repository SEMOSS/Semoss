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
package prerna.util.pptx;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

import java.awt.Color;
import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.ZipFile;

import javax.imageio.ImageIO;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.poi.xslf.usermodel.XMLSlideShow;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PptxInspectionUnitTests {
    @TempDir Path root;

    private Path deck(int count, int hidden) throws Exception {
        Path path = root.resolve("deck.pptx");
        try (XMLSlideShow ppt = new XMLSlideShow()) {
            for (int i = 1; i <= count; i++) {
                var slide = ppt.createSlide();
                slide.createTextBox().setText("Original slide " + i);
                if (i == hidden) slide.getXmlObject().setShow(false);
            }
            try (var out = Files.newOutputStream(path)) { ppt.write(out); }
        }
        return path;
    }

    private byte[] pdf(int count) throws Exception {
        try (PDDocument doc = new PDDocument(); ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            for (int i = 0; i < count; i++) {
                PDPage page = new PDPage(new PDRectangle(720, 405));
                doc.addPage(page);
                try (PDPageContentStream content = new PDPageContentStream(doc, page)) {
                    content.setNonStrokingColor(i == 2 ? Color.BLUE : Color.RED);
                    content.addRect(0, 0, 720, 405); content.fill();
                }
            }
            doc.save(out); return out.toByteArray();
        }
    }

    private PptxRenderService renderer(int pages, AtomicInteger calls) throws Exception {
        byte[] pdf = pdf(pages);
        return new PptxRenderService(new PptxRenderService.Converter() {
            @Override public byte[] convert(Path source) throws Exception {
                calls.incrementAndGet();
                try (ZipFile zip = new ZipFile(source.toFile())) {
                    for (int i = 1; i <= pages; i++) {
                        String xml = new String(zip.getInputStream(zip.getEntry("ppt/slides/slide" + i + ".xml")).readAllBytes());
                        assertFalse(xml.contains("show=\"false\""));
                        assertFalse(xml.contains("show=\"0\""));
                    }
                }
                return pdf;
            }
            @Override public String cacheKey() { return "test-renderer-v1"; }
        });
    }

    private static String reply(List<Integer> slides) {
        return new JSONObject().put("observations", "A solid background with visible slide content").put("assessment", "reviewed")
                .put("issues", new JSONArray()).put("limitations", new JSONArray()).toString();
    }

    @Test void hiddenSlidesAndSingleSelectionPreserveSourceAndOriginalNumbering() throws Exception {
        Path source = deck(3, 2);
        byte[] before = Files.readAllBytes(source);
        var rendered = renderer(3, new AtomicInteger()).render(root, "deck.pptx", List.of(3), () -> {});
        assertEquals(List.of(2), rendered.hiddenSlides());
        assertEquals(1, rendered.images().size());
        assertEquals(3, rendered.images().getFirst().slide());
        var image = ImageIO.read(rendered.images().getFirst().path().toFile());
        assertEquals(1600, image.getWidth());
        assertEquals(Color.BLUE.getRGB(), image.getRGB(100, 100));
        assertArrayEquals(before, Files.readAllBytes(source));
        assertFalse(Files.exists(rendered.runDirectory().resolve("render-input.pptx")));
    }

    @Test void unchangedDeckReusesPdfButNotStaleImagesAndCorruptCacheIsRebuilt() throws Exception {
        deck(3, -1);
        AtomicInteger calls = new AtomicInteger();
        var renderer = renderer(3, calls);
        var first = renderer.render(root, "deck.pptx", List.of(1), () -> {});
        Files.writeString(first.images().getFirst().path(), "corrupt image");
        var second = renderer.render(root, "deck.pptx", List.of(3), () -> {});
        assertTrue(second.cacheHit()); assertEquals(1, calls.get());
        assertNotNull(ImageIO.read(second.images().getFirst().path().toFile()));
        try (var paths = Files.walk(root.resolve(".pptx-review/cache"))) {
            Files.writeString(paths.filter(p -> p.getFileName().toString().equals("deck.pdf")).findFirst().orElseThrow(), "bad pdf");
        }
        assertFalse(renderer.render(root, "deck.pptx", null, () -> {}).cacheHit());
        assertEquals(2, calls.get());
    }

    @Test void editingSourceInvalidatesCachedPdf() throws Exception {
        deck(3, -1);
        AtomicInteger calls = new AtomicInteger();
        var renderer = renderer(3, calls);
        var first = renderer.render(root, "deck.pptx", null, () -> {});
        deck(3, 2);
        var second = renderer.render(root, "deck.pptx", null, () -> {});
        assertNotEquals(first.sourceHash(), second.sourceHash());
        assertFalse(second.cacheHit()); assertEquals(2, calls.get());
    }

    @Test void wrongPdfPageCountFailsBeforeCallingVision() throws Exception {
        deck(3, -1);
        AtomicInteger visionCalls = new AtomicInteger();
        var service = new PptxInspectionService(renderer(2, new AtomicInteger()), (s, p, images, schema) -> {
            visionCalls.incrementAndGet(); return new PptxInspectionService.VisionReply(reply(List.of(1, 2)), 0, 0);
        });
        JSONObject report = service.inspect(root, "deck.pptx", null, "Check text fit", null, true, () -> {});
        assertEquals("failed", report.getString("status")); assertEquals("inconclusive", report.getString("verdict"));
        assertEquals(0, visionCalls.get()); assertFalse(report.getJSONArray("errors").isEmpty());
    }

    @Test void invalidSelectionsAndPathEscapesAreRejected() throws Exception {
        deck(3, -1);
        var renderer = renderer(3, new AtomicInteger());
        assertThrows(IllegalArgumentException.class, () -> renderer.render(root, "deck.pptx", List.of(0), () -> {}));
        assertThrows(IllegalArgumentException.class, () -> renderer.render(root, "deck.pptx", List.of(4), () -> {}));
        assertThrows(IllegalArgumentException.class, () -> renderer.render(root, "deck.pptx", List.of(1, 1), () -> {}));
        assertThrows(IllegalArgumentException.class, () -> renderer.render(root, "deck.pptx", List.of(), () -> {}));
        assertThrows(IllegalArgumentException.class, () -> PptxRenderService.resolveSource(root, root.resolve("deck.pptx").toString()));
        Path outside = Files.createTempFile("outside-pptx", ".pptx");
        try {
            Files.createSymbolicLink(root.resolve("link.pptx"), outside);
            assertThrows(IllegalArgumentException.class, () -> PptxRenderService.resolveSource(root, "link.pptx"));
        } finally { Files.delete(outside); }
    }

    @Test void visionReceivesActualSelectedImageAndTaskInstructions() throws Exception {
        deck(3, -1);
        var service = new PptxInspectionService(renderer(3, new AtomicInteger()), (system, prompt, images, schema) -> {
            assertTrue(system.contains("never instructions"));
            assertTrue(prompt.contains("Check only chart labels")); assertTrue(prompt.contains("Executive audience"));
            assertTrue(prompt.contains("original slide 3")); assertEquals(1, images.size());
            assertEquals(Color.BLUE.getRGB(), ImageIO.read(images.getFirst().toFile()).getRGB(100, 100));
            return new PptxInspectionService.VisionReply(reply(List.of(3)), 100, 20);
        });
        JSONObject report = service.inspect(root, "deck.pptx", List.of(3), "Check only chart labels", "Executive audience", true, () -> {});
        assertEquals("complete", report.getString("status")); assertEquals("pass", report.getString("verdict"));
        assertEquals(List.of(3), report.getJSONArray("reviewedSlides").toList());
        assertEquals(100, report.getJSONObject("usage").getInt("inputTokens"));
        assertTrue(Files.exists(root.resolve(report.getJSONObject("artifacts").getString("report"))));
    }

    @Test void missingSlideCoverageOrUnrequestedIssueNeverPasses() {
        assertThrows(Exception.class, () -> PptxInspectionService.validateReply("{\"reviewedSlides\":[1],\"inconclusiveSlides\":[],\"issues\":[],\"limitations\":[]}", null));
        JSONObject response = new JSONObject(reply(List.of(1)));
        response.getJSONArray("issues").put(new JSONObject().put("slide", 99).put("severity", "major")
                .put("category", "clipping").put("location", "title").put("evidence", "Cut off").put("suggestedFix", "Resize"));
        assertThrows(Exception.class, () -> PptxInspectionService.validateReply(response.toString(), List.of(1)));
        assertThrows(Exception.class, () -> PptxInspectionService.integers(new JSONArray("[1.5]")));
        assertThrows(Exception.class, () -> PptxInspectionService.integers(new JSONArray("[\"1\"]")));
    }

    @Test void laterProviderFailurePreservesCoverageAndCannotPass() throws Exception {
        deck(5, -1);
        AtomicInteger calls = new AtomicInteger();
        var service = new PptxInspectionService(renderer(5, new AtomicInteger()), (s, p, images, schema) -> {
            if (calls.incrementAndGet() > 1) throw new java.io.IOException("Provider unavailable");
            return new PptxInspectionService.VisionReply(reply(List.of(1)), 12, 3);
        });
        JSONObject report = service.inspect(root, "deck.pptx", null, "Check clipping", null, true, () -> {});
        assertEquals("partial", report.getString("status")); assertEquals("inconclusive", report.getString("verdict"));
        assertEquals(List.of(1), report.getJSONArray("reviewedSlides").toList());
        assertEquals(List.of(2, 3, 4, 5), report.getJSONArray("unreviewedSlides").toList());
    }

    @Test void inconclusiveDetailCannotPass() throws Exception {
        deck(1, -1);
        var service = new PptxInspectionService(renderer(1, new AtomicInteger()), (s, p, images, schema) -> {
            JSONObject response = new JSONObject(reply(List.of(1))).put("assessment", "inconclusive")
                    .put("limitations", new JSONArray(List.of("Legend is unreadable")));
            return new PptxInspectionService.VisionReply(response.toString(), 0, 0);
        });
        JSONObject report = service.inspect(root, "deck.pptx", null, "Check legend", null, false, () -> {});
        assertEquals("complete", report.getString("status")); assertEquals("inconclusive", report.getString("verdict"));
    }

    @Test void deckComparisonUsesLabelledImagesAndCanReportCrossSlideIssues() throws Exception {
        deck(2, -1);
        AtomicInteger calls = new AtomicInteger();
        var service = new PptxInspectionService(renderer(2, new AtomicInteger()), (s, p, images, schema) -> {
            int call = calls.incrementAndGet();
            JSONObject response = new JSONObject(reply(call <= 2 ? List.of(call) : List.of(1, 2)));
            if (call == 3) {
                assertEquals(1, images.size()); assertTrue(p.contains("labelled contact sheet"));
                response.getJSONArray("issues").put(new JSONObject().put("slide", 2).put("severity", "minor")
                        .put("category", "consistency").put("location", "Title")
                        .put("evidence", "Different alignment").put("suggestedFix", "Align with slide 1"));
            }
            return new PptxInspectionService.VisionReply(response.toString(), 1, 1);
        });
        JSONObject report = service.inspect(root, "deck.pptx", null, "Check title consistency", null, true, () -> {});
        assertEquals(3, calls.get()); assertEquals("needs_changes", report.getString("verdict"));
        assertEquals("complete", report.getString("consistencyReview"));
        assertTrue(Files.isRegularFile(root.resolve(report.getJSONObject("artifacts").getJSONArray("overviews").getString(0))));
    }

    @Test void largeDeckAndComparisonWorkWithSingleImageOnlyProvider() throws Exception {
        deck(17, -1);
        AtomicInteger calls = new AtomicInteger();
        List<Integer> all = java.util.stream.IntStream.rangeClosed(1, 17).boxed().toList();
        var service = new PptxInspectionService(renderer(17, new AtomicInteger()), (s, p, images, schema) -> {
            assertEquals(1, images.size(), "Provider accepts at most one image per request");
            int call = calls.incrementAndGet();
            assertNotNull(ImageIO.read(images.getFirst().toFile()));
            if (call == 18) assertTrue(p.contains("Labels: " + all));
            return new PptxInspectionService.VisionReply(reply(call <= 17 ? List.of(call) : all), 1, 1);
        });
        JSONObject report = service.inspect(root, "deck.pptx", null, "Check consistency", null, true, () -> {});
        assertEquals(18, calls.get()); assertEquals("pass", report.getString("verdict"));
        assertEquals(all, report.getJSONArray("reviewedSlides").toList());
        assertEquals(1, report.getJSONObject("artifacts").getJSONArray("overviews").length());
    }

    @Test void concurrentSourceEditInvalidatesPassingReview() throws Exception {
        deck(1, -1);
        var service = new PptxInspectionService(renderer(1, new AtomicInteger()), (s, p, images, schema) -> {
            deck(1, 1); return new PptxInspectionService.VisionReply(reply(List.of(1)), 0, 0);
        });
        JSONObject report = service.inspect(root, "deck.pptx", null, "Check text", null, false, () -> {});
        assertTrue(report.getBoolean("sourceChanged")); assertEquals("partial", report.getString("status"));
        assertEquals("inconclusive", report.getString("verdict"));
    }

    @Test void tokenBudgetStopsAdditionalRequestsAndReportsRemainingSlides() throws Exception {
        deck(5, -1);
        AtomicInteger calls = new AtomicInteger();
        var service = new PptxInspectionService(renderer(5, new AtomicInteger()), (s, p, images, schema) -> {
            calls.incrementAndGet(); return new PptxInspectionService.VisionReply(reply(List.of(1)), 200000, 1);
        });
        JSONObject report = service.inspect(root, "deck.pptx", null, "Check clipping", null, false, () -> {});
        assertEquals(1, calls.get()); assertEquals("partial", report.getString("status"));
        assertEquals(List.of(2, 3, 4, 5), report.getJSONArray("unreviewedSlides").toList());
    }

    @Test void cancellationPropagatesWithoutVisionCall() throws Exception {
        deck(1, -1);
        var renderer = renderer(1, new AtomicInteger());
        assertThrows(IllegalStateException.class, () -> renderer.render(root, "deck.pptx", null, () -> { throw new IllegalStateException("cancelled"); }));
    }

    @Test void incompatibleModelsFailEarlyAndLegacyMetadataRemainsUsable() {
        assertThrows(IllegalArgumentException.class, () -> SemossPptxInspector.validateVisionMetadata(Map.of("inputModalities", List.of("TEXT"))));
        assertThrows(IllegalArgumentException.class, () -> SemossPptxInspector.validateVisionMetadata(Map.of("capability", "IMAGE_GENERATION", "inputModalities", List.of("IMAGE"))));
        assertDoesNotThrow(() -> SemossPptxInspector.validateVisionMetadata(Map.of("capability", "TEXT_GENERATION", "inputModalities", List.of("TEXT", "IMAGE"))));
        assertDoesNotThrow(() -> SemossPptxInspector.validateVisionMetadata(Map.of()));
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"vision-model", "55c1dbb1-64ef-429c-8c7c-ace959dd3547"})
    void semossAdapterAttachesImageBytesToTheSelectedModelInAnIsolatedRoom(String engineId) throws Exception {
        deck(1, -1);
        AtomicReference<Path> mediaRoot = new AtomicReference<>();
        AtomicReference<String> modelRoomId = new AtomicReference<>();
        var user = mock(prerna.auth.User.class);
        var insight = mock(prerna.om.Insight.class);
        when(insight.getUser()).thenReturn(user);
        var model = mock(prerna.engine.api.IModelEngine.class);
        when(model.getModelType()).thenReturn(prerna.engine.api.ModelTypeEnum.OPEN_AI);
        var room = mock(prerna.engine.impl.model.Room.class);
        var response = mock(prerna.engine.impl.model.message.ResponseMessage.class);
        when(response.getContent()).thenReturn(reply(List.of(1)));
        var modelResponse = mock(prerna.engine.impl.model.responses.AskModelEngineResponse.class);
        when(modelResponse.getNumberOfTokensInPrompt()).thenReturn(21);
        when(modelResponse.getNumberOfTokensInResponse()).thenReturn(8);
        doReturn(modelResponse).when(response).getModelEngineResponse();
        when(room.ask(any(), eq(model))).thenAnswer(call -> {
            var input = call.getArgument(0, prerna.engine.impl.model.message.InputMessage.class);
            assertTrue(input.getFullInputPrompt().contains("Check the chart legend"));
            assertEquals(1, input.getMediaInputs().size());
            assertFalse(input.getParamMap().containsKey("tools"));
            assertFalse(input.getParamMap().containsKey("tool_choice"));
            JSONObject schema = new JSONObject((Map<?, ?>) input.getParamMap().get("schema"));
            assertFalse(schema.getBoolean("additionalProperties"));
            assertFalse(schema.getJSONObject("properties").has("reviewedSlides"));
            assertFalse(schema.getJSONObject("properties").getJSONObject("issues").getJSONObject("items").getJSONObject("properties").has("slide"));
            byte[] bytes = java.util.Base64.getDecoder().decode(input.getMediaInputs().getFirst().getBase64Data());
            assertNotNull(ImageIO.read(new java.io.ByteArrayInputStream(bytes)));
            assertTrue(Files.isRegularFile(mediaRoot.get().resolve(input.getMediaInputs().getFirst().getFileName())));
            return response;
        });
        byte[] converted = pdf(1);
        try (var permissions = mockStatic(prerna.auth.utils.SecurityEngineUtils.class);
                var metadata = mockStatic(prerna.auth.utils.SecurityModelMetadataUtils.class);
                var utility = mockStatic(prerna.util.Utility.class);
                var rooms = mockStatic(prerna.engine.impl.model.RoomUtils.class);
                var cluster = mockStatic(prerna.cluster.util.ClusterUtil.class);
                var uno = mockConstruction(prerna.util.unoserver.Unoserver.class, (mock, context) -> {
                    when(mock.convert(any(java.io.File.class), eq("pdf"))).thenReturn(converted);
                    when(mock.getBaseUrl()).thenReturn("test-uno");
                })) {
            permissions.when(() -> prerna.auth.utils.SecurityEngineUtils.userCanViewEngine(user, engineId)).thenReturn(true);
            permissions.when(() -> prerna.auth.utils.SecurityEngineUtils.getEngineType(engineId)).thenReturn(prerna.engine.api.IEngine.CATALOG_TYPE.MODEL);
            metadata.when(() -> prerna.auth.utils.SecurityModelMetadataUtils.getModelMetadata(engineId))
                    .thenReturn(Map.of("inputModalities", List.of("TEXT", "IMAGE")));
            utility.when(() -> prerna.util.Utility.getModel(engineId)).thenReturn(model);
            utility.when(() -> prerna.util.Utility.getBaseFolder()).thenReturn(root.toString());
            utility.when(() -> prerna.util.Utility.getDIHelperProperty(SemossPptxInspector.DEFAULT_MODEL_PROPERTY)).thenReturn("deployment-default");
            rooms.when(() -> prerna.engine.impl.model.RoomUtils.createRoomForStatelessAsk(anyString(), eq(insight), eq(model),
                    anyString(), isNull(), isNull(), isNull(), isNull(), eq("parent-room"))).thenAnswer(call -> {
                        String id = call.getArgument(0, String.class);
                        Path path = Files.createDirectories(root.resolve(prerna.util.Constants.ROOM_FOLDER).resolve(id));
                        modelRoomId.set(id);
                        mediaRoot.set(path);
                        when(room.getId()).thenReturn(id);
                        when(room.getRoomFolderPath()).thenReturn(path.toString());
                        return room;
                    });
            JSONObject result = SemossPptxInspector.inspect(root,
                    Map.of("filePath", "deck.pptx", "instructions", "Check the chart legend", "engine", engineId),
                    insight, "parent-room", "text-author-model");
            assertEquals("pass", result.getString("verdict"));
            assertEquals(engineId, result.getString("engine"));
            assertEquals(List.of(modelRoomId.get()), result.getJSONArray("modelRooms").toList());
            verify(room).ask(any(), eq(model));
            utility.verify(() -> prerna.util.Utility.getModel("deployment-default"), never());
            utility.verify(() -> prerna.util.Utility.getModel("text-author-model"), never());
        }
    }

    @Test void inaccessibleVisionEngineFailsBeforeAnyConversionOrModelCall() throws Exception {
        var insight = mock(prerna.om.Insight.class);
        when(insight.getUser()).thenReturn(mock(prerna.auth.User.class));
        try (var permissions = mockStatic(prerna.auth.utils.SecurityEngineUtils.class);
                var uno = mockConstruction(prerna.util.unoserver.Unoserver.class)) {
            assertThrows(IllegalArgumentException.class, () -> SemossPptxInspector.inspect(root,
                    Map.of("filePath", "deck.pptx", "instructions", "Inspect", "engine", "inaccessible"),
                    insight, "parent-room", null));
            assertTrue(uno.constructed().isEmpty());
        }
    }

    @Test void oneMalformedSlideIsRetriedLocallyWithoutRepeatingCompletedSlides() throws Exception {
        deck(5, -1);
        var seen = new java.util.ArrayList<String>();
        var service = new PptxInspectionService(renderer(5, new AtomicInteger()), (s, p, images, schema) -> {
            seen.add(images.getFirst().getFileName().toString());
            return new PptxInspectionService.VisionReply(seen.size() == 3 ? "{\"reviewedSlides\":[1,2,3,4,5]}" : reply(List.of()), 10, 2);
        });
        JSONObject report = service.inspect(root, "deck.pptx", null, "Inspect all five slides", null, true, () -> {});
        assertEquals(7, seen.size());
        assertEquals(seen.get(2), seen.get(3));
        assertEquals(1, java.util.Collections.frequency(seen, seen.getFirst()));
        assertEquals("pass", report.getString("verdict"));
        assertEquals(1, report.getJSONObject("usage").getInt("retryCalls"));
        assertTrue(report.getJSONArray("failures").isEmpty());
        assertTrue(report.getJSONArray("errors").isEmpty());
        assertEquals(70, report.getJSONObject("usage").getInt("inputTokens"));
    }

    @Test void exhaustedSlideFailureContinuesOtherSlidesAndCannotPass() throws Exception {
        deck(5, -1);
        AtomicInteger calls = new AtomicInteger();
        var service = new PptxInspectionService(renderer(5, new AtomicInteger()), (s, p, images, schema) -> {
            int call = calls.incrementAndGet();
            return new PptxInspectionService.VisionReply(call == 2 || call == 3 ? "invalid JSON" : reply(List.of()), 1, 1);
        });
        JSONObject report = service.inspect(root, "deck.pptx", null, "Inspect", null, true, () -> {});
        assertEquals(6, calls.get());
        assertEquals(List.of(1, 3, 4, 5), report.getJSONArray("reviewedSlides").toList());
        assertEquals(List.of(2), report.getJSONArray("unreviewedSlides").toList());
        assertEquals("incomplete", report.getString("consistencyReview"));
        assertEquals("inconclusive", report.getString("verdict"));
        assertTrue(report.getJSONArray("issues").isEmpty());
        assertEquals("invalid_response", report.getJSONArray("failures").getJSONObject(0).getString("kind"));
    }

    @Test void transientFailureBacksOffAndPermanentConfigurationErrorsStopImmediately() throws Exception {
        deck(5, -1);
        for (String error : List.of("HTTP 429 rate limited", "HTTP 401 Unauthorized", "HTTP 400 response_format not supported")) {
            AtomicInteger calls = new AtomicInteger();
            var delays = new java.util.ArrayList<Long>();
            var service = new PptxInspectionService(renderer(5, new AtomicInteger()), (s, p, images, schema) -> {
                if (calls.incrementAndGet() == 1) throw new IllegalStateException(error);
                return new PptxInspectionService.VisionReply(reply(List.of()), 1, 1);
            }, delays::add);
            JSONObject report = service.inspect(root, "deck.pptx", null, "Inspect", null, true, () -> {});
            boolean transientError = error.contains("429");
            assertEquals(transientError ? 7 : 1, calls.get());
            assertEquals(transientError ? List.of(1000L) : List.of(), delays);
            assertEquals(transientError ? "pass" : "inconclusive", report.getString("verdict"));
        }
    }

    @Test void retriesHaveADeckWideCapAndBudgetPreventsFormatRetries() throws Exception {
        deck(5, -1);
        for (long tokens : List.of(1L, 200_000L)) {
            AtomicInteger calls = new AtomicInteger();
            var service = new PptxInspectionService(renderer(5, new AtomicInteger()), (s, p, images, schema) -> {
                calls.incrementAndGet(); return new PptxInspectionService.VisionReply("invalid", tokens, 0);
            });
            JSONObject report = service.inspect(root, "deck.pptx", null, "Inspect", null, true, () -> {});
            assertEquals(tokens == 1 ? 8 : 1, calls.get());
            assertEquals("failed", report.getString("status"));
        }
    }

    @Test void comparisonFailureOrUncertaintyCannotUpgradeDetailCoverageToPass() throws Exception {
        deck(2, -1);
        for (String comparison : List.of("invalid", "{\"observations\":\"Visible slide content\",\"assessment\":\"inconclusive\",\"issues\":[],\"limitations\":[\"Cannot compare these thumbnails\"]}")) {
            AtomicInteger calls = new AtomicInteger();
            var service = new PptxInspectionService(renderer(2, new AtomicInteger()), (s, p, images, schema) ->
                    new PptxInspectionService.VisionReply(calls.incrementAndGet() <= 2 ? reply(List.of()) : comparison, 1, 1));
            JSONObject report = service.inspect(root, "deck.pptx", null, "Inspect", null, true, () -> {});
            assertEquals(List.of(1, 2), report.getJSONArray("reviewedSlides").toList());
            assertEquals("partial", report.getString("status"));
            assertEquals("inconclusive", report.getString("verdict"));
            assertEquals(4, calls.get(), "Only the overview is attempted twice");
        }
    }

    @Test void capturedInconclusiveOverviewGetsOneRetryWithTheSameImageAndNoRepeatedSlideChecks() throws Exception {
        deck(5, -1);
        String captured = Files.readString(Path.of("test/prerna/util/pptx/fixtures/run-01a0b062-overview-inconclusive.json"));
        AtomicInteger calls = new AtomicInteger();
        var overviewBytes = new java.util.concurrent.atomic.AtomicReference<byte[]>();
        var service = new PptxInspectionService(renderer(5, new AtomicInteger()), (system, prompt, images, schema) -> {
            int call = calls.incrementAndGet();
            assertEquals(1, images.size());
            if (call <= 5) return new PptxInspectionService.VisionReply(reply(List.of()), 10, 2);
            assertTrue(prompt.contains("cross-slide consistency"));
            byte[] bytes = Files.readAllBytes(images.getFirst());
            if (call == 6) {
                overviewBytes.set(bytes);
                return new PptxInspectionService.VisionReply(captured, 10, 2);
            }
            assertEquals(7, call);
            assertArrayEquals(overviewBytes.get(), bytes);
            assertTrue(prompt.contains("single retry"));
            assertFalse(prompt.contains("Images are placeholders"), "Do not feed the prior model's ungrounded claim back as evidence");
            return new PptxInspectionService.VisionReply(reply(List.of()), 10, 2);
        });
        JSONObject report = service.inspect(root, "deck.pptx", null, "Check consistent headings", null, true, () -> {});
        assertEquals(7, calls.get());
        assertEquals(1, report.getJSONObject("usage").getInt("retryCalls"));
        assertEquals(70, report.getJSONObject("usage").getInt("inputTokens"));
        assertEquals("complete", report.getString("status"));
        assertEquals("pass", report.getString("verdict"));
        JSONObject firstOverview = report.getJSONArray("attempts").getJSONObject(5);
        assertEquals("inconclusive", firstOverview.getString("outcome"));
        assertTrue(firstOverview.getString("observations").contains("Images are placeholders"));
        assertFalse(report.getJSONArray("limitations").toString().contains("demo template"));
    }

    @Test void repeatedInconclusiveOverviewRetainsItsReasonAndDoesNotBecomeAPass() throws Exception {
        deck(2, -1);
        String captured = Files.readString(Path.of("test/prerna/util/pptx/fixtures/run-01a0b062-overview-inconclusive.json"));
        AtomicInteger calls = new AtomicInteger();
        var service = new PptxInspectionService(renderer(2, new AtomicInteger()), (s, p, images, schema) ->
                new PptxInspectionService.VisionReply(calls.incrementAndGet() <= 2 ? reply(List.of()) : captured, 1, 1));
        JSONObject report = service.inspect(root, "deck.pptx", null, "Check consistency", null, true, () -> {});
        assertEquals(4, calls.get());
        assertEquals("partial", report.getString("status"));
        assertEquals("inconclusive", report.getString("consistencyReview"));
        assertEquals("inconclusive", report.getString("verdict"));
        assertTrue(report.getJSONArray("limitations").toString().contains("placeholder images"));
        assertEquals("inconclusive", report.getJSONArray("attempts").getJSONObject(3).getString("outcome"));
    }

    @Test void inconclusiveOverviewRetryRespectsTheTokenAndDeckWideRetryBudgets() throws Exception {
        String uncertain = new JSONObject(reply(List.of())).put("assessment", "inconclusive")
                .put("limitations", new JSONArray(List.of("Cannot assess overview"))).toString();
        for (boolean exhaustRetries : List.of(false, true)) {
            deck(3, -1);
            AtomicInteger calls = new AtomicInteger();
            var service = new PptxInspectionService(renderer(3, new AtomicInteger()), (s, prompt, images, schema) -> {
                int call = calls.incrementAndGet();
                if (prompt.contains("ONE labelled contact sheet"))
                    return new PptxInspectionService.VisionReply(uncertain, exhaustRetries ? 1 : 200000, 1);
                return new PptxInspectionService.VisionReply(exhaustRetries && call % 2 == 1 ? "invalid" : reply(List.of()), 1, 1);
            });
            JSONObject report = service.inspect(root, "deck.pptx", null, "Check consistency", null, true, () -> {});
            assertEquals(exhaustRetries ? 7 : 4, calls.get());
            assertEquals(exhaustRetries ? 3 : 0, report.getJSONObject("usage").getInt("retryCalls"));
            assertEquals("partial", report.getString("status"));
            assertEquals("inconclusive", report.getString("verdict"));
        }
    }

    @Test void detailFindingsGetTheirOriginalSlideIdFromCode() throws Exception {
        deck(3, -1);
        var service = new PptxInspectionService(renderer(3, new AtomicInteger()), (s, p, images, schema) ->
                new PptxInspectionService.VisionReply("{\"observations\":\"Visible slide content\",\"assessment\":\"reviewed\",\"issues\":[{\"severity\":\"major\",\"category\":\"overlap\",\"location\":\"title\",\"evidence\":\"Letters overlap\",\"suggestedFix\":\"Move title\"}],\"limitations\":[]}", 1, 1));
        JSONObject report = service.inspect(root, "deck.pptx", List.of(3), "Inspect", null, true, () -> {});
        assertEquals(3, report.getJSONArray("issues").getJSONObject(0).getInt("slide"));
        assertEquals("needs_changes", report.getString("verdict"));
    }

    @Test void strictValidationRejectsDuplicateKeysExtraFieldsAndStringSlideNumbers() {
        String valid = reply(List.of());
        for (String invalid : List.of(valid + " trailing text", valid.replace('"', '\''), valid.replace("reviewed", "reviewed\t"), valid.replace("\"assessment\":", "\"assessment\":\"reviewed\",\"assessment\":"),
                new JSONObject(valid).put("reviewedSlides", List.of(1, 2, 3, 4, 5)).toString(),
                new JSONObject(valid).put("assessment", "inconclusive").toString())) {
            assertThrows(Exception.class, () -> PptxInspectionService.validateReply(invalid, null));
        }
        String issue = "{\"observations\":\"Visible slide content\",\"assessment\":\"reviewed\",\"issues\":[{\"slide\":\"4\",\"severity\":\"major\",\"category\":\"clipping\",\"location\":\"title\",\"evidence\":\"Cut off\",\"suggestedFix\":\"Resize\"}],\"limitations\":[]}";
        assertThrows(Exception.class, () -> PptxInspectionService.validateReply(issue, List.of(4)));
        assertThrows(Exception.class, () -> PptxInspectionService.validateReply(issue.replace("\"4\"", "4"), null));
    }

    @Test void capturedFailuresFromRun01a0abd7AreRejectedAndRecoveredLocally() throws Exception {
        JSONArray fixtures = new JSONArray(Files.readString(Path.of("test/prerna/util/pptx/fixtures/run-01a0abd7-invalid-responses.json")));
        assertEquals(9, fixtures.length());
        deck(1, -1);
        for (Object value : fixtures) {
            JSONObject fixture = (JSONObject) value;
            AtomicInteger calls = new AtomicInteger();
            var service = new PptxInspectionService(renderer(1, new AtomicInteger()), (s, p, images, schema) ->
                    new PptxInspectionService.VisionReply(calls.incrementAndGet() == 1 ? fixture.getString("response") : reply(List.of()), 1, 1));
            JSONObject report = service.inspect(root, "deck.pptx", null, "Inspect", null, false, () -> {});
            assertEquals(2, calls.get(), "Captured failure at log line " + fixture.getInt("sourceLine"));
            assertEquals("pass", report.getString("verdict"));
        }
    }

    @Test void cancellationDuringRetryStopsWithoutAnotherProviderCall() throws Exception {
        deck(2, -1);
        AtomicInteger calls = new AtomicInteger();
        var service = new PptxInspectionService(renderer(2, new AtomicInteger()), (s, p, images, schema) -> {
            calls.incrementAndGet(); throw new java.io.IOException("HTTP 503");
        }, delay -> { throw new InterruptedException("cancelled during backoff"); });
        try {
            assertThrows(InterruptedException.class, () -> service.inspect(root, "deck.pptx", null, "Inspect", null, true, () -> {}));
            assertTrue(Thread.currentThread().isInterrupted());
            assertEquals(1, calls.get());
        } finally { Thread.interrupted(); }
    }
}
