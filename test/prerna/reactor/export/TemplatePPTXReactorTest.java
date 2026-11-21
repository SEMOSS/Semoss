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
package prerna.reactor.export;

import com.google.gson.JsonObject;
import org.apache.poi.sl.usermodel.PictureData.PictureType;
import org.apache.poi.xslf.usermodel.XMLSlideShow;
import org.apache.poi.xslf.usermodel.XSLFShape;
import org.apache.poi.xslf.usermodel.XSLFSlide;
import org.apache.poi.xslf.usermodel.XSLFTextBox;
import org.apache.poi.xslf.usermodel.XSLFTextShape;
import org.openxmlformats.schemas.presentationml.x2006.main.CTShape;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import java.awt.geom.Rectangle2D;
import java.io.FileInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.om.Insight;
import prerna.om.InsightFile;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * Unit tests for TemplatePPTXReactor.
 *
 * These tests exercise the pure-logic methods directly (no Semoss runtime,
 * no Insight, no security layer). Each test constructs real Apache POI objects
 * in-memory and verifies the transformation.
 */
class TemplatePPTXReactorTest {

    private TemplatePPTXReactor reactor;

    @BeforeEach
    void setUp() {
        reactor = new TemplatePPTXReactor();
    }

    // =========================================================================
    // parsePayload
    // =========================================================================

    @Test
    void parsePayload_validJson_returnsAllKeys() {
        String json = "{"
                + "\"slide_title\": {\"type\": \"text\", \"value\": \"Hello\"},"
                + "\"company_logo\": {\"type\": \"image\", \"path\": \"/tmp/logo.png\"}"
                + "}";
        Map<String, JsonObject> result = reactor.parsePayload(json);
        assertEquals(2, result.size());
        assertTrue(result.containsKey("slide_title"));
        assertTrue(result.containsKey("company_logo"));
        assertEquals("text", result.get("slide_title").get("type").getAsString());
        assertEquals("Hello", result.get("slide_title").get("value").getAsString());
    }

    @Test
    void parsePayload_emptyString_throwsIllegalArgument() {
        assertThrows(IllegalArgumentException.class, () -> reactor.parsePayload(""));
    }

    @Test
    void parsePayload_nullInput_throwsIllegalArgument() {
        assertThrows(IllegalArgumentException.class, () -> reactor.parsePayload(null));
    }

    @Test
    void parsePayload_blankString_throwsIllegalArgument() {
        assertThrows(IllegalArgumentException.class, () -> reactor.parsePayload("   "));
    }

    @Test
    void parsePayload_emptyObject_returnsEmptyMap() {
        Map<String, JsonObject> result = reactor.parsePayload("{}");
        assertTrue(result.isEmpty());
    }

    @Test
    void parsePayload_preservesInsertionOrder() {
        String json = "{\"c\":{\"type\":\"text\",\"value\":\"C\"},\"a\":{\"type\":\"text\",\"value\":\"A\"},\"b\":{\"type\":\"text\",\"value\":\"B\"}}";
        Map<String, JsonObject> result = reactor.parsePayload(json);
        String[] keys = result.keySet().toArray(new String[0]);
        assertArrayEquals(new String[] { "c", "a", "b" }, keys);
    }

    // =========================================================================
    // detectPictureType
    // =========================================================================

    @Test
    void detectPictureType_png() {
        assertEquals(PictureType.PNG, reactor.detectPictureType("logo.png"));
    }

    @Test
    void detectPictureType_jpg() {
        assertEquals(PictureType.JPEG, reactor.detectPictureType("photo.jpg"));
    }

    @Test
    void detectPictureType_jpeg() {
        assertEquals(PictureType.JPEG, reactor.detectPictureType("photo.JPEG"));
    }

    @Test
    void detectPictureType_gif() {
        assertEquals(PictureType.GIF, reactor.detectPictureType("anim.gif"));
    }

    @Test
    void detectPictureType_emf() {
        assertEquals(PictureType.EMF, reactor.detectPictureType("vector.emf"));
    }

    @Test
    void detectPictureType_wmf() {
        assertEquals(PictureType.WMF, reactor.detectPictureType("vector.wmf"));
    }

    @Test
    void detectPictureType_svg() {
        assertEquals(PictureType.SVG, reactor.detectPictureType("icon.svg"));
    }

    @Test
    void detectPictureType_unknownExtension_defaultsPng() {
        assertEquals(PictureType.PNG, reactor.detectPictureType("file.tiff"));
    }

    @Test
    void detectPictureType_uppercasePng() {
        assertEquals(PictureType.PNG, reactor.detectPictureType("/some/path/IMAGE.PNG"));
    }

    // =========================================================================
    // replaceText
    // =========================================================================

    @Test
    void replaceText_singleRunSetsValue() throws IOException {
        try (XMLSlideShow ppt = new XMLSlideShow()) {
            XSLFSlide slide = ppt.createSlide();
            XSLFTextBox box = slide.createTextBox();
            box.setAnchor(new Rectangle2D.Double(0, 0, 200, 50));
            // Use POI's setText to establish content properly
            box.setText("OLD TEXT");

            reactor.replaceText(box, "NEW TEXT");

            assertEquals("NEW TEXT", box.getText());
        }
    }

    @Test
    void replaceText_multipleRunsCollapsedToOne() throws IOException {
        try (XMLSlideShow ppt = new XMLSlideShow()) {
            XSLFSlide slide = ppt.createSlide();
            XSLFTextBox box = slide.createTextBox();
            box.setAnchor(new Rectangle2D.Double(0, 0, 200, 50));
            // Build multiple runs directly on the XML bean (same as real PPTX content)
            org.openxmlformats.schemas.drawingml.x2006.main.CTTextBody txBody = box.getTextBody().getXmlObject();
            org.openxmlformats.schemas.drawingml.x2006.main.CTTextParagraph ctPara = txBody.getPArray(0);
            ctPara.addNewR().setT("Part1");
            ctPara.addNewR().setT("Part2");
            ctPara.addNewR().setT("Part3");

            reactor.replaceText(box, "REPLACED");

            assertEquals("REPLACED", box.getText());
            // Only one paragraph and one run survive
            assertEquals(1, txBody.sizeOfPArray());
            assertEquals(1, txBody.getPArray(0).sizeOfRArray());
        }
    }

    @Test
    void replaceText_multipleParagraphsCollapsedToOne() throws IOException {
        try (XMLSlideShow ppt = new XMLSlideShow()) {
            XSLFSlide slide = ppt.createSlide();
            XSLFTextBox box = slide.createTextBox();
            box.setAnchor(new Rectangle2D.Double(0, 0, 200, 100));
            // Build multiple paragraphs directly on the XML bean
            org.openxmlformats.schemas.drawingml.x2006.main.CTTextBody txBody = box.getTextBody().getXmlObject();
            txBody.getPArray(0).addNewR().setT("Line 1");
            txBody.addNewP().addNewR().setT("Line 2");
            txBody.addNewP().addNewR().setT("Line 3");

            reactor.replaceText(box, "SINGLE LINE");

            assertEquals("SINGLE LINE", box.getText());
            assertEquals(1, txBody.sizeOfPArray());
        }
    }

    @Test
    void replaceText_emptyShapeSetsText() throws IOException {
        try (XMLSlideShow ppt = new XMLSlideShow()) {
            XSLFSlide slide = ppt.createSlide();
            XSLFTextBox box = slide.createTextBox();
            box.setAnchor(new Rectangle2D.Double(0, 0, 200, 50));

            reactor.replaceText(box, "INJECTED");

            assertEquals("INJECTED", box.getText());
        }
    }

    // =========================================================================
    // applyReplacements — shape-not-found warning (no exception thrown)
    // =========================================================================

    @Test
    void applyReplacements_unmatchedPayloadKey_doesNotThrow() throws IOException {
        try (XMLSlideShow ppt = new XMLSlideShow()) {
            ppt.createSlide(); // slide with no named shapes
            String json = "{\"nonexistent_shape\": {\"type\": \"text\", \"value\": \"X\"}}";
            Map<String, JsonObject> replacements = reactor.parsePayload(json);
            // must not throw — just warn
            assertDoesNotThrow(() -> reactor.applyReplacements(ppt, replacements));
        }
    }

    @Test
    void applyReplacements_textReplacement_endToEnd(@TempDir File tempDir) throws IOException {
        try (XMLSlideShow ppt = new XMLSlideShow()) {
            XSLFSlide slide = ppt.createSlide();

            // Create a named text box
            XSLFTextBox box = slide.createTextBox();
            box.setAnchor(new Rectangle2D.Double(0, 0, 300, 60));
            ((CTShape) box.getXmlObject()).getNvSpPr().getCNvPr().setName("slide_title");
            box.setText("PLACEHOLDER");

            String json = "{\"slide_title\": {\"type\": \"text\", \"value\": \"Q3 Results\"}}";
            reactor.applyReplacements(ppt, reactor.parsePayload(json));

            // Verify the text was replaced
            boolean found = false;
            for (XSLFShape shape : slide.getShapes()) {
                if ("slide_title".equals(shape.getShapeName()) && shape instanceof XSLFTextShape) {
                    assertEquals("Q3 Results", ((XSLFTextShape) shape).getText());
                    found = true;
                }
            }
            assertTrue(found, "slide_title shape should have been found and updated");

            // Also verify the file round-trips without error
            File out = new File(tempDir, "test_output.pptx");
            try (FileOutputStream fos = new FileOutputStream(out)) {
                ppt.write(fos);
            }
            assertTrue(out.exists());
            assertTrue(out.length() > 0);
        }
    }

    @Test
    void applyReplacements_unknownType_doesNotThrowButSkips() throws IOException {
        try (XMLSlideShow ppt = new XMLSlideShow()) {
            XSLFSlide slide = ppt.createSlide();
            XSLFTextBox box = slide.createTextBox();
            box.setAnchor(new Rectangle2D.Double(0, 0, 100, 50));
            ((CTShape) box.getXmlObject()).getNvSpPr().getCNvPr().setName("my_shape");
            box.setText("original");

            String json = "{\"my_shape\": {\"type\": \"unknown_type\", \"value\": \"X\"}}";
            // must not propagate — warn and skip
            assertDoesNotThrow(() -> reactor.applyReplacements(ppt, reactor.parsePayload(json)));

            // original text should be unchanged since the replacement was skipped
            assertEquals("original", box.getText());
        }
    }

    @Test
    void applyReplacements_emptyPayload_doesNotModifySlides() throws IOException {
        try (XMLSlideShow ppt = new XMLSlideShow()) {
            XSLFSlide slide = ppt.createSlide();
            XSLFTextBox box = slide.createTextBox();
            box.setAnchor(new Rectangle2D.Double(0, 0, 100, 50));
            ((CTShape) box.getXmlObject()).getNvSpPr().getCNvPr().setName("title");
            box.setText("unchanged");

            reactor.applyReplacements(ppt, reactor.parsePayload("{}"));

            assertEquals("unchanged", box.getText());
        }
    }

    // =========================================================================
    // resolveTemplateFile
    // =========================================================================
    @Test
    void resolveTemplateFile_insightRelativePath_returnsFileInInsightFolder(@TempDir Path tempDir)
            throws IOException {
        java.nio.file.Files.createFile(tempDir.resolve("template.pptx"));

        File resolved = resolveWithInsightFolder(tempDir, "template.pptx");

        assertEquals(new File(tempDir.toFile(), "template.pptx"), resolved);
    }

    @Test
    void resolveTemplateFile_insightRelativeSubfolder_returnsFileInInsightFolder(@TempDir Path tempDir)
            throws IOException {
        java.nio.file.Files.createDirectories(tempDir.resolve("assets"));
        java.nio.file.Files.createFile(tempDir.resolve("assets").resolve("template.pptx"));

        File resolved = resolveWithInsightFolder(tempDir, "assets/template.pptx");

        assertEquals(new File(tempDir.toFile(), "assets/template.pptx"), resolved);
    }

    @Test
    void resolveTemplateFile_absolutePathOutsideInsightFolder_returnsAbsoluteFile(@TempDir Path tempDir)
            throws IOException {
        Path insightFolder = java.nio.file.Files.createDirectories(tempDir.resolve("insight"));
        Path template = java.nio.file.Files.createFile(tempDir.resolve("template.pptx"));

        File resolved = resolveWithInsightFolder(insightFolder, template.toString());

        assertEquals(template.toFile(), resolved);
    }

    @Test
    void resolveTemplateFile_missingRelativePath_throwsWithSearchedLocations(@TempDir Path tempDir) {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> resolveWithInsightFolder(tempDir, "missing.pptx"));

        assertTrue(e.getMessage().contains("Template file not found"));
        assertTrue(e.getMessage().contains(new File(tempDir.toFile(), "missing.pptx").getAbsolutePath()));
    }

    @Test
    void resolveTemplateFile_missingAbsolutePath_throws(@TempDir Path tempDir) {
        String missing = tempDir.resolve("missing.pptx").toString();

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> resolveWithInsightFolder(tempDir, missing));

        assertTrue(e.getMessage().contains("Template file not found"));
    }

    @Test
    void resolveTemplateFile_normalizesRawValueBeforeLookup(@TempDir Path tempDir) throws IOException {
        java.nio.file.Files.createFile(tempDir.resolve("template.pptx"));
        Insight insight = mock(Insight.class);
        when(insight.getInsightFolder()).thenReturn(tempDir.toString());
        reactor.setInsight(insight);

        try (MockedStatic<Utility> utility = mockStatic(Utility.class)) {
            utility.when(() -> Utility.normalizePath("raw\\template.pptx")).thenReturn("template.pptx");

            File resolved = reactor.resolveTemplateFile("raw\\template.pptx");

            assertEquals(new File(tempDir.toFile(), "template.pptx"), resolved);
            utility.verify(() -> Utility.normalizePath("raw\\template.pptx"));
        }
    }

    private File resolveWithInsightFolder(Path insightFolder, String rawValue) {
        Insight insight = mock(Insight.class);
        when(insight.getInsightFolder()).thenReturn(insightFolder.toString());
        reactor.setInsight(insight);
        try (MockedStatic<Utility> utility = mockStatic(Utility.class)) {
            utility.when(() -> Utility.normalizePath(anyString()))
                    .thenAnswer(invocation -> invocation.getArgument(0));
            return reactor.resolveTemplateFile(rawValue);
        }
    }

    // =========================================================================
    // execute — SEMOSS runtime and Insight export registration
    // =========================================================================

    @Test
    void execute_registersGeneratedFileInInsightFolder(@TempDir Path tempDir) throws Exception {
        User user = mock(User.class);
        Insight insight = mock(Insight.class);
        when(insight.getUser()).thenReturn(user);
        when(insight.getInsightFolder()).thenReturn(tempDir.toString());
        TemplatePPTXReactor reactor = reactorForExecute(
                insight, createTemplate(tempDir.resolve("template.pptx")), null);

        try (MockedStatic<AbstractSecurityUtils> security = mockStatic(AbstractSecurityUtils.class);
                MockedStatic<Utility> utility = mockStatic(Utility.class)) {
            security.when(AbstractSecurityUtils::adminSetExporter).thenReturn(false);
            stubUtility(utility, user);

            NounMetadata result = reactor.execute();

            assertSuccessfulExport(result, insight, tempDir, true);
        }
    }

    @Test
    void execute_registersExplicitOutputFolderWithoutInsightCleanup(@TempDir Path tempDir) throws Exception {
        Path insightFolder = tempDir.resolve("insight");
        Path outputFolder = tempDir.resolve("exports");
        java.nio.file.Files.createDirectories(insightFolder);
        java.nio.file.Files.createDirectories(outputFolder);

        User user = mock(User.class);
        Insight insight = mock(Insight.class);
        when(insight.getUser()).thenReturn(user);
        when(insight.getInsightFolder()).thenReturn(insightFolder.toString());
        TemplatePPTXReactor reactor = reactorForExecute(
                insight, createTemplate(tempDir.resolve("template.pptx")), outputFolder);

        try (MockedStatic<AbstractSecurityUtils> security = mockStatic(AbstractSecurityUtils.class);
                MockedStatic<Utility> utility = mockStatic(Utility.class)) {
            security.when(AbstractSecurityUtils::adminSetExporter).thenReturn(false);
            stubUtility(utility, user);

            NounMetadata result = reactor.execute();

            assertSuccessfulExport(result, insight, outputFolder, false);
        }
    }

    @Test
    void execute_rejectsNonExporterWhenExportIsRestricted(@TempDir Path tempDir) {
        User user = mock(User.class);
        Insight insight = mock(Insight.class);
        when(insight.getUser()).thenReturn(user);
        TemplatePPTXReactor reactor = reactorForExecute(
                insight, tempDir.resolve("missing-template.pptx"), null);

        try (MockedStatic<AbstractSecurityUtils> security = mockStatic(AbstractSecurityUtils.class);
                MockedStatic<SecurityQueryUtils> securityQueries = mockStatic(SecurityQueryUtils.class)) {
            security.when(AbstractSecurityUtils::adminSetExporter).thenReturn(true);
            securityQueries.when(() -> SecurityQueryUtils.userIsExporter(user)).thenReturn(false);

            assertThrows(SemossPixelException.class, reactor::execute);
            verify(insight, never()).addExportFile(anyString(), any(InsightFile.class));
        }
    }

    private TemplatePPTXReactor reactorForExecute(Insight insight, Path template, Path outputFolder) {
        NounStore store = new NounStore("template-pptx-execute-test");
        store.makeGenRowStruct(ReactorKeysEnum.TEMPLATE_FILE.getKey())
                .add(template.toString(), PixelDataType.CONST_STRING);
        store.makeGenRowStruct(TemplatePPTXReactor.PAYLOAD_KEY)
                .add("{\"slide_title\":{\"type\":\"text\",\"value\":\"Generated title\"}}",
                        PixelDataType.CONST_STRING);
        if (outputFolder != null) {
            store.makeGenRowStruct(ReactorKeysEnum.FILE_PATH.getKey())
                    .add(outputFolder.toString(), PixelDataType.CONST_STRING);
        }

        TemplatePPTXReactor executeReactor = new TemplatePPTXReactor();
        executeReactor.setInsight(insight);
        executeReactor.setNounStore(store);
        return executeReactor;
    }

    private Path createTemplate(Path templatePath) throws IOException {
        try (XMLSlideShow ppt = new XMLSlideShow();
                FileOutputStream outputStream = new FileOutputStream(templatePath.toFile())) {
            XSLFTextBox title = ppt.createSlide().createTextBox();
            title.setAnchor(new Rectangle2D.Double(0, 0, 300, 60));
            ((CTShape) title.getXmlObject()).getNvSpPr().getCNvPr().setName("slide_title");
            title.setText("PLACEHOLDER");
            ppt.write(outputStream);
        }
        return templatePath;
    }

    private void stubUtility(MockedStatic<Utility> utility, User user) {
        utility.when(() -> Utility.normalizePath(anyString()))
                .thenAnswer(invocation -> invocation.getArgument(0));
        utility.when(() -> Utility.getCurrentZonedDateTimeForUser(user))
                .thenReturn(ZonedDateTime.parse("2024-01-02T03:04:05Z"));
    }

    private void assertSuccessfulExport(NounMetadata result, Insight insight, Path expectedFolder,
            boolean deleteOnInsightClose) throws Exception {
        assertEquals(PixelDataType.CONST_STRING, result.getNounType());
        assertTrue(result.getOpType().contains(PixelOperationType.FILE_DOWNLOAD));

        String downloadKey = (String) result.getValue();
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<InsightFile> fileCaptor = ArgumentCaptor.forClass(InsightFile.class);
        verify(insight).addExportFile(keyCaptor.capture(), fileCaptor.capture());

        assertEquals(downloadKey, keyCaptor.getValue());
        InsightFile export = fileCaptor.getValue();
        assertEquals(downloadKey, export.getFileKey());
        assertEquals(deleteOnInsightClose, export.isDeleteOnInsightClose());

        Path outputPath = Path.of(export.getFilePath());
        assertEquals(expectedFolder, outputPath.getParent());
        assertTrue(java.nio.file.Files.isRegularFile(outputPath));
        try (FileInputStream inputStream = new FileInputStream(outputPath.toFile());
                XMLSlideShow exportedPpt = new XMLSlideShow(inputStream)) {
            XSLFTextShape title = (XSLFTextShape) exportedPpt.getSlides().get(0).getShapes().get(0);
            assertEquals("Generated title", title.getText());
        }
    }
}
