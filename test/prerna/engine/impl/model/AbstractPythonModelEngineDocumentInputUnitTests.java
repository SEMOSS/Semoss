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
package prerna.engine.impl.model;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import com.google.gson.Gson;

import prerna.auth.utils.SecurityModelMetadataUtils;
import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyUtils;
import prerna.engine.api.ModelModalityEnum;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MediaMessagePart;
import prerna.engine.impl.model.message.MessageInputMedia;
import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.Settings;

class AbstractPythonModelEngineDocumentInputUnitTests {

    @Test
    void textOnlyModelAllowsDocumentsToReachExtractionWithoutChangingHistory() {
        TestEngine engine = new TestEngine(EnumSet.of(ModelModalityEnum.TEXT));
        InputMessage pdf = document("application/pdf");
        InputMessage pptx = document("application/vnd.openxmlformats-officedocument.presentationml.presentation");
        pptx.setParentMessageId(pdf.getMessageId());
        assertDoesNotThrow(() -> engine.validateInputModalities(List.of(pdf, pptx)));
        assertDoesNotThrow(() -> engine.validateInputModalities(List.of(pdf), pptx));
        assertInstanceOf(MediaMessagePart.class, pdf.getParts().getFirst());
    }

    @Test
    void fileCapabilityAlsoAllowsPdfWithoutTextCapability() {
        TestEngine engine = new TestEngine(EnumSet.of(ModelModalityEnum.FILE));
        assertDoesNotThrow(() -> engine.validateInputModalities(List.of(document("application/pdf"))));
    }

    @Test
    void automaticExtractionDoesNotUnlockImageAudioOrVideo() {
        TestEngine engine = new TestEngine(EnumSet.of(ModelModalityEnum.TEXT));
        for (String mime : List.of("image/png", "audio/wav", "video/mp4")) {
            assertThrows(IllegalArgumentException.class,
                    () -> engine.validateInputModalities(List.of(document(mime))));
        }
    }

    @Test
    void modelWithoutTextInputCannotUseExtraction() {
        TestEngine engine = new TestEngine(EnumSet.of(ModelModalityEnum.AUDIO));
        assertThrows(IllegalArgumentException.class,
                () -> engine.validateInputModalities(List.of(document("application/pdf"))));
    }

    @Test
    void legacyPythonEnginesRetainTheirValidation() {
        TestEngine engine = new TestEngine(EnumSet.of(ModelModalityEnum.TEXT));
        engine.modelType = ModelTypeEnum.EMBEDDED;
        assertThrows(IllegalArgumentException.class,
                () -> engine.validateInputModalities(List.of(document("application/pdf"))));
    }

    @Test
    void selectedEngineMetadataWinsOverRequestParametersWithoutMutatingThem() {
        TestEngine engine = new TestEngine(EnumSet.of(ModelModalityEnum.TEXT));
        Map<String, Object> parameters = Map.of("message_json", "[]",
                "_semoss_input_modalities", List.of("FILE"));
        String command = ask(engine, parameters);
        assertTrue(command.contains("_semoss_input_modalities=" + PyUtils.determineStringType(List.of("TEXT"))));
        assertFalse(command.contains("FILE"));
        assertEquals(List.of("FILE"), parameters.get("_semoss_input_modalities"));
    }

    @Test
    void metadataFlowsToPythonWithoutAnyDocumentInitArguments() throws Exception {
        TestEngine engine = new TestEngine(null);
        Properties properties = properties();
        try (MockedStatic<SecurityModelMetadataUtils> metadata = mockStatic(SecurityModelMetadataUtils.class)) {
            metadata.when(() -> SecurityModelMetadataUtils.getModelMetadata("model-id"))
                    .thenReturn(Map.of("inputModalities", List.of("text", "pdf")));
            engine.open(properties);
        }
        String command = ask(engine, Map.of("message_json", "[]"));
        assertTrue(command.contains("_semoss_input_modalities="
                + PyUtils.determineStringType(List.of("PDF", "TEXT"))));
        assertFalse(engine.getSmssProp().containsKey("NATIVE_DOCUMENT_MIME_TYPES"));
    }

    @Test
    void existingSmssInputModalitiesOverrideIsHonored() throws Exception {
        TestEngine engine = new TestEngine(null);
        Properties properties = properties();
        properties.setProperty(Constants.INPUT_MODALITIES, "TEXT");
        try (MockedStatic<SecurityModelMetadataUtils> metadata = mockStatic(SecurityModelMetadataUtils.class)) {
            metadata.when(() -> SecurityModelMetadataUtils.getModelMetadata("model-id"))
                    .thenReturn(Map.of("inputModalities", List.of("TEXT", "FILE")));
            engine.open(properties);
        }
        String command = ask(engine, Map.of("message_json", "[]"));
        assertTrue(command.contains("_semoss_input_modalities=" + PyUtils.determineStringType(List.of("TEXT"))));
        assertFalse(command.contains("FILE"));
    }

    @Test
    void missingMetadataKeepsExistingDeliveryAndDropsCallerCapabilityOverride() {
        TestEngine engine = new TestEngine(null);
        assertFalse(ask(engine, Map.of("_semoss_input_modalities", List.of("TEXT")))
                .contains("_semoss_input_modalities"));
    }

    @Test
    void batchesCarryMetadataOnlyForSemossHistories() {
        TestEngine engine = new TestEngine(EnumSet.of(ModelModalityEnum.TEXT));
        when(engine.pyTranslator.runDirectPyNoCancelTrace(anyString()))
                .thenReturn(Map.of("provider_batch_id", "batch-test", "status", "submitted"));
        Map<String, Object> history = Map.of("custom_id", "history", "message_json", "[]");
        Map<String, Object> nativeBody = Map.of("custom_id", "native", "body", Map.of("messages", List.of()));
        engine.submitBatch(List.of(history, nativeBody), Map.of());
        ArgumentCaptor<String> command = ArgumentCaptor.forClass(String.class);
        verify(engine.pyTranslator).runDirectPyNoCancelTrace(command.capture());
        String script = command.getValue();
        String literal = script.substring("test_model.submit_batch(requests=".length(), script.length() - 1);
        Gson gson = new Gson();
        List<?> requests = gson.fromJson(gson.fromJson(literal, String.class), List.class);
        assertEquals(List.of("TEXT"), ((Map<?, ?>) requests.get(0)).get("_semoss_input_modalities"));
        assertFalse(((Map<?, ?>) requests.get(1)).containsKey("_semoss_input_modalities"));
        assertFalse(history.containsKey("_semoss_input_modalities"));
    }

    private static Properties properties() {
        Properties properties = new Properties();
        properties.setProperty(Constants.ENGINE, "model-id");
        properties.setProperty(Constants.ENGINE_ALIAS, "test-model");
        properties.setProperty(Settings.VAR_NAME, "test_model");
        return properties;
    }

    private static InputMessage document(String mime) {
        Room room = new Room();
        room.setId("document-test");
        InputMessage message = InputMessage.builder(room).build();
        message.addPart(new MediaMessagePart(new Gson().fromJson(
                "{\"mimeType\":\"" + mime + "\"}", MessageInputMedia.class)));
        return message;
    }

    private static String ask(TestEngine engine, Map<String, Object> parameters) {
        Insight insight = mock(Insight.class);
        when(insight.getUserId()).thenReturn("user-test");
        when(engine.pyTranslator.runDirectPyNoCancelTrace(eq(insight), anyString()))
                .thenReturn(Map.of("response", "ok", "prompt_tokens", 0, "response_tokens", 0));
        try (MockedStatic<ModelInferenceLogsUtils> logs = mockStatic(ModelInferenceLogsUtils.class)) {
            engine.askCall(null, insight, "room-test", parameters);
        }
        ArgumentCaptor<String> command = ArgumentCaptor.forClass(String.class);
        verify(engine.pyTranslator).runDirectPyNoCancelTrace(eq(insight), command.capture());
        return command.getValue();
    }

    private static class TestEngine extends AbstractPythonModelEngine {
        private ModelTypeEnum modelType = ModelTypeEnum.OPEN_AI;

        private TestEngine(Set<ModelModalityEnum> modalities) {
            this.inputModalities = modalities;
            this.varName = "test_model";
            this.pyTranslator = mock(PyTranslator.class);
            setBasic(true);
        }

        @Override
        protected void checkSocketStatus() {
            // Unit tests capture the Python call instead of starting a process.
        }

        @Override
        public ModelTypeEnum getModelType() {
            return modelType;
        }
    }
}
