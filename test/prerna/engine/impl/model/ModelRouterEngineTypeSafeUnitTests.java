package prerna.engine.impl.model;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import prerna.auth.utils.SecurityModelMetadataUtils;
import prerna.engine.api.ITypeSafeEngine;
import prerna.engine.api.ModelModalityEnum;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageInputMedia;
import prerna.engine.impl.model.responses.TypeSafeModelEngineResponse;
import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.Utility;

class ModelRouterEngineTypeSafeUnitTests {

	@TempDir
	Path directory;

	@Test
	@SuppressWarnings({ "unchecked", "rawtypes" })
	void classifierGetsTheTurnsImagesOnlyWhenItsModalitiesAllowThem() throws Exception {
		ITypeSafeEngine classifier = mock(ITypeSafeEngine.class);
		when(classifier.evaluate(any(), any(), any(), any(), any())).thenReturn(TypeSafeModelEngineResponse.fromObject(
				Map.of("model", "jev", "usage", Map.of(), "answers",
						Map.of("route", Map.of("type", "choice", "choice", "Vision")))));
		InputMessage message = mock(InputMessage.class);
		List<MessageInputMedia> media = List.of(media("image/png", "aW1n"), media("application/pdf", "cGRm"));
		when(message.getMediaInfos()).thenReturn(media);
		try (MockedStatic<SecurityModelMetadataUtils> metadata = mockStatic(SecurityModelMetadataUtils.class);
				MockedStatic<Utility> utility = mockStatic(Utility.class)) {
			utility.when(() -> Utility.getEngine("classifier")).thenReturn(classifier);
			ModelRouterEngine router = openRouter();

			when(classifier.getInputModalities()).thenReturn(Set.of(ModelModalityEnum.TEXT, ModelModalityEnum.IMAGE));
			assertEquals("vision", route(router, message));
			// unconfigured classifiers stay text-only
			when(classifier.getInputModalities()).thenReturn(null);
			assertEquals("vision", route(router, message));
		}
		ArgumentCaptor<Object> state = ArgumentCaptor.forClass(Object.class);
		ArgumentCaptor<Map<String, String>> images = ArgumentCaptor.forClass((Class) Map.class);
		verify(classifier, times(2)).evaluate(state.capture(), any(), images.capture(), any(), any());
		assertEquals(List.of("<image_1>", "what is this?"), state.getAllValues().get(0));
		assertEquals(Map.of("<image_1>", "aW1n"), images.getAllValues().get(0));
		assertEquals("what is this?", state.getAllValues().get(1));
		assertEquals(Map.of(), images.getAllValues().get(1));
	}

	private static MessageInputMedia media(String mimeType, String base64) {
		MessageInputMedia media = mock(MessageInputMedia.class);
		when(media.resolveMimeType()).thenReturn(mimeType);
		when(media.getBase64Data()).thenReturn(base64);
		return media;
	}

	private static String route(ModelRouterEngine router, InputMessage message) throws Exception {
		Method select = ModelRouterEngine.class.getDeclaredMethod("selectRouteByLLM", String.class,
				InputMessage.class, Insight.class, Room.class, Map.class);
		select.setAccessible(true);
		return (String) select.invoke(router, "what is this?", message, null, null, new HashMap<String, Object>());
	}

	private ModelRouterEngine openRouter() throws Exception {
		Path config = directory.resolve("router.json");
		Files.writeString(config, """
				{"mode": "typesafe", "classifier_engine": "classifier", "default_route": "text",
				 "routes": [{"name": "Text", "engine_id": "text", "description": "Text only"},
				            {"name": "Vision", "engine_id": "vision", "description": "Needs vision"}]}
				""");
		ModelRouterEngine router = new ModelRouterEngine() {
			@Override
			public File resolveConfigFile() {
				return config.toFile();
			}
		};
		router.setBasic(true);
		Properties properties = new Properties();
		properties.setProperty(Constants.ENGINE, "router");
		properties.setProperty(Constants.ENGINE_ALIAS, "test-router");
		router.open(properties);
		return router;
	}
}
