package prerna.engine.impl.model;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import prerna.auth.utils.SecurityModelMetadataUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.util.Constants;
import prerna.util.Utility;

class ModelRouterEngineTurnRoutingUnitTests {

	@TempDir
	Path directory;

	@Test
	void toolResultsStayOnTheEngineThatServedTheTurnWithoutStickyRouting() throws Exception {
		IModelEngine code = target();
		IModelEngine fallback = target();
		InputMessage question = input("review my code", false);
		InputMessage toolResult = input("{\"entries\":[\"image.png\"]}", true);
		InputMessage nextQuestion = input("more code please", false);
		// the code route is down for the first turn, so the fallback serves it
		when(code.askRoom(same(question), any(), any())).thenThrow(new IllegalStateException("down"));
		Room room = mock(Room.class);
		when(room.getMessages()).thenReturn(List.<AbstractMessage>of(question, toolResult));
		try (MockedStatic<SecurityModelMetadataUtils> metadata = mockStatic(SecurityModelMetadataUtils.class);
				MockedStatic<Utility> utility = mockStatic(Utility.class);
				MockedStatic<RoomUtils> rooms = mockStatic(RoomUtils.class)) {
			utility.when(() -> Utility.getEngine("code")).thenReturn(code);
			utility.when(() -> Utility.getEngine("fallback")).thenReturn(fallback);
			rooms.when(() -> RoomUtils.createRoomForStatelessAsk(any(), any(), any(), any())).thenReturn(room);
			ModelRouterEngine router = openRouter();

			router.askCall(question, null, "room", null);
			router.askCall(toolResult, null, "room", null);
			router.askCall(nextQuestion, null, "room", null);
		}
		// routing the tool result again would pick the code route
		verify(fallback).askRoom(same(question), any(), any());
		verify(fallback).askRoom(same(toolResult), any(), any());
		// a new user turn is routed again, since sticky is off
		verify(code).askRoom(same(nextQuestion), any(), any());
		verify(code, times(2)).askRoom(any(), any(), any());
		verify(fallback, times(2)).askRoom(any(), any(), any());
	}

	private static IModelEngine target() {
		IModelEngine engine = mock(IModelEngine.class);
		when(engine.askRoom(any(), any(), any())).thenAnswer(call -> mock(AskModelEngineResponse.class));
		return engine;
	}

	private static InputMessage input(String text, boolean toolResult) {
		InputMessage message = mock(InputMessage.class);
		when(message.getFullInputPrompt()).thenReturn(text);
		when(message.hasToolResultPart()).thenReturn(toolResult);
		return message;
	}

	private ModelRouterEngine openRouter() throws Exception {
		Path config = directory.resolve("router.json");
		Files.writeString(config, """
				{"routes": [{"name": "Code", "engine_id": "code", "keywords": ["code"]}],
				 "default_route": "fallback"}
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
