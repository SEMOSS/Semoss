package prerna.engine.impl.model.workers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Properties;

import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.engine.api.IEngine;
import prerna.engine.impl.CaseInsensitiveProperties;
import prerna.engine.impl.model.AbstractModelEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.vector.AbstractVectorDatabaseEngine;
import prerna.engine.impl.vector.PGVectorDatabaseEngine;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class ModelEngineInferenceLogsWorkerUnitTests {
    private User user;
    private IEngine engine;
    private Insight insight;
    private IProject project;
    private AccessToken token;
    private VarStore varStore;
    private NounMetadata metadata;
    ModelEngineInferenceLogsWorker reactor;

    @BeforeEach
    void setUp() {
        user = mock(User.class);
        insight = mock(Insight.class);
        project = mock(IProject.class);
        token = mock(AccessToken.class);
        varStore = mock(VarStore.class);
        metadata = mock(NounMetadata.class);
    }

    @Test
    void test() {
        engine = mock(AbstractModelEngine.class);
        reactor = new ModelEngineInferenceLogsWorker(
            "id",
            "method",
            engine,
            insight.getInsightId(),
			insight.getContextProjectId(),
			insight.getProjectId(),
			insight.getUser(),
            "sessionId",
			"roomId",
            "context",
            "",
            new ArrayList(){{add("full prompt");}},
            0,
            ZonedDateTime.of(LocalDate.now(), LocalTime.now(), ZoneId.systemDefault()),
            "response",
            1,
            ZonedDateTime.of(LocalDate.now(), LocalTime.now(), ZoneId.systemDefault())
        );

        when(engine.getSmssProp()).thenReturn(new Properties());
        when(engine.getCatalogSubType(new Properties())).thenReturn("");

        String sessionId = "sessionId";
		String roomId = "roomId";
        String projectId = "projectId";
        when(insight.getContextProjectId()).thenReturn(null);
        when(insight.getProjectId()).thenReturn(projectId);

        try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
            MockedStatic<ModelInferenceLogsUtils> milUtils = Mockito.mockStatic(ModelInferenceLogsUtils.class)) {
            util.when(() -> Utility.getProject(projectId)).thenReturn(project);

            when(project.getProjectName()).thenReturn("porjectName");

            String insightId = "insightId";
            when(insight.getInsightId()).thenReturn(insightId);
            when(insight.getUser()).thenReturn(user);
            when(user.getPrimaryLoginToken()).thenReturn(token);
            when(token.getId()).thenReturn("userId");
            when(token.getName()).thenReturn(null);
            when(token.getUsername()).thenReturn("userUsername");
            when(token.getEmail()).thenReturn("userEmail");
            
            String engineId = "engineId";
            String engineName = "engineName";
			String userEmail= "userEmail";
            when(engine.getEngineId()).thenReturn(engineId);
            when(engine.getEngineName()).thenReturn(engineName);
            milUtils.when(() -> ModelInferenceLogsUtils.doModelIsRegistered(engineId)).thenReturn(false);
            milUtils.when(() -> ModelInferenceLogsUtils.doCreateNewAgent(engineId, engineName, null, "", "tokenId")).thenAnswer((Answer<Void>) invocation -> null);

            milUtils.when(() -> ModelInferenceLogsUtils.doCheckRoomExists(insightId)).thenReturn(false);
            milUtils.when(() -> ModelInferenceLogsUtils.doCreateNewConversation(
                insightId,
                "full prompt",
                null,
                "userId",
                userEmail,
                "tokenUsername",
                "",
                engineId,
                true,
                projectId,
                "porjectName"
            )).thenAnswer((Answer<Void>) invocation -> null);
            
            milUtils.when(() -> ModelInferenceLogsUtils.setRoomContext(insightId, "userId", "userUsername")).thenAnswer((Answer<Void>) invocation -> null);
            
            when(((AbstractModelEngine) engine).keepInputOutput()).thenReturn(true);

            milUtils.when(() -> ModelInferenceLogsUtils.doRecordMessage(
                eq("messageId"),
                eq("INPUT"),
                eq("full prompt"),
                eq("method"),
                eq(0),
                any(Double.class),
                any(ZonedDateTime.class),
                eq(engineId),
                eq(insightId),
                eq(sessionId),
				eq(insightId), //room id 
                eq("userId"),
                eq("userUserName"),
                eq(userEmail)
            )).thenAnswer((Answer<Void>) invocation -> null);
            milUtils.when(() -> ModelInferenceLogsUtils.doRecordMessage(
                eq("messageId"),
                eq("RESPONSE"),
                eq("full prompt"),
                eq("method"),
                eq(0),
                any(Double.class),
                any(ZonedDateTime.class),
                eq(engineId),
                eq(insightId),
                eq(sessionId),
				eq(insightId), //room id
                eq("userId"),
                eq("userUserName"),
                eq(userEmail)
            )).thenAnswer((Answer<Void>) invocation -> null);
            
            /////////////////////
            /// Method to Run ///
            /////////////////////
            
            reactor.run();

            milUtils.verify(() -> {
                ModelInferenceLogsUtils.doModelIsRegistered(engineId);

                ModelInferenceLogsUtils.doCreateNewAgent(engineId, engineName, null, "", "tokenId");

                ModelInferenceLogsUtils.doCheckRoomExists(insightId);

                ModelInferenceLogsUtils.doCreateNewConversation(
                    insightId,
                    "full prompt",
                    null,
                    "userId",
                    userEmail,
                    "tokenUsername",
                    "",
                    engineId,
                    true,
                    projectId,
                    "porjectName"
                );

                ModelInferenceLogsUtils.setRoomContext(insightId, "userId", "userUsername");

                ModelInferenceLogsUtils.doRecordMessage(
                eq("messageId"),
                eq("INPUT"),
                eq("full prompt"),
                eq("method"),
                eq(0),
                any(Double.class),
                any(ZonedDateTime.class),
                eq(engineId),
                eq(insightId),
                eq(sessionId),
				eq(insightId), //room id
                eq("userId"),
                eq("userUserName"),
                eq(userEmail)
            );

                ModelInferenceLogsUtils.doRecordMessage(
                    eq("messageId"),
                    eq("RESPONSE"),
                    eq("full prompt"),
                    eq("method"),
                    eq(0),
                    any(Double.class),
                    any(ZonedDateTime.class),
                    eq(engineId),
                    eq(insightId),
                    eq(sessionId),
					eq(insightId), //room id
                    eq("userId"),
                    eq("userUserName"),
                    eq(userEmail)
                );
            });
        }
    }

    @Test
    void testAbstractVectorDatabaseEngine() {
        engine = mock(AbstractVectorDatabaseEngine.class);
        reactor = new ModelEngineInferenceLogsWorker(
            "id",
            "method",
            engine,
            insight.getInsightId(),
			insight.getContextProjectId(),
			insight.getProjectId(),
			insight.getUser(),
            "sessionId",
			"roomId",
            "context",
            "",
            new ArrayList(){{add(new JSONObject());}},
            0,
            ZonedDateTime.of(LocalDate.now(), LocalTime.now(), ZoneId.systemDefault()),
            "response",
            1,
            ZonedDateTime.of(LocalDate.now(), LocalTime.now(), ZoneId.systemDefault())
        );

        when(engine.getSmssProp()).thenReturn(new Properties());
        when(engine.getCatalogSubType(new Properties())).thenReturn("");

        String sessionId = "sessionId";
		String roomId = "roomId";
        String projectId = "projectId";
        when(insight.getContextProjectId()).thenReturn(null);
        when(insight.getProjectId()).thenReturn(projectId);

        try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
            MockedStatic<ModelInferenceLogsUtils> milUtils = Mockito.mockStatic(ModelInferenceLogsUtils.class)) {
            util.when(() -> Utility.getProject(projectId)).thenReturn(project);

            when(project.getProjectName()).thenReturn("porjectName");

            String insightId = "insightId";
            when(insight.getInsightId()).thenReturn(insightId);
            when(insight.getUser()).thenReturn(user);
            when(user.getPrimaryLoginToken()).thenReturn(token);
            when(token.getId()).thenReturn("userId");
            when(token.getName()).thenReturn(null);
            when(token.getUsername()).thenReturn(null);
            when(token.getEmail()).thenReturn("userEmail");
            
            String engineId = "engineId";
            String engineName = "engineName";
			String userEmail="userEmail";
            when(engine.getEngineId()).thenReturn(engineId);
            when(engine.getEngineName()).thenReturn(engineName);
            milUtils.when(() -> ModelInferenceLogsUtils.doModelIsRegistered(engineId)).thenReturn(false);
            milUtils.when(() -> ModelInferenceLogsUtils.doCreateNewAgent(engineId, engineName, null, "", "tokenId")).thenAnswer((Answer<Void>) invocation -> null);

            milUtils.when(() -> ModelInferenceLogsUtils.doCheckRoomExists(insightId)).thenReturn(false);
            milUtils.when(() -> ModelInferenceLogsUtils.doCreateNewConversation(
                insightId,
                "full prompt",
                null,
                "userId",
                userEmail,
                "tokenUsername",
                "",
                engineId,
                true,
                projectId,
                "porjectName"
            )).thenAnswer((Answer<Void>) invocation -> null);
            
            milUtils.when(() -> ModelInferenceLogsUtils.setRoomContext(insightId, "userId", "userUsername")).thenAnswer((Answer<Void>) invocation -> null);
            
            when(((AbstractVectorDatabaseEngine) engine).keepInputOutput()).thenReturn(false);

            milUtils.when(() -> ModelInferenceLogsUtils.doRecordMessage(
                eq("messageId"),
                eq("INPUT"),
                eq("full prompt"),
                eq("method"),
                eq(0),
                any(Double.class),
                any(ZonedDateTime.class),
                eq(engineId),
                eq(insightId),
                eq(sessionId),
				eq(insightId), //room id
                eq("userId"),
                eq("userUserName"),
                eq(userEmail)
            )).thenAnswer((Answer<Void>) invocation -> null);
            milUtils.when(() -> ModelInferenceLogsUtils.doRecordMessage(
                eq("messageId"),
                eq("RESPONSE"),
                eq("full prompt"),
                eq("method"),
                eq(0),
                any(Double.class),
                any(ZonedDateTime.class),
                eq(engineId),
                eq(insightId),
                eq(sessionId),
				eq(insightId), //room id
                eq("userId"),
                eq("userUserName"),
                eq(userEmail)
            )).thenAnswer((Answer<Void>) invocation -> null);
            
            /////////////////////
            /// Method to Run ///
            /////////////////////
            
            reactor.run();

            milUtils.verify(() -> {
                ModelInferenceLogsUtils.doModelIsRegistered(engineId);

                ModelInferenceLogsUtils.doCreateNewAgent(engineId, engineName, null, "", "tokenId");

                ModelInferenceLogsUtils.doCheckRoomExists(insightId);

                ModelInferenceLogsUtils.doCreateNewConversation(
                    insightId,
                    "full prompt",
                    null,
                    "userId",
                    "tokenUsername",
                    userEmail,
                    "",
                    engineId,
                    true,
                    projectId,
                    "porjectName"
                );

                ModelInferenceLogsUtils.setRoomContext(insightId, "userId", "userUsername");

                ModelInferenceLogsUtils.doRecordMessage(
                eq("messageId"),
                eq("INPUT"),
                eq("full prompt"),
                eq("method"),
                eq(0),
                any(Double.class),
                any(ZonedDateTime.class),
                eq(engineId),
                eq(insightId),
                eq(sessionId),
				eq(insightId), //room id
                eq("userId"),
                eq("userUserName"),
                eq(userEmail)
            );

                ModelInferenceLogsUtils.doRecordMessage(
                    eq("messageId"),
                    eq("RESPONSE"),
                    eq("full prompt"),
                    eq("method"),
                    eq(0),
                    any(Double.class),
                    any(ZonedDateTime.class),
                    eq(engineId),
                    eq(insightId),
                    eq(sessionId),
					eq(insightId), //room id
                    eq("userId"),
                    eq("userUserName"),
                    eq(userEmail)
                );
            });
        }
    }

    @Test
    void testPGVectorDatabaseEngine() {
        engine = mock(PGVectorDatabaseEngine.class);
        reactor = new ModelEngineInferenceLogsWorker(
            "id",
            "method",
            engine,
            insight.getInsightId(),
			insight.getContextProjectId(),
			insight.getProjectId(),
			insight.getUser(),
            "sessionId",
			"roomId",
            "context",
            "",
            new JSONObject(),
            0,
            ZonedDateTime.of(LocalDate.now(), LocalTime.now(), ZoneId.systemDefault()),
            "response",
            1,
            ZonedDateTime.of(LocalDate.now(), LocalTime.now(), ZoneId.systemDefault())
        );

        when(engine.getSmssProp()).thenReturn(new CaseInsensitiveProperties());
        when(engine.getCatalogSubType(new Properties())).thenReturn("");

        String sessionId = "sessionId";
		String roomId = "roomId";
        String projectId = "projectId";
        when(insight.getContextProjectId()).thenReturn(null);
        when(insight.getProjectId()).thenReturn(projectId);

        try (MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);
            MockedStatic<ModelInferenceLogsUtils> milUtils = Mockito.mockStatic(ModelInferenceLogsUtils.class)) {
            util.when(() -> Utility.getProject(projectId)).thenReturn(project);

            when(project.getProjectName()).thenReturn("porjectName");

            String insightId = "insightId";
            when(insight.getInsightId()).thenReturn(insightId);
            when(insight.getUser()).thenReturn(user);
            when(user.getPrimaryLoginToken()).thenReturn(token);
            when(token.getId()).thenReturn("userId");
            when(token.getName()).thenReturn(null);
            when(token.getUsername()).thenReturn(null);
            when(token.getEmail()).thenReturn("userEmail");
            
            String engineId = "engineId";
            String engineName = "engineName";
			String userEmail="userEmail";
            when(engine.getEngineId()).thenReturn(engineId);
            when(engine.getEngineName()).thenReturn(engineName);
            milUtils.when(() -> ModelInferenceLogsUtils.doModelIsRegistered(engineId)).thenReturn(false);
            milUtils.when(() -> ModelInferenceLogsUtils.doCreateNewAgent(engineId, engineName, null, "", "tokenId")).thenAnswer((Answer<Void>) invocation -> null);

            milUtils.when(() -> ModelInferenceLogsUtils.doCheckRoomExists(insightId)).thenReturn(false);
            milUtils.when(() -> ModelInferenceLogsUtils.doCreateNewConversation(
                insightId,
                "full prompt",
                null,
                "userId",
                userEmail,
                "tokenUsername",
                "",
                engineId,
                true,
                projectId,
                "porjectName"
            )).thenAnswer((Answer<Void>) invocation -> null);
            
            milUtils.when(() -> ModelInferenceLogsUtils.setRoomContext(insightId, "userId", "userUsername")).thenAnswer((Answer<Void>) invocation -> null);
            
            when(((PGVectorDatabaseEngine) engine).keepInputOutput()).thenReturn(false);

            milUtils.when(() -> ModelInferenceLogsUtils.doRecordMessage(
                eq("messageId"),
                eq("INPUT"),
                eq("full prompt"),
                eq("method"),
                eq(0),
                any(Double.class),
                any(ZonedDateTime.class),
                eq(engineId),
                eq(insightId),
                eq(sessionId),
				eq(insightId), //room id
                eq("userId"),
                eq("userUserName"),
                eq(userEmail)
            )).thenAnswer((Answer<Void>) invocation -> null);
            milUtils.when(() -> ModelInferenceLogsUtils.doRecordMessage(
                eq("messageId"),
                eq("RESPONSE"),
                eq("full prompt"),
                eq("method"),
                eq(0),
                any(Double.class),
                any(ZonedDateTime.class),
                eq(engineId),
                eq(insightId),
                eq(sessionId),
				eq(insightId), //room id
                eq("userId"),
                eq("userUserName"),
                eq(userEmail)
            )).thenAnswer((Answer<Void>) invocation -> null);
            
            /////////////////////
            /// Method to Run ///
            /////////////////////
            
            reactor.run();

            milUtils.verify(() -> {
                ModelInferenceLogsUtils.doModelIsRegistered(engineId);

                ModelInferenceLogsUtils.doCreateNewAgent(engineId, engineName, null, "", "tokenId");

                ModelInferenceLogsUtils.doCheckRoomExists(insightId);

                ModelInferenceLogsUtils.doCreateNewConversation(
                    insightId,
                    "full prompt",
                    null,
                    "userId",
                    userEmail,
                    "tokenUsername",
                    "",
                    engineId,
                    true,
                    projectId,
                    "porjectName"
                );

                ModelInferenceLogsUtils.setRoomContext(insightId, "userId", "userUsername");

                ModelInferenceLogsUtils.doRecordMessage(
                eq("messageId"),
                eq("INPUT"),
                eq("full prompt"),
                eq("method"),
                eq(0),
                any(Double.class),
                any(ZonedDateTime.class),
                eq(engineId),
                eq(insightId),
                eq(sessionId),
				eq(insightId), //room id
                eq("userId"),
                eq("userUserName"),
                eq(userEmail)
            );

                ModelInferenceLogsUtils.doRecordMessage(
                    eq("messageId"),
                    eq("RESPONSE"),
                    eq("full prompt"),
                    eq("method"),
                    eq(0),
                    any(Double.class),
                    any(ZonedDateTime.class),
                    eq(engineId),
                    eq(insightId),
                    eq(sessionId),
					eq(insightId), //room id
                    eq("userId"),
                    eq("userUserName"),
                    eq(userEmail)
                );
            });
        }

    }
}
