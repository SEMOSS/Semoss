package prerna.engine.impl.model;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;

import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.om.Insight;

public class VertexAIChatCompletionRestEngine extends OpenAiChatCompletionRestEngine {

    private GoogleCredentials credentials = null;

    @Override
    protected AskModelEngineResponse askCall(String question, Object fullPrompt, String context, Insight insight, Map<String, Object> parameters) {
        // Refresh the access token
        String accessToken = getVertexAccessToken();
        if (accessToken != null) {
            this.headersMap.put("Authorization", "Bearer " + accessToken);
        }

        // Call the superclass's askCall method
        return super.askCall(question, fullPrompt, context, insight, parameters);
    }

    private String getVertexAccessToken() {
        try {
            // Initialize credentials if they are not already initialized
            if (credentials == null) {
                String serviceAccountKeyFile = this.smssProp.getProperty("SERVICE_ACCOUNT_KEY_FILE");
                if (serviceAccountKeyFile == null || serviceAccountKeyFile.trim().isEmpty()) {
                    throw new IllegalArgumentException("Service account key file path is not provided.");
                }

                credentials = ServiceAccountCredentials.fromStream(Files.newInputStream(Paths.get(serviceAccountKeyFile)))
                        .createScoped(Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
            }

            // Refresh credentials if expired
            credentials.refreshIfExpired();
            return credentials.getAccessToken().getTokenValue();

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}