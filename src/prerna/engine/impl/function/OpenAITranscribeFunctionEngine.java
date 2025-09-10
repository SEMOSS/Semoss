package prerna.engine.impl.function;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.FunctionTypeEnum;
import prerna.util.Constants;
import prerna.util.Utility;


public class OpenAITranscribeFunctionEngine extends AbstractFunctionEngine {
	private static final Logger classLogger = LogManager.getLogger(OpenAITranscribeFunctionEngine.class);
	
	public static final String URL = "URL";
	public static final String API_KEY = "API_KEY";
	public static final String MODEL = "MODEL";
	
	private String url;
	private String apiKey;
	private String model;
	
	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.apiKey = smssProp.getProperty(API_KEY);
		this.url = smssProp.getProperty(URL);
		
		if(this.apiKey == null || (this.apiKey.isEmpty())) {
			throw new RuntimeException("Must define the requiredParameters");
		}
		if(this.url == null || this.url.isEmpty()){
			throw new RuntimeException("Must pass in an access key");
		}		
	}

    @Override
    public Object execute(Map<String, Object> parameterValues) {
        String filePath = (String) parameterValues.getOrDefault("filePath", null);
        if (filePath == null || filePath.isEmpty()) {
            throw new IllegalArgumentException("Parameter 'filePath' is required.");
        }

        try {
            return transcribe(new File(filePath));
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Transcription failed: " + e.getMessage(), e);
        }
    }

    private String transcribe(File audioFile) throws IOException, InterruptedException {
        if (!audioFile.exists()) {
            throw new IOException("File does not exist: " + audioFile.getAbsolutePath());
        }

        String modelUrl = this.url.endsWith("/")
                ? this.url + "audio/transcriptions"
                : this.url + "/audio/transcriptions";

        String boundary = "----JavaBoundary" + UUID.randomUUID();
        byte[] body = buildMultipartBody(boundary, this.model, audioFile);

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(20))
                .build();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(modelUrl))
                .timeout(Duration.ofMinutes(5))
                .header("Authorization", "Bearer " + this.apiKey)
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        int status = response.statusCode();
        String respBody = response.body();

        if (status / 100 != 2) {
            throw new IOException("Non-2xx response (" + status + "): " + respBody);
        }
        return extractTextField(respBody);
    }

    private static byte[] buildMultipartBody(String boundary, String model, File audioFile) throws IOException {
        String lineBreak = "\r\n";
        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        bos.write(("--" + boundary + lineBreak).getBytes(StandardCharsets.UTF_8));
        bos.write(("Content-Disposition: form-data; name=\"model\"" + lineBreak).getBytes(StandardCharsets.UTF_8));
        bos.write(("Content-Type: text/plain; charset=UTF-8" + lineBreak + lineBreak).getBytes(StandardCharsets.UTF_8));
        bos.write((model + lineBreak).getBytes(StandardCharsets.UTF_8));

        String fileName = audioFile.getName();
        String contentType = guessAudioContentType(fileName);

        bos.write(("--" + boundary + lineBreak).getBytes(StandardCharsets.UTF_8));
        bos.write(("Content-Disposition: form-data; name=\"file\"; filename=\"" + fileName + "\"" + lineBreak).getBytes(StandardCharsets.UTF_8));
        bos.write(("Content-Type: " + contentType + lineBreak + lineBreak).getBytes(StandardCharsets.UTF_8));

        try (FileInputStream fis = new FileInputStream(audioFile)) {
            fis.transferTo(bos);
        }
        bos.write(lineBreak.getBytes(StandardCharsets.UTF_8));

        bos.write(("--" + boundary + "--" + lineBreak).getBytes(StandardCharsets.UTF_8));

        return bos.toByteArray();
    }

    private static String guessAudioContentType(String fileName) {
        String lower = fileName.toLowerCase();
        if (lower.endsWith(".wav")) return "audio/wav";
        if (lower.endsWith(".mp3")) return "audio/mpeg";
        if (lower.endsWith(".m4a")) return "audio/mp4";
        if (lower.endsWith(".flac")) return "audio/flac";
        if (lower.endsWith(".ogg")) return "audio/ogg";
        return "application/octet-stream";
    }

    private static String extractTextField(String json) {
        String key = "\"text\":";
        int idx = json.indexOf(key);
        if (idx == -1) return json;

        int start = json.indexOf('"', idx + key.length());
        int end = json.indexOf('"', start + 1);
        if (start != -1 && end != -1) {
            return json.substring(start + 1, end);
        }
        return json;
    }
    
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
	}
	
	@Override
	public String getCatalogSubType(Properties smssProp) {
		return FunctionTypeEnum.LOCAL_PYTHON_CUSTOM_EMBEDDINGS.name();
	}
}
