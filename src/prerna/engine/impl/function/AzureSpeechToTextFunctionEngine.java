package prerna.engine.impl.function;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.microsoft.cognitiveservices.speech.CancellationReason;
import com.microsoft.cognitiveservices.speech.ResultReason;
import com.microsoft.cognitiveservices.speech.SpeechConfig;
import com.microsoft.cognitiveservices.speech.SpeechRecognizer;
import com.microsoft.cognitiveservices.speech.audio.AudioConfig;
import com.microsoft.cognitiveservices.speech.audio.AudioInputStream;
import com.microsoft.cognitiveservices.speech.audio.AudioStreamContainerFormat;
import com.microsoft.cognitiveservices.speech.audio.AudioStreamFormat;
import com.microsoft.cognitiveservices.speech.audio.PullAudioInputStream;

import prerna.util.Constants;

public class AzureSpeechToTextFunctionEngine extends AbstractFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(AzureSpeechToTextFunctionEngine.class);
	private static final String SPEECH_KEY = "SPEECH_KEY";
	private static final String SPEECH_REGION = "SPEECH_REGION";
	private static final String FILE_PATH = "filePath";
	private static final String FILE_TYPE_MP3 = ".mp3";
	private static final String FILE_TYPE_WAV = ".wav";
	private static final String SPEECH_RECOGNITION_LANGUAGE = "en-US";

	private String speechKey;
	private String speechRegion;

	private Semaphore stopRecognition;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.speechKey = smssProp.getProperty(SPEECH_KEY);
		this.speechRegion = smssProp.getProperty(SPEECH_REGION);

		if (this.requiredParameters == null || (this.requiredParameters.isEmpty())) {
			throw new RuntimeException("Must define the requiredParameters");
		}
		if (this.speechKey == null || this.speechKey.isEmpty()) {
			throw new RuntimeException("Must pass in a speech key");
		}
		if (this.speechRegion == null || this.speechRegion.isEmpty()) {
			throw new RuntimeException("Must pass in a speech region");
		}

	}

	@Override
	public Object execute(Map<String, Object> parameterValues) {
		stopRecognition = new Semaphore(0);

		StringBuilder text = new StringBuilder();
		File file = null;
		String filePath = null;
		String fileKeyName = null;

		// validate all the required keys are set
		if (this.requiredParameters != null && !this.requiredParameters.isEmpty()) {
			Set<String> missingParameters = new HashSet<>();
			for (String requiredParameters : this.requiredParameters) {
				if (!parameterValues.containsKey(requiredParameters)) {
					missingParameters.add(requiredParameters);
				}
			}
			if (!missingParameters.isEmpty()) {
				throw new IllegalArgumentException("Must define required keys: " + missingParameters);
			}
		}

		try {
			for (String key : parameterValues.keySet()) {
				if (key.contains(FILE_PATH)) {
					file = new File(parameterValues.get(key).toString());
					fileKeyName = file.getName();
					filePath = file.getPath();
				}
			}

			AudioConfig audioConfig = this.audioFileReader(fileKeyName, filePath);
			text = this.speechToTextConverter(audioConfig, text);

		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error: " + e.getMessage());
		}
		return text;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
	}

	public AudioConfig audioFileReader(String fileKeyName, String filePath)
			throws FileNotFoundException, IllegalArgumentException {
		AudioConfig audioConfig = null;
		if (fileKeyName.contains(FILE_TYPE_MP3)) { // install "gstreamer" in your server
			PullAudioInputStream pullAudio = AudioInputStream.createPullStream(new BinaryAudioStreamReader(filePath),
					AudioStreamFormat.getCompressedFormat(AudioStreamContainerFormat.MP3));
			audioConfig = AudioConfig.fromStreamInput(pullAudio);
		} else if (fileKeyName.contains(FILE_TYPE_WAV)) {
			audioConfig = AudioConfig.fromWavFileInput(filePath);

		} else {
			classLogger.error("Unsupported file type.Supported types are MP3 and WAV.");
			throw new IllegalArgumentException("Unsupported file type.Supported types are MP3 and WAV.");
		}
		return audioConfig;
	}

	public StringBuilder speechToTextConverter(AudioConfig audioConfig, StringBuilder text)
			throws InterruptedException, ExecutionException {
		SpeechConfig speechConfig = SpeechConfig.fromSubscription(speechKey, speechRegion);
		speechConfig.setSpeechRecognitionLanguage(SPEECH_RECOGNITION_LANGUAGE);

		SpeechRecognizer recognizer = new SpeechRecognizer(speechConfig, audioConfig);

		recognizer.recognizing.addEventListener((s, e) -> {
			classLogger.debug("Recognizing Text.");
		});

		recognizer.recognized.addEventListener((s, e) -> {
			if (e.getResult().getReason() == ResultReason.RecognizedSpeech) {
				text.append(e.getResult().getText()).append(" ");
				classLogger.debug("Recognized text: " + e.getResult().getText());
			} else if (e.getResult().getReason() == ResultReason.NoMatch) {
				classLogger.warn("Speech could not be recognized.");
			}
		});

		recognizer.canceled.addEventListener((s, e) -> {
			classLogger.info("Cancelled Reason: " + e.getReason());
			if (e.getReason() == CancellationReason.Error) {
				classLogger.info("Event cancelled");
				classLogger.warn("ErrorCode: " + e.getErrorCode());
				classLogger.warn("ErrorDetails: " + e.getErrorDetails());
			}

			stopRecognition.release();
		});

		recognizer.sessionStarted.addEventListener((s, e) -> {
			classLogger.info("Event started.");
		});

		recognizer.sessionStopped.addEventListener((s, e) -> {
			classLogger.info("Event ended.");
		});

		// Starts continuous recognition.
		recognizer.startContinuousRecognitionAsync().get();

		// Waits for completion.
		stopRecognition.acquire();

		recognizer.stopContinuousRecognitionAsync().get();

		audioConfig.close();
		speechConfig.close();
		recognizer.close();
		return text;
	}

}
