package prerna.engine.impl.function.aws;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.poi.xwpf.usermodel.XWPFParagraph;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.comprehend.AmazonComprehend;
import com.amazonaws.services.comprehend.AmazonComprehendClientBuilder;
import com.amazonaws.services.comprehend.model.ClassifyDocumentRequest;
import com.amazonaws.services.comprehend.model.ClassifyDocumentResult;
import com.amazonaws.services.comprehend.model.DetectDominantLanguageRequest;
import com.amazonaws.services.comprehend.model.DetectDominantLanguageResult;
import com.amazonaws.services.comprehend.model.DetectEntitiesRequest;
import com.amazonaws.services.comprehend.model.DetectEntitiesResult;
import com.amazonaws.services.comprehend.model.DetectKeyPhrasesRequest;
import com.amazonaws.services.comprehend.model.DetectKeyPhrasesResult;
import com.amazonaws.services.comprehend.model.DetectPiiEntitiesRequest;
import com.amazonaws.services.comprehend.model.DetectPiiEntitiesResult;
import com.amazonaws.services.comprehend.model.DetectSentimentRequest;
import com.amazonaws.services.comprehend.model.DetectSentimentResult;
import com.amazonaws.services.comprehend.model.DetectTargetedSentimentRequest;
import com.amazonaws.services.comprehend.model.DetectTargetedSentimentResult;
import com.amazonaws.services.comprehend.model.DetectToxicContentRequest;
import com.amazonaws.services.comprehend.model.DetectToxicContentResult;
import com.amazonaws.services.comprehend.model.DocumentClass;
import com.amazonaws.services.comprehend.model.DominantLanguage;
import com.amazonaws.services.comprehend.model.Entity;
import com.amazonaws.services.comprehend.model.KeyPhrase;
import com.amazonaws.services.comprehend.model.LanguageCode;
import com.amazonaws.services.comprehend.model.PiiEntity;
import com.amazonaws.services.comprehend.model.TargetedSentimentEntity;
import com.amazonaws.services.comprehend.model.TextSegment;
import com.amazonaws.services.comprehend.model.ToxicLabels;

import prerna.engine.api.AWSComprehendInsightTypeEnum;
import prerna.engine.impl.function.AbstractFunctionEngine;
import prerna.util.Constants;

public class AWSComprehendFunctionEngine extends AbstractFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(AWSComprehendFunctionEngine.class);
	private static final String ACCESS_KEY = "ACCESS_KEY";
	private static final String SECRET_KEY = "SECRET_KEY";
	private static final String REGION = "REGION";
	private static final String LANGUAGE_CODE = "languageCode";
	private static final String FILE_PATH = "filePath";
	private static final String INSIGHT_TYPES = "insightTypes";
	private static final String AWS_PROMPT_SAFETY_ARN = "arn:aws:comprehend:%s:aws:document-classifier-endpoint/prompt-safety";
	private static final String FILE_TYPE_TEXT = ".txt";
	private static final String FILE_TYPE_PDF = ".pdf";
	private static final String FILE_TYPE_DOCX = ".docx";
	private static final String KEY_DOMINANT_LANGUAGE = "DominantLanguage";
	private static final String KEY_ENTITIES = "Entities";
	private static final String KEY_SENTIMENT = "Sentiment";
	private static final String KEY_TARGETED_SENTIMENT = "TargetedSentiment";
	private static final String KEY_PII = "PII";
	private static final String KEY_TOXICITY_DETECTION = "ToxicityDetection";
	private static final String KEY_PROMPT_SAFETY = "PromptSafetyClassification";
	private static final String KEY_KEY_PHRASES = "KeyPhrases";
	private static final String ERROR = "Error: ";

	private String accessKey;
	private String secretKey;
	private String region;

	@Override
	public void open(Properties smssProp) throws Exception {
		String error_requiredParametrers = "Must define the Required Parameters.";
		String error_accessKey = "Must pass a access key.";
		String error_secretKey = "Must pass a secret key.";
		String error_region = "Must pass a region.";

		super.open(smssProp);

		this.accessKey = smssProp.getProperty(ACCESS_KEY);
		this.secretKey = smssProp.getProperty(SECRET_KEY);
		this.region = smssProp.getProperty(REGION);

		if (this.requiredParameters == null || (this.requiredParameters.isEmpty())) {
			classLogger.error(error_requiredParametrers);
			throw new RuntimeException(error_requiredParametrers);
		}
		if (this.accessKey == null || this.accessKey.isEmpty()) {
			classLogger.error(error_accessKey);
			throw new RuntimeException(error_accessKey);
		}
		if (this.secretKey == null || this.secretKey.isEmpty()) {
			classLogger.error(error_secretKey);
			throw new RuntimeException(error_secretKey);
		}
		if (this.region == null || this.region.isEmpty()) {
			classLogger.error(error_region);
			throw new RuntimeException(error_region);
		}
	}

	@Override
	public Object execute(Map<String, Object> parameterValues) {
		String error_requiredKeys = "Must define required keys: ";
		File file = null;
		String filePath = null;
		String insightTypes = null;
		String languageCode = null;

		// validate all the required keys are set
		if (this.requiredParameters != null && !this.requiredParameters.isEmpty()) {
			Set<String> missingParameters = new HashSet<>();
			for (String requiredParameters : this.requiredParameters) {
				if (!parameterValues.containsKey(requiredParameters)) {
					missingParameters.add(requiredParameters);
				}
			}
			if (!missingParameters.isEmpty()) {
				classLogger.error(error_requiredKeys + missingParameters);
				throw new IllegalArgumentException(error_requiredKeys + missingParameters);
			}
		}

		try {
			for (String key : parameterValues.keySet()) {
				if (key.contains(FILE_PATH)) {
					filePath = parameterValues.get(key).toString();
					file = new File(filePath);
				}
				if (key.contains(INSIGHT_TYPES)) {
					insightTypes = parameterValues.get(key).toString();
				}
				if (key.contains(LANGUAGE_CODE)) {
					languageCode = parameterValues.get(key).toString();
				}
			}

			// validate the language
			languageCode = this.validateLanguageCode(languageCode);

			// Validate the insight types
			List<String> insightTypeList = this.validateInsightTypes(insightTypes);

			// Validate the file
			String text = this.validateFile(filePath, file);

			// Analyze file content
			Map<String, Object> response = this.analyzeText(text, insightTypeList, languageCode);

			return response;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(ERROR + e.getMessage());
		}
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
	}

	/**
	 * 
	 * @param languageCode
	 * @return
	 */
	public String validateLanguageCode(String languageCode) {
		String error_invalidLanguageCode = "Invalid language code: ";

		if (StringUtils.isEmpty(languageCode)) {
			languageCode = LanguageCode.En.toString();
		} else {
			try {
				LanguageCode.fromValue(languageCode);
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, error_invalidLanguageCode + languageCode);
				throw new IllegalArgumentException(error_invalidLanguageCode + languageCode);
			}
		}
		return languageCode;
	}

	/**
	 * 
	 * @param insightTypes
	 * @return
	 */
	public List<String> validateInsightTypes(String insightTypes) {
		List<String> insightTypeEnumList = new ArrayList<String>();

		if (StringUtils.isEmpty(insightTypes) || insightTypes.toLowerCase()
				.contains(AWSComprehendInsightTypeEnum.ALL.getInsightType().toLowerCase())) {
			insightTypeEnumList = AWSComprehendInsightTypeEnum.getAllValidInsightTypes();
		} else {
			List<String> insightTypesList = Arrays.asList(insightTypes.split(","));
			insightTypeEnumList.addAll(insightTypesList);
		}

		return insightTypeEnumList;
	}

	public String validateFile(String filePath, File file) {
		String error_invalidFileType = String.format("Invalid file type. Supported file types: %s, %s, %s .",
				FILE_TYPE_TEXT, FILE_TYPE_PDF, FILE_TYPE_DOCX);
		String error_invalidContent = "Invalid file content.";
		String text = null;

		// Extract file content
		if (filePath.endsWith(FILE_TYPE_DOCX)) {
			text = this.readDocxFile(file);
		} else if (filePath.endsWith(FILE_TYPE_PDF)) {
			text = this.readPdfFile(file);
		} else if (filePath.endsWith(FILE_TYPE_TEXT)) {
			text = this.readTextFile(file);
		} else {

			classLogger.error(error_invalidFileType);
			throw new RuntimeException(error_invalidFileType);
		}
		text = text.trim();
		// Validate file content
		if (StringUtils.isEmpty(text) || text == null) {
			classLogger.error(error_invalidContent);
			throw new RuntimeException(error_invalidContent);
		}
		return text;
	}

	public String readTextFile(File file) {
		try {
			byte[] bytes = Files.readAllBytes(file.toPath());
			String text = new String(bytes);
			return text;
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(ERROR + e.getMessage());
		}
	}

	public String readDocxFile(File file) {
		try {
			String text = "";
			FileInputStream fis = new FileInputStream(file.getAbsolutePath());
			XWPFDocument xwpfDocument = new XWPFDocument(fis);
			List<XWPFParagraph> xwpfParagraphs = xwpfDocument.getParagraphs();

			for (XWPFParagraph xwpfParagraph : xwpfParagraphs) {
				text = text + "\n" + xwpfParagraph.getText();
			}
			fis.close();
			xwpfDocument.close();
			return text;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(ERROR + e.getMessage());
		}
	}

	public String readPdfFile(File file) {
		try {
			PDDocument pdDocument = PDDocument.load(file);
			String text = new PDFTextStripper().getText(pdDocument);
			return text;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(ERROR + e.getMessage());
		}
	}

	public AmazonComprehend buildAmazonComprehendClient() {
		BasicAWSCredentials credentials = new BasicAWSCredentials(this.accessKey, this.secretKey);
		AmazonComprehend client = AmazonComprehendClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion(this.region).build();
		return client;
	}

	public List<DominantLanguage> detectDominantLanguage(AmazonComprehend client, String text, String languageCode) {
		DetectDominantLanguageRequest detectDominantLanguageRequest = new DetectDominantLanguageRequest()
				.withText(text);
		DetectDominantLanguageResult detectDominantLanguage = client
				.detectDominantLanguage(detectDominantLanguageRequest);
		List<DominantLanguage> languages = detectDominantLanguage.getLanguages();
		return languages;
	}

	public List<Entity> detectEntities(AmazonComprehend client, String text, String languageCode) {
		DetectEntitiesRequest detectEntitiesRequest = new DetectEntitiesRequest().withText(text)
				.withLanguageCode(languageCode);
		DetectEntitiesResult detectEntitiesResult = client.detectEntities(detectEntitiesRequest);
		List<Entity> entities = detectEntitiesResult.getEntities();
		return entities;
	}

	public String detectSentiment(AmazonComprehend client, String text, String languageCode) {
		DetectSentimentRequest detectSentimentRequest = new DetectSentimentRequest().withText(text)
				.withLanguageCode(languageCode);
		DetectSentimentResult detectSentiment = client.detectSentiment(detectSentimentRequest);
		String sentiment = detectSentiment.getSentiment();
		return sentiment;
	}

	public List<TargetedSentimentEntity> detectTargetedSentiment(AmazonComprehend client, String text,
			String languageCode) {
		DetectTargetedSentimentRequest detectTargetedSentimentRequest = new DetectTargetedSentimentRequest()
				.withText(text).withLanguageCode(languageCode);
		DetectTargetedSentimentResult detectTargetedSentiment = client
				.detectTargetedSentiment(detectTargetedSentimentRequest);
		List<TargetedSentimentEntity> targetedSentimentEntities = detectTargetedSentiment.getEntities();
		return targetedSentimentEntities;
	}

	public List<PiiEntity> detectPiiEntities(AmazonComprehend client, String text, String languageCode) {
		DetectPiiEntitiesRequest detectPiiEntitiesRequest = new DetectPiiEntitiesRequest().withText(text)
				.withLanguageCode(languageCode);
		DetectPiiEntitiesResult detectPiiEntities = client.detectPiiEntities(detectPiiEntitiesRequest);
		List<PiiEntity> piiEntities = detectPiiEntities.getEntities();
		return piiEntities;
	}

	public List<ToxicLabels> detectToxicContent(AmazonComprehend client, String text, String languageCode) {
		TextSegment textSegments = new TextSegment();
		textSegments.setText(text);
		DetectToxicContentRequest detectToxicContentRequest = new DetectToxicContentRequest()
				.withTextSegments(textSegments).withLanguageCode(languageCode);
		DetectToxicContentResult detectToxicContent = client.detectToxicContent(detectToxicContentRequest);
		List<ToxicLabels> toxicLabels = detectToxicContent.getResultList();
		return toxicLabels;
	}

	public List<DocumentClass> detectPromptSafety(AmazonComprehend client, String text, String languageCode) {
		String promptSafetyArn = String.format(AWS_PROMPT_SAFETY_ARN, region);
		ClassifyDocumentRequest classifyDocumentRequest = new ClassifyDocumentRequest().withText(text)
				.withEndpointArn(promptSafetyArn);
		ClassifyDocumentResult classifyDocument = client.classifyDocument(classifyDocumentRequest);
		List<DocumentClass> classes = classifyDocument.getClasses();
		return classes;
	}

	public List<KeyPhrase> detectKeyPhrases(AmazonComprehend client, String text, String languageCode) {
		DetectKeyPhrasesRequest detectKeyPhrasesRequest = new DetectKeyPhrasesRequest().withText(text)
				.withLanguageCode(languageCode);
		DetectKeyPhrasesResult detectKeyPhrases = client.detectKeyPhrases(detectKeyPhrasesRequest);
		List<KeyPhrase> keyPhrases = detectKeyPhrases.getKeyPhrases();
		return keyPhrases;
	}

	public Map<String, Object> analyzeText(String text, List<String> insightTypeList, String languageCode) {
		String error_invalidInsightType = "Invalid insight type: ";
		Map<String, Object> fullResponse = new HashMap<String, Object>();
		AmazonComprehend client = this.buildAmazonComprehendClient();

		for (String insightType : insightTypeList) {
			insightType = insightType.trim();
			AWSComprehendInsightTypeEnum enumValueOf = AWSComprehendInsightTypeEnum.getEnumFromInsightType(insightType);
			switch (enumValueOf) {
			case DOMINANT_LANGUAGE:
				List<DominantLanguage> languages = this.detectDominantLanguage(client, text, languageCode);
				fullResponse.put(KEY_DOMINANT_LANGUAGE, languages);
				break;
			case ENTITIES:
				List<Entity> detectEntities = this.detectEntities(client, text, languageCode);
				fullResponse.put(KEY_ENTITIES, detectEntities);
				break;
			case SENTIMENT:
				String sentiment = this.detectSentiment(client, text, languageCode);
				fullResponse.put(KEY_SENTIMENT, sentiment);
				break;
			case TARGETED_SENTIMENT:
				List<TargetedSentimentEntity> targetedSentimentEntities = this.detectTargetedSentiment(client, text,
						languageCode);
				fullResponse.put(KEY_TARGETED_SENTIMENT, targetedSentimentEntities);
				break;
			case PII_ENTITIES:
				List<PiiEntity> piiEntities = this.detectPiiEntities(client, text, languageCode);
				fullResponse.put(KEY_PII, piiEntities);
				break;
			case TOXIC_CONTENT:
				List<ToxicLabels> toxicLabels = this.detectToxicContent(client, text, languageCode);
				fullResponse.put(KEY_TOXICITY_DETECTION, toxicLabels);
				break;
			case PROMPT_SAFETY:
				List<DocumentClass> promptSafetyClasses = this.detectPromptSafety(client, text, languageCode);
				fullResponse.put(KEY_PROMPT_SAFETY, promptSafetyClasses);
				break;
			case KEY_PHRASES:
				List<KeyPhrase> keyPhrases = this.detectKeyPhrases(client, text, languageCode);
				fullResponse.put(KEY_KEY_PHRASES, keyPhrases);
				break;
			default:
				classLogger.error(Constants.STACKTRACE, error_invalidInsightType + insightType);
				throw new IllegalArgumentException(error_invalidInsightType + insightType);
			}
		}
		return fullResponse;
	}
}