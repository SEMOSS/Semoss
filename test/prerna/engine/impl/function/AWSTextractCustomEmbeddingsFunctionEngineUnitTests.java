package prerna.engine.impl.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Vector;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.openpdf.text.Document;
import org.openpdf.text.Paragraph;
import org.openpdf.text.pdf.PdfWriter;

import com.google.gson.Gson;

import prerna.auth.User;
import prerna.engine.api.FunctionTypeEnum;
import prerna.engine.api.IFunctionEngine;
import prerna.om.Insight;
import prerna.util.Constants;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.textract.TextractClient;
import software.amazon.awssdk.services.textract.TextractClientBuilder;

public class AWSTextractCustomEmbeddingsFunctionEngineUnitTests {
	private Insight insight;
	private User user;
	private AWSTextractCustomEmbeddingsFunctionEngine engine;
	
	@BeforeEach
	void setUp() {
		user = mock(User.class);
		// using inner class to access abstract methods
		engine = new AWSTextractCustomEmbeddingsFunctionEngine();
		insight = mock(Insight.class);
	}
	
	@Test
	void testOpenWithProperties() throws Exception {
		Properties testProps = new Properties();
		String testEngine = "asdf-1234";
		String testEngineName = "engine_name";
		String funtionName = "function_name";
		String functionDescription = "function_description";
		String accessKey = "access_key";
		String secretKey = "secret_key";
		String region = "region";
		String storageEngineId = "strg_engine_id";
		String storagePath = "storage/path";
		String pageLength = "5";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineName);
		testProps.setProperty(IFunctionEngine.NAME_KEY, funtionName);
		testProps.setProperty(IFunctionEngine.DESCRIPTION_KEY, functionDescription);
		testProps.setProperty("ACCESS_KEY", accessKey);
		testProps.setProperty("SECRET_KEY", secretKey);
		testProps.setProperty("REGION", region);
		testProps.setProperty("S3BUCKETENGINEID", storageEngineId);
		testProps.setProperty("STORAGE_PATH", storagePath);
		testProps.setProperty("PAGE_LENGTH", pageLength);
		
		Gson gson = new Gson();
		String funcParamName = "func_param_name";
		String funcParamType = "func_param_type";
		String funcParamDesc = "func_param_desc";
		List<FunctionParameter> params = new Vector<>();
		FunctionParameter param = new FunctionParameter(funcParamName, funcParamType, funcParamDesc);
		params.add(param);
		testProps.setProperty(IFunctionEngine.PARAMETER_KEY, gson.toJson(params));
		
		List<String> requiredParams = new Vector<>();
		{
			for (int idx = 0; idx < 5; idx++) {
				requiredParams.add("required_param"+idx);
			}
		}
		testProps.setProperty(IFunctionEngine.REQUIRED_PARAMETER_KEY, gson.toJson(requiredParams));
		
		
		try(MockedStatic<AwsBasicCredentials> abc = Mockito.mockStatic(AwsBasicCredentials.class);
				MockedStatic<TextractClient> tc = Mockito.mockStatic(TextractClient.class);){
			AwsBasicCredentials abcMock = mock(AwsBasicCredentials.class);
			TextractClientBuilder clientBuilderMock = mock(TextractClientBuilder.class);
			TextractClient tcMock = mock(TextractClient.class);
			abc.when(()->AwsBasicCredentials.create(accessKey, secretKey)).thenReturn(abcMock);
			tc.when(()->TextractClient.builder()).thenReturn(clientBuilderMock);
			when(clientBuilderMock.region(any(Region.class))).thenReturn(clientBuilderMock);
			when(clientBuilderMock.credentialsProvider(any(StaticCredentialsProvider.class))).thenReturn(clientBuilderMock);
			when(clientBuilderMock.build()).thenReturn(tcMock);
			
			engine.setBasic(true); // skips folder initialization
			engine.open(testProps);
			Properties engineProps = engine.getSmssProp();
			for (Entry<Object, Object> testProp : testProps.entrySet()) {
				assertTrue(engineProps.containsKey(testProp.getKey()));
				assertTrue(engineProps.containsValue(testProp.getValue()));
			}
			List<FunctionParameter> engineParams = engine.getParameters();
			// test engine parameters
			assertEquals(params.size(), engineParams.size());
			for (FunctionParameter engineParam : engineParams) {
				assertEquals(funcParamName, engineParam.getParameterName());
				assertEquals(funcParamType, engineParam.getParameterType());
				assertEquals(funcParamDesc, engineParam.getParameterDescription());
			}
			// test required parameters
			List<String> engineReqParams = engine.getRequiredParameters();
			assertEquals(requiredParams.size(), engineReqParams.size());
			for (int reqIdx = 0; reqIdx < engineReqParams.size(); reqIdx++) {
				assertEquals(requiredParams.get(reqIdx), engineReqParams.get(reqIdx));
			}
		}
	}

	@Test
	void testOpenWithPropertiesNoAccessKey() throws Exception {
		Properties testProps = new Properties();
		String testEngine = "asdf-1234";
		String testEngineName = "engine_name";
		String funtionName = "function_name";
		String functionDescription = "function_description";
		String accessKey = "access_key";
		String secretKey = "secret_key";
		String region = "region";
		String storageEngineId = "strg_engine_id";
		String storagePath = "storage/path";
		String pageLength = "5";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineName);
		testProps.setProperty(IFunctionEngine.NAME_KEY, funtionName);
		testProps.setProperty(IFunctionEngine.DESCRIPTION_KEY, functionDescription);
//		testProps.setProperty("ACCESS_KEY", accessKey);
		testProps.setProperty("SECRET_KEY", secretKey);
		testProps.setProperty("REGION", region);
		testProps.setProperty("S3BUCKETENGINEID", storageEngineId);
		testProps.setProperty("STORAGE_PATH", storagePath);
		testProps.setProperty("PAGE_LENGTH", pageLength);

		Gson gson = new Gson();
		String funcParamName = "func_param_name";
		String funcParamType = "func_param_type";
		String funcParamDesc = "func_param_desc";
		List<FunctionParameter> params = new Vector<>();
		FunctionParameter param = new FunctionParameter(funcParamName, funcParamType, funcParamDesc);
		params.add(param);
		testProps.setProperty(IFunctionEngine.PARAMETER_KEY, gson.toJson(params));

		List<String> requiredParams = new Vector<>();
		{
			for (int idx = 0; idx < 5; idx++) {
				requiredParams.add("required_param" + idx);
			}
		}
		testProps.setProperty(IFunctionEngine.REQUIRED_PARAMETER_KEY, gson.toJson(requiredParams));

		try (MockedStatic<AwsBasicCredentials> abc = Mockito.mockStatic(AwsBasicCredentials.class);
				MockedStatic<TextractClient> tc = Mockito.mockStatic(TextractClient.class);) {
			AwsBasicCredentials abcMock = mock(AwsBasicCredentials.class);
			TextractClientBuilder clientBuilderMock = mock(TextractClientBuilder.class);
			TextractClient tcMock = mock(TextractClient.class);
			abc.when(() -> AwsBasicCredentials.create(accessKey, secretKey)).thenReturn(abcMock);
			tc.when(() -> TextractClient.builder()).thenReturn(clientBuilderMock);
			when(clientBuilderMock.region(any(Region.class))).thenReturn(clientBuilderMock);
			when(clientBuilderMock.credentialsProvider(any(StaticCredentialsProvider.class)))
					.thenReturn(clientBuilderMock);
			when(clientBuilderMock.build()).thenReturn(tcMock);

			engine.setBasic(true); // skips folder initialization
			RuntimeException e = assertThrows(RuntimeException.class, () -> engine.open(testProps));
			assertEquals("Must pass in an access key", e.getMessage());
		}
	}
	
	@Test
	void testOpenWithPropertiesNoSecretKey() throws Exception {
		Properties testProps = new Properties();
		String testEngine = "asdf-1234";
		String testEngineName = "engine_name";
		String funtionName = "function_name";
		String functionDescription = "function_description";
		String accessKey = "access_key";
		String secretKey = "secret_key";
		String region = "region";
		String storageEngineId = "strg_engine_id";
		String storagePath = "storage/path";
		String pageLength = "5";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineName);
		testProps.setProperty(IFunctionEngine.NAME_KEY, funtionName);
		testProps.setProperty(IFunctionEngine.DESCRIPTION_KEY, functionDescription);
		testProps.setProperty("ACCESS_KEY", accessKey);
//		testProps.setProperty("SECRET_KEY", secretKey);
		testProps.setProperty("REGION", region);
		testProps.setProperty("S3BUCKETENGINEID", storageEngineId);
		testProps.setProperty("STORAGE_PATH", storagePath);
		testProps.setProperty("PAGE_LENGTH", pageLength);

		Gson gson = new Gson();
		String funcParamName = "func_param_name";
		String funcParamType = "func_param_type";
		String funcParamDesc = "func_param_desc";
		List<FunctionParameter> params = new Vector<>();
		FunctionParameter param = new FunctionParameter(funcParamName, funcParamType, funcParamDesc);
		params.add(param);
		testProps.setProperty(IFunctionEngine.PARAMETER_KEY, gson.toJson(params));

		List<String> requiredParams = new Vector<>();
		{
			for (int idx = 0; idx < 5; idx++) {
				requiredParams.add("required_param" + idx);
			}
		}
		testProps.setProperty(IFunctionEngine.REQUIRED_PARAMETER_KEY, gson.toJson(requiredParams));

		try (MockedStatic<AwsBasicCredentials> abc = Mockito.mockStatic(AwsBasicCredentials.class);
				MockedStatic<TextractClient> tc = Mockito.mockStatic(TextractClient.class);) {
			AwsBasicCredentials abcMock = mock(AwsBasicCredentials.class);
			TextractClientBuilder clientBuilderMock = mock(TextractClientBuilder.class);
			TextractClient tcMock = mock(TextractClient.class);
			abc.when(() -> AwsBasicCredentials.create(accessKey, secretKey)).thenReturn(abcMock);
			tc.when(() -> TextractClient.builder()).thenReturn(clientBuilderMock);
			when(clientBuilderMock.region(any(Region.class))).thenReturn(clientBuilderMock);
			when(clientBuilderMock.credentialsProvider(any(StaticCredentialsProvider.class)))
					.thenReturn(clientBuilderMock);
			when(clientBuilderMock.build()).thenReturn(tcMock);

			engine.setBasic(true); // skips folder initialization
			RuntimeException e = assertThrows(RuntimeException.class, () -> engine.open(testProps));
			assertEquals("Must pass in a secret key", e.getMessage());
		}
	}
	
	@Test
	void testOpenWithPropertiesNoRegion() throws Exception {
		Properties testProps = new Properties();
		String testEngine = "asdf-1234";
		String testEngineName = "engine_name";
		String funtionName = "function_name";
		String functionDescription = "function_description";
		String accessKey = "access_key";
		String secretKey = "secret_key";
		String region = "region";
		String storageEngineId = "strg_engine_id";
		String storagePath = "storage/path";
		String pageLength = "5";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineName);
		testProps.setProperty(IFunctionEngine.NAME_KEY, funtionName);
		testProps.setProperty(IFunctionEngine.DESCRIPTION_KEY, functionDescription);
		testProps.setProperty("ACCESS_KEY", accessKey);
		testProps.setProperty("SECRET_KEY", secretKey);
//		testProps.setProperty("REGION", region);
		testProps.setProperty("S3BUCKETENGINEID", storageEngineId);
		testProps.setProperty("STORAGE_PATH", storagePath);
		testProps.setProperty("PAGE_LENGTH", pageLength);

		Gson gson = new Gson();
		String funcParamName = "func_param_name";
		String funcParamType = "func_param_type";
		String funcParamDesc = "func_param_desc";
		List<FunctionParameter> params = new Vector<>();
		FunctionParameter param = new FunctionParameter(funcParamName, funcParamType, funcParamDesc);
		params.add(param);
		testProps.setProperty(IFunctionEngine.PARAMETER_KEY, gson.toJson(params));

		List<String> requiredParams = new Vector<>();
		{
			for (int idx = 0; idx < 5; idx++) {
				requiredParams.add("required_param" + idx);
			}
		}
		testProps.setProperty(IFunctionEngine.REQUIRED_PARAMETER_KEY, gson.toJson(requiredParams));

		try (MockedStatic<AwsBasicCredentials> abc = Mockito.mockStatic(AwsBasicCredentials.class);
				MockedStatic<TextractClient> tc = Mockito.mockStatic(TextractClient.class);) {
			AwsBasicCredentials abcMock = mock(AwsBasicCredentials.class);
			TextractClientBuilder clientBuilderMock = mock(TextractClientBuilder.class);
			TextractClient tcMock = mock(TextractClient.class);
			abc.when(() -> AwsBasicCredentials.create(accessKey, secretKey)).thenReturn(abcMock);
			tc.when(() -> TextractClient.builder()).thenReturn(clientBuilderMock);
			when(clientBuilderMock.region(any(Region.class))).thenReturn(clientBuilderMock);
			when(clientBuilderMock.credentialsProvider(any(StaticCredentialsProvider.class)))
					.thenReturn(clientBuilderMock);
			when(clientBuilderMock.build()).thenReturn(tcMock);

			engine.setBasic(true); // skips folder initialization
			RuntimeException e = assertThrows(RuntimeException.class, () -> engine.open(testProps));
			assertEquals("Must define the requiredParameters", e.getMessage());
		}
	}
	
	@Test
	void testOpenWithPropertiesNoBucketPath() throws Exception {
		Properties testProps = new Properties();
		String testEngine = "asdf-1234";
		String testEngineName = "engine_name";
		String funtionName = "function_name";
		String functionDescription = "function_description";
		String accessKey = "access_key";
		String secretKey = "secret_key";
		String region = "region";
		String storageEngineId = "strg_engine_id";
		String storagePath = "storage/path";
		String pageLength = "5";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineName);
		testProps.setProperty(IFunctionEngine.NAME_KEY, funtionName);
		testProps.setProperty(IFunctionEngine.DESCRIPTION_KEY, functionDescription);
		testProps.setProperty("ACCESS_KEY", accessKey);
		testProps.setProperty("SECRET_KEY", secretKey);
		testProps.setProperty("REGION", region);
//		testProps.setProperty("S3BUCKETENGINEID", storageEngineId);
		testProps.setProperty("STORAGE_PATH", storagePath);
		testProps.setProperty("PAGE_LENGTH", pageLength);

		Gson gson = new Gson();
		String funcParamName = "func_param_name";
		String funcParamType = "func_param_type";
		String funcParamDesc = "func_param_desc";
		List<FunctionParameter> params = new Vector<>();
		FunctionParameter param = new FunctionParameter(funcParamName, funcParamType, funcParamDesc);
		params.add(param);
		testProps.setProperty(IFunctionEngine.PARAMETER_KEY, gson.toJson(params));

		List<String> requiredParams = new Vector<>();
		{
			for (int idx = 0; idx < 5; idx++) {
				requiredParams.add("required_param" + idx);
			}
		}
		testProps.setProperty(IFunctionEngine.REQUIRED_PARAMETER_KEY, gson.toJson(requiredParams));

		try (MockedStatic<AwsBasicCredentials> abc = Mockito.mockStatic(AwsBasicCredentials.class);
				MockedStatic<TextractClient> tc = Mockito.mockStatic(TextractClient.class);) {
			AwsBasicCredentials abcMock = mock(AwsBasicCredentials.class);
			TextractClientBuilder clientBuilderMock = mock(TextractClientBuilder.class);
			TextractClient tcMock = mock(TextractClient.class);
			abc.when(() -> AwsBasicCredentials.create(accessKey, secretKey)).thenReturn(abcMock);
			tc.when(() -> TextractClient.builder()).thenReturn(clientBuilderMock);
			when(clientBuilderMock.region(any(Region.class))).thenReturn(clientBuilderMock);
			when(clientBuilderMock.credentialsProvider(any(StaticCredentialsProvider.class)))
					.thenReturn(clientBuilderMock);
			when(clientBuilderMock.build()).thenReturn(tcMock);

			engine.setBasic(true); // skips folder initialization
			RuntimeException e = assertThrows(RuntimeException.class, () -> engine.open(testProps));
			assertEquals("Must pass in a Storage Engine Id for an S3 Bucket", e.getMessage());
		}
	}
	
	@Test
	void testCanProcessDocumentNonPDF(@TempDir Path tempDir) throws IOException {
		String mainDir = tempDir.toString();
		Path mainDirPath = Paths.get(mainDir);
		String fileName = "test.csv";
		Path filePath = mainDirPath.resolve(fileName);
		Files.createFile(filePath);
		
		assertFalse(engine.canProcessDocument(new File(filePath.toString())));
	}
	
	@Test
	void testCanProcessDocument(@TempDir Path tempDir) throws IOException {
		String mainDir = tempDir.toString();
		Path mainDirPath = Paths.get(mainDir);
		String fileName = "HelloWorld.pdf";
		Path filePath = mainDirPath.resolve(fileName);
		
		Document document = new Document();
		PdfWriter.getInstance(document, new FileOutputStream(filePath.toString()));
		document.open();
		document.add(new Paragraph("Hello World -- Page 1"));
		document.add(new Paragraph("This is my first PDF."));

		document.newPage();

		document.add(new Paragraph("Hello World -- Page 2"));
		document.close();
		
		assertFalse(engine.canProcessDocument(new File(filePath.toString())));
	}
	
	@Test
	void testGetCatalogSubType() {
		assertEquals(FunctionTypeEnum.AWS_TEXTRACT_CUSTOM_EMBEDDINGS.name(), engine.getCatalogSubType(null));
	}
}
