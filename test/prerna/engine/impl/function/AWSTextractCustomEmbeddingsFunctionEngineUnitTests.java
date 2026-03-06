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

import prerna.SemossUnitTest;
import prerna.auth.User;
import prerna.engine.api.FunctionTypeEnum;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.impl.CaseInsensitiveProperties;
import prerna.engine.impl.storage.S3StorageEngine;
import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.Utility;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.textract.TextractClient;
import software.amazon.awssdk.services.textract.TextractClientBuilder;

public class AWSTextractCustomEmbeddingsFunctionEngineUnitTests extends SemossUnitTest {
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
		String objectPath = "object_path";

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
		testProps.setProperty("OBJECT_PATH", objectPath);

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
			MockedStatic<TextractClient> tc = Mockito.mockStatic(TextractClient.class);
			MockedStatic<Utility> util = Mockito.mockStatic(Utility.class)) {

			AwsBasicCredentials abcMock = mock(AwsBasicCredentials.class);
			TextractClientBuilder clientBuilderMock = mock(TextractClientBuilder.class);
			TextractClient tcMock = mock(TextractClient.class);
			abc.when(()->AwsBasicCredentials.create(accessKey, secretKey)).thenReturn(abcMock);
			tc.when(()->TextractClient.builder()).thenReturn(clientBuilderMock);
			when(clientBuilderMock.region(any(Region.class))).thenReturn(clientBuilderMock);
			when(clientBuilderMock.credentialsProvider(any(StaticCredentialsProvider.class))).thenReturn(clientBuilderMock);
			when(clientBuilderMock.build()).thenReturn(tcMock);

			S3StorageEngine s3StorageEngine = mock(S3StorageEngine.class);
			util.when(() -> Utility.getStorage(storageEngineId)).thenReturn(s3StorageEngine);
			when(s3StorageEngine.getStorageType()).thenCallRealMethod();
			CaseInsensitiveProperties storageProps = new CaseInsensitiveProperties();
			storageProps.put(S3StorageEngine.S3_BUCKET_KEY, "bucket");
			when(s3StorageEngine.getSmssProp()).thenReturn(storageProps);
			
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
			assertEquals("bucket", engine.bucketName);
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
			assertEquals("Must define the region", e.getMessage());
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
	void testCanProcessDocumentNonPDF() throws IOException {
		String mainDir = tempDir.toString();
		Path mainDirPath = Paths.get(mainDir);
		String fileName = "test.csv";
		Path filePath = mainDirPath.resolve(fileName);
		Files.createFile(filePath);
		
		assertFalse(engine.canProcessDocument(new File(filePath.toString())));
	}
	
	@Test
	void testCanProcessDocument() throws IOException {
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
