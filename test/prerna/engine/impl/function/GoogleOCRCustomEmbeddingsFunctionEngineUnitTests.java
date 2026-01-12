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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Vector;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.documentai.v1.DocumentProcessorServiceClient;
import com.google.cloud.documentai.v1.DocumentProcessorServiceSettings;
import com.google.gson.Gson;

import prerna.auth.User;
import prerna.engine.api.FunctionTypeEnum;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.impl.CaseInsensitiveProperties;
import prerna.engine.impl.storage.GoogleCloudStorageEngine;
import prerna.om.Insight;
import prerna.util.Utility;

public class GoogleOCRCustomEmbeddingsFunctionEngineUnitTests {
	private Insight insight;
	private User user;
	private GoogleOCRCustomEmbeddingsFunctionEngine engine;
	
	@BeforeEach
	void setUp() {
		user = mock(User.class);
		// using inner class to access abstract methods
		engine = new GoogleOCRCustomEmbeddingsFunctionEngine();
		insight = mock(Insight.class);
	}
	
	@Test
	void testOpenWithProperties() throws Exception {
		Properties testProps = new Properties();
		String projectId = "project_id";
		String processorId = "processor_id";
		String region = "region";
		String gogleStorageId = "google_storage_id";
		String storagePath = "storage_path";
		String serviceAccountFile = "service_account_file";
		String pageLength = "2";
		String objectPath = "object_path";
		
		testProps.setProperty("PROJECT_ID", projectId);
		testProps.setProperty("PROCESSOR_ID", processorId);
		testProps.setProperty("REGION", region);
		testProps.setProperty("GOOGLE_BUCKET_ENGINEID", gogleStorageId);
		testProps.setProperty("STORAGE_PATH", storagePath);
		testProps.setProperty("SERVICE_ACCOUNT_CREDENTIALS", serviceAccountFile);
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
		
		
		try(MockedStatic<GoogleCredentials> gc = Mockito.mockStatic(GoogleCredentials.class);
			MockedStatic<DocumentProcessorServiceSettings> dpss = Mockito.mockStatic(DocumentProcessorServiceSettings.class);
			MockedStatic<DocumentProcessorServiceClient> dpsc = Mockito.mockStatic(DocumentProcessorServiceClient.class);
			MockedStatic<Utility> util = Mockito.mockStatic(Utility.class);){
			
			GoogleCredentials credentialsMock = mock(GoogleCredentials.class);
			gc.when(()->GoogleCredentials.fromStream(any(ByteArrayInputStream.class))).thenReturn(credentialsMock);
			when(credentialsMock.createScoped(anyString(),anyString())).thenReturn(credentialsMock);
			
			DocumentProcessorServiceSettings dpssMock = mock(DocumentProcessorServiceSettings.class);
			DocumentProcessorServiceSettings.Builder builderMock = mock(DocumentProcessorServiceSettings.Builder.class);
			dpss.when(()->DocumentProcessorServiceSettings.newBuilder()).thenReturn(builderMock);
			when(builderMock.setCredentialsProvider(any())).thenReturn(builderMock);
			when(builderMock.setEndpoint(any())).thenReturn(builderMock);
			when(builderMock.build()).thenReturn(dpssMock);
			
			DocumentProcessorServiceClient dpscMock = mock(DocumentProcessorServiceClient.class);
			dpsc.when(()->DocumentProcessorServiceClient.create(dpssMock)).thenReturn(dpscMock);

			GoogleCloudStorageEngine cloudStorageEngineMock = mock(GoogleCloudStorageEngine.class);
			util.when(() -> Utility.getStorage(gogleStorageId)).thenReturn(cloudStorageEngineMock);
			CaseInsensitiveProperties gcpProps = new CaseInsensitiveProperties();
			gcpProps.put(GoogleCloudStorageEngine.GCS_BUCKET_KEY, "gcsBucket");
			when(cloudStorageEngineMock.getSmssProp()).thenReturn(gcpProps);
			when(cloudStorageEngineMock.getStorageType()).thenCallRealMethod();
			
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
			assertEquals("gcsBucket", engine.bucketName);
		}
	}
	
	@Test
	void testOpenWithPropertiesNoProjectId() throws Exception {
		Properties testProps = new Properties();
		String projectId = "project_id";
		String processorId = "processor_id";
		String region = "region";
		String gogleStorageId = "google_storage_id";
		String storagePath = "storage_path";
		String serviceAccountFile = "service_account_file";
		String pageLength = "2";
		
//		testProps.setProperty("PROJECT_ID", projectId);
		testProps.setProperty("PROCESSOR_ID", processorId);
		testProps.setProperty("REGION", region);
		testProps.setProperty("GOOGLE_BUCKET_ENGINEID", gogleStorageId);
		testProps.setProperty("STORAGE_PATH", storagePath);
		testProps.setProperty("SERVICE_ACCOUNT_CREDENTIALS", serviceAccountFile);
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
		
		
		try(MockedStatic<GoogleCredentials> gc = Mockito.mockStatic(GoogleCredentials.class);
				MockedStatic<DocumentProcessorServiceSettings> dpss = Mockito.mockStatic(DocumentProcessorServiceSettings.class);
				MockedStatic<DocumentProcessorServiceClient> dpsc = Mockito.mockStatic(DocumentProcessorServiceClient.class);){
			
			GoogleCredentials credentialsMock = mock(GoogleCredentials.class);
			gc.when(()->GoogleCredentials.fromStream(any(ByteArrayInputStream.class))).thenReturn(credentialsMock);
			when(credentialsMock.createScoped(anyString(),anyString())).thenReturn(credentialsMock);
			
			DocumentProcessorServiceSettings dpssMock = mock(DocumentProcessorServiceSettings.class);
			DocumentProcessorServiceSettings.Builder builderMock = mock(DocumentProcessorServiceSettings.Builder.class);
			dpss.when(()->DocumentProcessorServiceSettings.newBuilder()).thenReturn(builderMock);
			when(builderMock.setCredentialsProvider(any())).thenReturn(builderMock);
			when(builderMock.setEndpoint(any())).thenReturn(builderMock);
			when(builderMock.build()).thenReturn(dpssMock);
			
			DocumentProcessorServiceClient dpscMock = mock(DocumentProcessorServiceClient.class);
			dpsc.when(()->DocumentProcessorServiceClient.create(dpssMock)).thenReturn(dpscMock);
			
			engine.setBasic(true); // skips folder initialization
			RuntimeException e = assertThrows(RuntimeException.class, () -> engine.open(testProps));
			assertEquals("Must pass in a project Id", e.getMessage());
		}
	}
	
	@Test
	void testOpenWithPropertiesNoProcessorId() throws Exception {
		Properties testProps = new Properties();
		String projectId = "project_id";
		String processorId = "processor_id";
		String region = "region";
		String gogleStorageId = "google_storage_id";
		String storagePath = "storage_path";
		String serviceAccountFile = "service_account_file";
		String pageLength = "2";
		
		testProps.setProperty("PROJECT_ID", projectId);
//		testProps.setProperty("PROCESSOR_ID", processorId);
		testProps.setProperty("REGION", region);
		testProps.setProperty("GOOGLE_BUCKET_ENGINEID", gogleStorageId);
		testProps.setProperty("STORAGE_PATH", storagePath);
		testProps.setProperty("SERVICE_ACCOUNT_CREDENTIALS", serviceAccountFile);
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
		
		
		try(MockedStatic<GoogleCredentials> gc = Mockito.mockStatic(GoogleCredentials.class);
				MockedStatic<DocumentProcessorServiceSettings> dpss = Mockito.mockStatic(DocumentProcessorServiceSettings.class);
				MockedStatic<DocumentProcessorServiceClient> dpsc = Mockito.mockStatic(DocumentProcessorServiceClient.class);){
			
			GoogleCredentials credentialsMock = mock(GoogleCredentials.class);
			gc.when(()->GoogleCredentials.fromStream(any(ByteArrayInputStream.class))).thenReturn(credentialsMock);
			when(credentialsMock.createScoped(anyString(),anyString())).thenReturn(credentialsMock);
			
			DocumentProcessorServiceSettings dpssMock = mock(DocumentProcessorServiceSettings.class);
			DocumentProcessorServiceSettings.Builder builderMock = mock(DocumentProcessorServiceSettings.Builder.class);
			dpss.when(()->DocumentProcessorServiceSettings.newBuilder()).thenReturn(builderMock);
			when(builderMock.setCredentialsProvider(any())).thenReturn(builderMock);
			when(builderMock.setEndpoint(any())).thenReturn(builderMock);
			when(builderMock.build()).thenReturn(dpssMock);
			
			DocumentProcessorServiceClient dpscMock = mock(DocumentProcessorServiceClient.class);
			dpsc.when(()->DocumentProcessorServiceClient.create(dpssMock)).thenReturn(dpscMock);
			
			engine.setBasic(true); // skips folder initialization
			RuntimeException e = assertThrows(RuntimeException.class, () -> engine.open(testProps));
			assertEquals("Must pass in a processor Id", e.getMessage());
		}
	}
	
	@Test
	void testOpenWithPropertiesNoRegion() throws Exception {
		Properties testProps = new Properties();
		String projectId = "project_id";
		String processorId = "processor_id";
		String region = "region";
		String gogleStorageId = "google_storage_id";
		String storagePath = "storage_path";
		String serviceAccountFile = "service_account_file";
		String pageLength = "2";
		
		testProps.setProperty("PROJECT_ID", projectId);
		testProps.setProperty("PROCESSOR_ID", processorId);
//		testProps.setProperty("REGION", region);
		testProps.setProperty("GOOGLE_BUCKET_ENGINEID", gogleStorageId);
		testProps.setProperty("STORAGE_PATH", storagePath);
		testProps.setProperty("SERVICE_ACCOUNT_CREDENTIALS", serviceAccountFile);
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
		
		
		try(MockedStatic<GoogleCredentials> gc = Mockito.mockStatic(GoogleCredentials.class);
				MockedStatic<DocumentProcessorServiceSettings> dpss = Mockito.mockStatic(DocumentProcessorServiceSettings.class);
				MockedStatic<DocumentProcessorServiceClient> dpsc = Mockito.mockStatic(DocumentProcessorServiceClient.class);){
			
			GoogleCredentials credentialsMock = mock(GoogleCredentials.class);
			gc.when(()->GoogleCredentials.fromStream(any(ByteArrayInputStream.class))).thenReturn(credentialsMock);
			when(credentialsMock.createScoped(anyString(),anyString())).thenReturn(credentialsMock);
			
			DocumentProcessorServiceSettings dpssMock = mock(DocumentProcessorServiceSettings.class);
			DocumentProcessorServiceSettings.Builder builderMock = mock(DocumentProcessorServiceSettings.Builder.class);
			dpss.when(()->DocumentProcessorServiceSettings.newBuilder()).thenReturn(builderMock);
			when(builderMock.setCredentialsProvider(any())).thenReturn(builderMock);
			when(builderMock.setEndpoint(any())).thenReturn(builderMock);
			when(builderMock.build()).thenReturn(dpssMock);
			
			DocumentProcessorServiceClient dpscMock = mock(DocumentProcessorServiceClient.class);
			dpsc.when(()->DocumentProcessorServiceClient.create(dpssMock)).thenReturn(dpscMock);
			
			engine.setBasic(true); // skips folder initialization
			RuntimeException e = assertThrows(RuntimeException.class, () -> engine.open(testProps));
			assertEquals("Must pass in a region", e.getMessage());
		}
	}
	
	@Test
	void testOpenWithPropertiesNoStorageEngineId() throws Exception {
		Properties testProps = new Properties();
		String projectId = "project_id";
		String processorId = "processor_id";
		String region = "region";
		String gogleStorageId = "google_storage_id";
		String storagePath = "storage_path";
		String serviceAccountFile = "service_account_file";
		String pageLength = "2";
		
		testProps.setProperty("PROJECT_ID", projectId);
		testProps.setProperty("PROCESSOR_ID", processorId);
		testProps.setProperty("REGION", region);
//		testProps.setProperty("GOOGLE_BUCKET_ENGINEID", gogleStorageId);
		testProps.setProperty("STORAGE_PATH", storagePath);
		testProps.setProperty("SERVICE_ACCOUNT_CREDENTIALS", serviceAccountFile);
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
		
		
		try(MockedStatic<GoogleCredentials> gc = Mockito.mockStatic(GoogleCredentials.class);
				MockedStatic<DocumentProcessorServiceSettings> dpss = Mockito.mockStatic(DocumentProcessorServiceSettings.class);
				MockedStatic<DocumentProcessorServiceClient> dpsc = Mockito.mockStatic(DocumentProcessorServiceClient.class);){
			
			GoogleCredentials credentialsMock = mock(GoogleCredentials.class);
			gc.when(()->GoogleCredentials.fromStream(any(ByteArrayInputStream.class))).thenReturn(credentialsMock);
			when(credentialsMock.createScoped(anyString(),anyString())).thenReturn(credentialsMock);
			
			DocumentProcessorServiceSettings dpssMock = mock(DocumentProcessorServiceSettings.class);
			DocumentProcessorServiceSettings.Builder builderMock = mock(DocumentProcessorServiceSettings.Builder.class);
			dpss.when(()->DocumentProcessorServiceSettings.newBuilder()).thenReturn(builderMock);
			when(builderMock.setCredentialsProvider(any())).thenReturn(builderMock);
			when(builderMock.setEndpoint(any())).thenReturn(builderMock);
			when(builderMock.build()).thenReturn(dpssMock);
			
			DocumentProcessorServiceClient dpscMock = mock(DocumentProcessorServiceClient.class);
			dpsc.when(()->DocumentProcessorServiceClient.create(dpssMock)).thenReturn(dpscMock);
			
			engine.setBasic(true); // skips folder initialization
			RuntimeException e = assertThrows(RuntimeException.class, () -> engine.open(testProps));
			assertEquals("Must pass in a google bucket EngineId", e.getMessage());
		}
	}
	
	@Test
	void testOpenWithPropertiesNoServiceAccountFile() throws Exception {
		Properties testProps = new Properties();
		String projectId = "project_id";
		String processorId = "processor_id";
		String region = "region";
		String gogleStorageId = "google_storage_id";
		String storagePath = "storage_path";
		String serviceAccountFile = "service_account_file";
		String pageLength = "2";
		
		testProps.setProperty("PROJECT_ID", projectId);
		testProps.setProperty("PROCESSOR_ID", processorId);
		testProps.setProperty("REGION", region);
		testProps.setProperty("GOOGLE_BUCKET_ENGINEID", gogleStorageId);
		testProps.setProperty("STORAGE_PATH", storagePath);
//		testProps.setProperty("SERVICE_ACCOUNT_CREDENTIALS", serviceAccountFile);
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
		
		
		try(MockedStatic<GoogleCredentials> gc = Mockito.mockStatic(GoogleCredentials.class);
				MockedStatic<DocumentProcessorServiceSettings> dpss = Mockito.mockStatic(DocumentProcessorServiceSettings.class);
				MockedStatic<DocumentProcessorServiceClient> dpsc = Mockito.mockStatic(DocumentProcessorServiceClient.class);){
			
			GoogleCredentials credentialsMock = mock(GoogleCredentials.class);
			gc.when(()->GoogleCredentials.fromStream(any(ByteArrayInputStream.class))).thenReturn(credentialsMock);
			when(credentialsMock.createScoped(anyString(),anyString())).thenReturn(credentialsMock);
			
			DocumentProcessorServiceSettings dpssMock = mock(DocumentProcessorServiceSettings.class);
			DocumentProcessorServiceSettings.Builder builderMock = mock(DocumentProcessorServiceSettings.Builder.class);
			dpss.when(()->DocumentProcessorServiceSettings.newBuilder()).thenReturn(builderMock);
			when(builderMock.setCredentialsProvider(any())).thenReturn(builderMock);
			when(builderMock.setEndpoint(any())).thenReturn(builderMock);
			when(builderMock.build()).thenReturn(dpssMock);
			
			DocumentProcessorServiceClient dpscMock = mock(DocumentProcessorServiceClient.class);
			dpsc.when(()->DocumentProcessorServiceClient.create(dpssMock)).thenReturn(dpscMock);
			
			engine.setBasic(true); // skips folder initialization
			RuntimeException e = assertThrows(RuntimeException.class, () -> engine.open(testProps));
			assertEquals("Must pass in a Service Account File", e.getMessage());
		}
	}
	
	@Test
	void testOpenWithPropertiesNoStoragePath() throws Exception {
		Properties testProps = new Properties();
		String projectId = "project_id";
		String processorId = "processor_id";
		String region = "region";
		String gogleStorageId = "google_storage_id";
		String storagePath = "storage_path";
		String serviceAccountFile = "service_account_file";
		String pageLength = "2";
		
		testProps.setProperty("PROJECT_ID", projectId);
		testProps.setProperty("PROCESSOR_ID", processorId);
		testProps.setProperty("REGION", region);
		testProps.setProperty("GOOGLE_BUCKET_ENGINEID", gogleStorageId);
//		testProps.setProperty("STORAGE_PATH", storagePath);
		testProps.setProperty("SERVICE_ACCOUNT_CREDENTIALS", serviceAccountFile);
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
		
		
		try(MockedStatic<GoogleCredentials> gc = Mockito.mockStatic(GoogleCredentials.class);
				MockedStatic<DocumentProcessorServiceSettings> dpss = Mockito.mockStatic(DocumentProcessorServiceSettings.class);
				MockedStatic<DocumentProcessorServiceClient> dpsc = Mockito.mockStatic(DocumentProcessorServiceClient.class);){
			
			GoogleCredentials credentialsMock = mock(GoogleCredentials.class);
			gc.when(()->GoogleCredentials.fromStream(any(ByteArrayInputStream.class))).thenReturn(credentialsMock);
			when(credentialsMock.createScoped(anyString(),anyString())).thenReturn(credentialsMock);
			
			DocumentProcessorServiceSettings dpssMock = mock(DocumentProcessorServiceSettings.class);
			DocumentProcessorServiceSettings.Builder builderMock = mock(DocumentProcessorServiceSettings.Builder.class);
			dpss.when(()->DocumentProcessorServiceSettings.newBuilder()).thenReturn(builderMock);
			when(builderMock.setCredentialsProvider(any())).thenReturn(builderMock);
			when(builderMock.setEndpoint(any())).thenReturn(builderMock);
			when(builderMock.build()).thenReturn(dpssMock);
			
			DocumentProcessorServiceClient dpscMock = mock(DocumentProcessorServiceClient.class);
			dpsc.when(()->DocumentProcessorServiceClient.create(dpssMock)).thenReturn(dpscMock);
			
			engine.setBasic(true); // skips folder initialization
			RuntimeException e = assertThrows(RuntimeException.class, () -> engine.open(testProps));
			assertEquals("Must pass in a object path", e.getMessage());
		}
	}
	
	@Test
	void testGetCatalogSubType() {
		assertEquals(FunctionTypeEnum.GOOGLE_OCR_CUSTOM_EMBEDDINGS.name(), engine.getCatalogSubType(null));
	}
}
