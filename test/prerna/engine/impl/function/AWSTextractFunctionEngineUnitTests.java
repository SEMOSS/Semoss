package prerna.engine.impl.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertLinesMatch;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Vector;

import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.amazonaws.services.textract.AmazonTextract;
import com.amazonaws.services.textract.AmazonTextractClientBuilder;
import com.google.gson.Gson;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.om.Insight;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.EngineUtility;
import prerna.util.UploadUtilities;

public class AWSTextractFunctionEngineUnitTests {
	private Insight insight;
	private User user;
	private AWSTextractFunctionEngine engine;
	
	@BeforeEach
	void setUp() {
		user = mock(User.class);
		// using inner class to access abstract methods
		engine = new AWSTextractFunctionEngine();
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
		String bucketPath = "bucket_path";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineName);
		testProps.setProperty(IFunctionEngine.NAME_KEY, funtionName);
		testProps.setProperty(IFunctionEngine.DESCRIPTION_KEY, functionDescription);
		testProps.setProperty(AWSTextractFunctionEngine.ACCESS_KEY, accessKey);
		testProps.setProperty(AWSTextractFunctionEngine.SECRET_KEY, secretKey);
		testProps.setProperty(AWSTextractFunctionEngine.REGION, region);
		testProps.setProperty(AWSTextractFunctionEngine.BUCKETNAME, bucketPath);
		
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
		
		try(MockedStatic<AmazonTextractClientBuilder> atcb = Mockito.mockStatic(AmazonTextractClientBuilder.class);){
			AmazonTextract textractMock = mock(AmazonTextract.class);
			AmazonTextractClientBuilder clientBuilderMock = mock(AmazonTextractClientBuilder.class);
			atcb.when(()->AmazonTextractClientBuilder.standard()).thenReturn(clientBuilderMock);
			when(clientBuilderMock.withCredentials(any())).thenReturn(clientBuilderMock);
			when(clientBuilderMock.withRegion(region)).thenReturn(clientBuilderMock);
			when(clientBuilderMock.build()).thenReturn(textractMock);
			
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
	void testOpenWithPropertiesNoRequiredParams() throws Exception {
		Properties testProps = new Properties();
		String testEngine = "asdf-1234";
		String testEngineName = "engine_name";
		String funtionName = "function_name";
		String functionDescription = "function_description";
		String accessKey = "access_key";
		String secretKey = "secret_key";
		String region = "region";
		String bucketPath = "bucket_path";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineName);
		testProps.setProperty(IFunctionEngine.NAME_KEY, funtionName);
		testProps.setProperty(IFunctionEngine.DESCRIPTION_KEY, functionDescription);
		testProps.setProperty(AWSTextractFunctionEngine.ACCESS_KEY, accessKey);
		testProps.setProperty(AWSTextractFunctionEngine.SECRET_KEY, secretKey);
		testProps.setProperty(AWSTextractFunctionEngine.REGION, region);
		testProps.setProperty(AWSTextractFunctionEngine.BUCKETNAME, bucketPath);
		
		Gson gson = new Gson();
		String funcParamName = "func_param_name";
		String funcParamType = "func_param_type";
		String funcParamDesc = "func_param_desc";
		List<FunctionParameter> params = new Vector<>();
		FunctionParameter param = new FunctionParameter(funcParamName, funcParamType, funcParamDesc);
		params.add(param);
		testProps.setProperty(IFunctionEngine.PARAMETER_KEY, gson.toJson(params));
		
		try(MockedStatic<AmazonTextractClientBuilder> atcb = Mockito.mockStatic(AmazonTextractClientBuilder.class);){
			AmazonTextract textractMock = mock(AmazonTextract.class);
			AmazonTextractClientBuilder clientBuilderMock = mock(AmazonTextractClientBuilder.class);
			atcb.when(()->AmazonTextractClientBuilder.standard()).thenReturn(clientBuilderMock);
			when(clientBuilderMock.withCredentials(any())).thenReturn(clientBuilderMock);
			when(clientBuilderMock.withRegion(region)).thenReturn(clientBuilderMock);
			when(clientBuilderMock.build()).thenReturn(textractMock);
			
			RuntimeException e = assertThrows(
					RuntimeException.class,
					()->engine.open(testProps));
			assertEquals("Must define the requiredParameters", e.getMessage());
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
		String bucketPath = "bucket_path";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineName);
		testProps.setProperty(IFunctionEngine.NAME_KEY, funtionName);
		testProps.setProperty(IFunctionEngine.DESCRIPTION_KEY, functionDescription);
//		testProps.setProperty(AWSTextractFunctionEngine.ACCESS_KEY, accessKey);
		testProps.setProperty(AWSTextractFunctionEngine.SECRET_KEY, secretKey);
		testProps.setProperty(AWSTextractFunctionEngine.REGION, region);
		testProps.setProperty(AWSTextractFunctionEngine.BUCKETNAME, bucketPath);
		
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
		
		try(MockedStatic<AmazonTextractClientBuilder> atcb = Mockito.mockStatic(AmazonTextractClientBuilder.class);){
			AmazonTextract textractMock = mock(AmazonTextract.class);
			AmazonTextractClientBuilder clientBuilderMock = mock(AmazonTextractClientBuilder.class);
			atcb.when(()->AmazonTextractClientBuilder.standard()).thenReturn(clientBuilderMock);
			when(clientBuilderMock.withCredentials(any())).thenReturn(clientBuilderMock);
			when(clientBuilderMock.withRegion(region)).thenReturn(clientBuilderMock);
			when(clientBuilderMock.build()).thenReturn(textractMock);
			
			RuntimeException e = assertThrows(
					RuntimeException.class,
					()->engine.open(testProps));
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
		String bucketPath = "bucket_path";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineName);
		testProps.setProperty(IFunctionEngine.NAME_KEY, funtionName);
		testProps.setProperty(IFunctionEngine.DESCRIPTION_KEY, functionDescription);
		testProps.setProperty(AWSTextractFunctionEngine.ACCESS_KEY, accessKey);
//		testProps.setProperty(AWSTextractFunctionEngine.SECRET_KEY, secretKey);
		testProps.setProperty(AWSTextractFunctionEngine.REGION, region);
		testProps.setProperty(AWSTextractFunctionEngine.BUCKETNAME, bucketPath);
		
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
		
		try(MockedStatic<AmazonTextractClientBuilder> atcb = Mockito.mockStatic(AmazonTextractClientBuilder.class);){
			AmazonTextract textractMock = mock(AmazonTextract.class);
			AmazonTextractClientBuilder clientBuilderMock = mock(AmazonTextractClientBuilder.class);
			atcb.when(()->AmazonTextractClientBuilder.standard()).thenReturn(clientBuilderMock);
			when(clientBuilderMock.withCredentials(any())).thenReturn(clientBuilderMock);
			when(clientBuilderMock.withRegion(region)).thenReturn(clientBuilderMock);
			when(clientBuilderMock.build()).thenReturn(textractMock);
			
			RuntimeException e = assertThrows(
					RuntimeException.class,
					()->engine.open(testProps));
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
		String bucketPath = "bucket_path";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineName);
		testProps.setProperty(IFunctionEngine.NAME_KEY, funtionName);
		testProps.setProperty(IFunctionEngine.DESCRIPTION_KEY, functionDescription);
		testProps.setProperty(AWSTextractFunctionEngine.ACCESS_KEY, accessKey);
		testProps.setProperty(AWSTextractFunctionEngine.SECRET_KEY, secretKey);
//		testProps.setProperty(AWSTextractFunctionEngine.REGION, region);
		testProps.setProperty(AWSTextractFunctionEngine.BUCKETNAME, bucketPath);
		
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
		
		try(MockedStatic<AmazonTextractClientBuilder> atcb = Mockito.mockStatic(AmazonTextractClientBuilder.class);){
			AmazonTextract textractMock = mock(AmazonTextract.class);
			AmazonTextractClientBuilder clientBuilderMock = mock(AmazonTextractClientBuilder.class);
			atcb.when(()->AmazonTextractClientBuilder.standard()).thenReturn(clientBuilderMock);
			when(clientBuilderMock.withCredentials(any())).thenReturn(clientBuilderMock);
			when(clientBuilderMock.withRegion(region)).thenReturn(clientBuilderMock);
			when(clientBuilderMock.build()).thenReturn(textractMock);
			
			RuntimeException e = assertThrows(
					RuntimeException.class,
					()->engine.open(testProps));
			assertEquals("Must pass in a region", e.getMessage());
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
		String bucketPath = "bucket_path";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineName);
		testProps.setProperty(IFunctionEngine.NAME_KEY, funtionName);
		testProps.setProperty(IFunctionEngine.DESCRIPTION_KEY, functionDescription);
		testProps.setProperty(AWSTextractFunctionEngine.ACCESS_KEY, accessKey);
		testProps.setProperty(AWSTextractFunctionEngine.SECRET_KEY, secretKey);
		testProps.setProperty(AWSTextractFunctionEngine.REGION, region);
//		testProps.setProperty(AWSTextractFunctionEngine.BUCKETNAME, bucketPath);
		
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
		
		try(MockedStatic<AmazonTextractClientBuilder> atcb = Mockito.mockStatic(AmazonTextractClientBuilder.class);){
			AmazonTextract textractMock = mock(AmazonTextract.class);
			AmazonTextractClientBuilder clientBuilderMock = mock(AmazonTextractClientBuilder.class);
			atcb.when(()->AmazonTextractClientBuilder.standard()).thenReturn(clientBuilderMock);
			when(clientBuilderMock.withCredentials(any())).thenReturn(clientBuilderMock);
			when(clientBuilderMock.withRegion(region)).thenReturn(clientBuilderMock);
			when(clientBuilderMock.build()).thenReturn(textractMock);
			
			RuntimeException e = assertThrows(
					RuntimeException.class,
					()->engine.open(testProps));
			assertEquals("Must pass in a S3BucketPath", e.getMessage());
		}
	}
	
	@Test
	void testExecuteMissingParameters() throws Exception {
		Properties testProps = new Properties();
		String testEngine = "asdf-1234";
		String testEngineName = "engine_name";
		String funtionName = "function_name";
		String functionDescription = "function_description";
		String accessKey = "access_key";
		String secretKey = "secret_key";
		String region = "region";
		String bucketPath = "bucket_path";

		testProps.setProperty(Constants.ENGINE, testEngine);
		testProps.setProperty(Constants.ENGINE_ALIAS, testEngineName);
		testProps.setProperty(IFunctionEngine.NAME_KEY, funtionName);
		testProps.setProperty(IFunctionEngine.DESCRIPTION_KEY, functionDescription);
		testProps.setProperty(AWSTextractFunctionEngine.ACCESS_KEY, accessKey);
		testProps.setProperty(AWSTextractFunctionEngine.SECRET_KEY, secretKey);
		testProps.setProperty(AWSTextractFunctionEngine.REGION, region);
		testProps.setProperty(AWSTextractFunctionEngine.BUCKETNAME, bucketPath);
		
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
			for (int idx = 0; idx < 1; idx++) {
				requiredParams.add("required_param"+idx);
			}
		}
		testProps.setProperty(IFunctionEngine.REQUIRED_PARAMETER_KEY, gson.toJson(requiredParams));
		
		try(MockedStatic<AmazonTextractClientBuilder> atcb = Mockito.mockStatic(AmazonTextractClientBuilder.class);){
			AmazonTextract textractMock = mock(AmazonTextract.class);
			AmazonTextractClientBuilder clientBuilderMock = mock(AmazonTextractClientBuilder.class);
			atcb.when(()->AmazonTextractClientBuilder.standard()).thenReturn(clientBuilderMock);
			when(clientBuilderMock.withCredentials(any())).thenReturn(clientBuilderMock);
			when(clientBuilderMock.withRegion(region)).thenReturn(clientBuilderMock);
			when(clientBuilderMock.build()).thenReturn(textractMock);
			
			engine.open(testProps);
			Map<String, Object> parameterValues = new HashMap<>();
			IllegalArgumentException e = assertThrows(
					IllegalArgumentException.class,
					()->engine.execute(parameterValues));
			assertEquals("Must define required keys = " + requiredParams, e.getMessage());
		}
	}
	
	@Test
	void testGetCatalogSubType() {
		assertEquals("AWS", engine.getCatalogSubType(null));
	}
}
