package prerna.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.User;
import prerna.reactor.AddVarReactor;
import prerna.om.Insight;
import prerna.om.Variable;
import prerna.project.api.IProject;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class AddVarReactorUnitTests {

	private AddVarReactor reactor;
	private Insight insight;
	private User user;
	private NounStore nounStore;
	private Map<String, String> keyValues;
	
	@BeforeEach
	void setup() {
		reactor = new AddVarReactor();
		insight = mock(Insight.class);
		user = mock(User.class);
		nounStore = mock(NounStore.class);
		 
		reactor.setInsight(insight);
		reactor.setNounStore(nounStore);
		when(insight.getUser()).thenReturn(user);
		keyValues = reactor.keyValue;
	}
	
	@Test
	void testExecute() {
		//keys (language and format aren't required)
		String variable = "Test";
		//List<Object> frames = Arrays.asList("FRAME610908"); //
		String frames = "FRAME1";
		String expression = "x + y"; //expression that needs to be dynamically calculated
		String language = "python"; //R Python or Java
		String format = "jpeg"; //format to save as jpeg gif or png
		
        // Arrange
//        when(nounStore.getNoun(ReactorKeysEnum.VARIABLE.getKey()).get(0)).thenReturn(variable);
//        when(nounStore.getNoun(ReactorKeysEnum.FRAME.getKey()).getAllValues()).thenReturn(Arrays.asList(frames));
//        when(nounStore.getNoun(ReactorKeysEnum.EXPRESSION.getKey()).get(0)).thenReturn(expression);
//        when(nounStore.getNoun(ReactorKeysEnum.LANGUAGE.getKey()).get(0)).thenReturn(language);
		when(nounStore.getNoun(ReactorKeysEnum.VARIABLE.getKey()).get(0)).thenReturn(variable);
		when(nounStore.getNoun(ReactorKeysEnum.FRAME.getKey()).get(0)).thenReturn(frames);
		when(nounStore.getNoun(ReactorKeysEnum.EXPRESSION.getKey()).get(0)).thenReturn(expression);
		when(nounStore.getNoun(ReactorKeysEnum.LANGUAGE.getKey()).get(0)).thenReturn(language);

		
		//so this isn't working as a way to store the keys ...
		//bc the reactor doesn't call for keyValues... DUUHH
//		keyValues.put(ReactorKeysEnum.VARIABLE.getKey(), "Test");
//		keyValues.put(ReactorKeysEnum.FRAME.getKey(), "FRAME1");
//		keyValues.put(ReactorKeysEnum.EXPRESSION.getKey(), "x + y");
//		keyValues.put(ReactorKeysEnum.LANGUAGE.getKey(), "python");
//		keyValues.put(ReactorKeysEnum.FORMAT.getKey(), "jpeg");
		
        // Act
        NounMetadata result = null;
        result = reactor.execute();

        // Assert
        assertNotNull(result);
        //assertEquals("varName", result.getName());
        assertEquals(PixelDataType.CONST_STRING, result.getNounType());
        assertEquals(PixelOperationType.ADD_VARIABLE, result.getNounType());
        
        assertTrue(result.getAdditionalReturn().contains(NounMetadata.getSuccessNounMessage("Variable Set : varName")));
		
		

//		when(insight.getRJavaTranslator(AddVarReactor.class.getCanonicalName())
//				.runRAndReturnOutput("tryCatch(" + expression + ", error=function(e) { 'error'})"))
//				.thenReturn("success");
//		
	
		
	}
	 @Test
	    void testExecuteWithErrorInExpression() {
	        // Prepare mock data
	        String variable = "Test";
	        List<Object> frames = Arrays.asList("Frame1", "Frame2");
	        String expression = "x + y";
	        String language = "python";
	        String format = "jpeg";

	        // Mock the NounStore behavior
	        mockNounStore(variable, frames, expression, language, format);

	        // Mock the R execution to return an error
	        when(insight.getRJavaTranslator(AddVarReactor.class.getCanonicalName()).runRAndReturnOutput("tryCatch(" + expression + ", error=function(e) { 'error'})"))
	                .thenReturn("error");

	        // Execute the reactor
	        NounMetadata result = reactor.execute();

	        // Verify the result
	        assertNotNull(result);
	        assertEquals("Expression has error, please correct " + expression, result);
	    }

	    @Test
	    void testExecuteWithoutOptionalKeys() {
	        // Prepare mock data
	        String variable = "Test";
	        List<Object> frames = Arrays.asList("Frame1", "Frame2");
	        String expression = "x + y";

	        // Mock the NounStore behavior
	        mockNounStore(variable, frames, expression, null, null);

	        // Execute the reactor
	        NounMetadata result = reactor.execute();

	        // Verify the result
	        assertNotNull(result);
	       // assertEquals(variable, result.getName());
	        assertEquals(PixelDataType.CONST_STRING, result.getNounType());
	       // assertEquals(PixelOperationType.ADD_VARIABLE, result.getOperationType());
	    }

	    private void mockNounStore(String variable, List<Object> frames, String expression, String language, String format) {
	        when(reactor.getNounStore().getNoun(ReactorKeysEnum.VARIABLE.getKey()).get(0)).thenReturn(variable);
	        when(reactor.getNounStore().getNoun(ReactorKeysEnum.FRAME.getKey()).getAllValues()).thenReturn(frames);
	        when(reactor.getNounStore().getNoun(ReactorKeysEnum.EXPRESSION.getKey()).get(0)).thenReturn(expression);
	        if (language != null) {
	            when(reactor.getNounStore().getNoun(ReactorKeysEnum.LANGUAGE.getKey()).get(0)).thenReturn(language);
	        }
	        if (format != null) {
	            when(reactor.getNounStore().getNoun(ReactorKeysEnum.FORMAT.getKey()).get(0)).thenReturn(format);
	        }

	    }
}