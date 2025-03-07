package prerna.testing;

import prerna.testing.AbstractBaseSemossApiTests;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.reactor.AddVarReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestEngineUtils;
import prerna.testing.ApiSemossTestUtils;
import prerna.testing.utility.TestEngineUtilities;

public class AddVarReactorApiTests extends AbstractBaseSemossApiTests{

	@Test
	public void testFullExecute() {
		//keys (language and format aren't required)
		String variable = "Test";
		String frame = "{ContractsPSCCombined1}"; //
		String expression = "x + y"; //expression that needs to be dynamically calculated
		String language = "r"; //R Python or Java
		String format = "jpeg"; //format to save as jpeg gif or png
		String pixel = ApiSemossTestUtils.buildPixelCall(AddVarReactor.class, 
				ReactorKeysEnum.VARIABLE.getKey(), variable,
				ReactorKeysEnum.FRAME.getKey(), frame,
				ReactorKeysEnum.EXPRESSION.getKey(), expression,
				ReactorKeysEnum.LANGUAGE.getKey(), language,
				ReactorKeysEnum.FORMAT.getKey(), format
				);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		
		assertNotNull(nm);
		assertEquals(PixelDataType.CONST_STRING, nm.getNounType());
	}
	
	public void testLangAndFormatNull() {
		
	}
}
