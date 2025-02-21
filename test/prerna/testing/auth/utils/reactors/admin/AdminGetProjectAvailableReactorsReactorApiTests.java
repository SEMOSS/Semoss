package prerna.testing.auth.utils.reactors.admin;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.auth.utils.reactors.admin.AdminGetProjectAvailableReactorsReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestUtils;
import prerna.testing.utility.TestProjectUtils;

public class AdminGetProjectAvailableReactorsReactorApiTests extends AbstractBaseSemossApiTests {

	@Test
	public void execute() {
		
		String project = TestProjectUtils.createBasicProject("testProject");
		String reactor = "AdminGetProjectAvailableReactors"; //make sure to include reactor name without "reactor" at the end!!
		String pixel = ApiSemossTestUtils.buildPixelCall(reactor, ReactorKeysEnum.PROJECT.getKey(), project);
		
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		
		String projectReactors = nm.getValue().toString();
        assertNotNull(nm);
        assertEquals(PixelDataType.CONST_STRING, nm.getNounType());
        assertEquals(projectReactors, nm.getValue().toString());
        assertEquals(projectReactors, "[]");

        
	}
}
