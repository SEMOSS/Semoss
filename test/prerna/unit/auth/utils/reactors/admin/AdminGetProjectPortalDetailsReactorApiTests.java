package prerna.unit.auth.utils.reactors.admin;

import prerna.testing.AbstractBaseSemossApiTests;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.auth.User;
import prerna.auth.utils.reactors.admin.AdminEngineInfoReactor;
import prerna.auth.utils.reactors.admin.AdminGetProjectPortalDetailsReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestEngineUtils;
import prerna.testing.ApiSemossTestUtils;
import prerna.testing.utility.TestEngineUtilities;

public class AdminGetProjectPortalDetailsReactorApiTests extends AbstractBaseSemossApiTests{
	
	
	@Test
	public void executeWithValidInput() {
       
//		User user = new User();
//        user.setRole("admin"); 
//        reactor.setInsight(new Insight(user)); 
		
		String project = "123";
	//String engine = ApiSemossTestEngineUtils.createBasicEngine();
		
		
		String pixel = ApiSemossTestUtils.buildPixelCall(AdminGetProjectPortalDetailsReactor.class, ReactorKeysEnum.PROJECT.getKey(), 
				project);
		
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		
		Map<String, Object> portalDetails = (Map<String, Object>) nm.getValue();
        assertNotNull(nm);
        assertEquals(PixelDataType.MAP, nm.getNounType());
        assertEquals(portalDetails, nm.getValue());
	}

}
