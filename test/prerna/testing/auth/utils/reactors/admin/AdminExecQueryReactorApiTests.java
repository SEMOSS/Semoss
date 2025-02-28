package prerna.testing.auth.utils.reactors.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import prerna.auth.utils.reactors.admin.AdminExecQueryReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestEngineUtils;
import prerna.testing.ApiSemossTestUtils;

import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;

import prerna.util.Constants;
import prerna.util.Utility;

public class AdminExecQueryReactorApiTests extends AbstractBaseSemossApiTests {
	@Test
	public void executeSelectQueryStructInput() {
		String engine = ApiSemossTestEngineUtils.createBasicEngine();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("INSIGHT__INSIGHTID"));
        
		String pixel = ApiSemossTestUtils.buildPixelCall(AdminExecQueryReactor.class, qs, engine);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
        List<PixelOperationType> opTypes = new ArrayList<>();
        opTypes.add(PixelOperationType.ALTER_DATABASE);
		
		assertNotNull(nm);
        assertEquals(PixelDataType.BOOLEAN, nm.getNounType());
		assertEquals(true, nm.getValue());
		assertEquals(PixelOperationType.ALTER_DATABASE, nm.getOpType());
	}

	@Test
    public void executeHardSelectQueryStructInput() {
        String engine = ApiSemossTestEngineUtils.createBasicEngine();
        HardSelectQueryStruct qs = new HardSelectQueryStruct();

        String query = "SELECT * FROM INSIGHT";
        qs.setQuery(query);
//        qs.setQsType(QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY);
//        qs.setEngine(Utility.getDatabase(Constants.LOCAL_MASTER_DB));
        
        String pixel = ApiSemossTestUtils.buildPixelCall(AdminExecQueryReactor.class, qs, engine);
        NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
        List<PixelOperationType> opTypes = new ArrayList<>();
        opTypes.add(PixelOperationType.ALTER_DATABASE);
        
        assertNotNull(nm);
        assertEquals(PixelDataType.BOOLEAN, nm.getNounType());
        assertEquals(true, nm.getValue());
		assertEquals(PixelOperationType.ALTER_DATABASE, nm.getOpType());
    }
}
