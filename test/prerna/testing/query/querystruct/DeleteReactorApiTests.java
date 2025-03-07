package prerna.testing.query.querystruct;

import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.junit.Before;
import org.junit.Test;

import prerna.auth.utils.reactors.admin.AdminEngineInfoReactor;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.delete.DeleteReactor;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.ApiSemossTestEngineUtils;
import prerna.testing.ApiSemossTestUtils;
import prerna.testing.utility.TestEngineUtilities;

public class DeleteReactorApiTests {
    private DeleteReactor reactor;
    private NounStore nounStore;
    private SelectQueryStruct qs;
    private SelectQueryStruct exist_qs;
    private GenRowStruct mockGR;
    private IQuerySelector mockQueryS;

    @Before
    public void setUp(){
        reactor = new DeleteReactor();
        qs = new SelectQueryStruct();
        exist_qs = new SelectQueryStruct();

    }

    @Test
 public void testMergeExistingValues(){
    //String engine = ApiSemossTestEngineUtils.createBasicEngine();
    String engine = ApiSemossTestEngineUtils.createBasicEngine();	
	Map<String, Object> map = new HashMap<>();

    String pixel = ApiSemossTestUtils.buildPixelCall(DeleteReactor.class);
    NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
    TestEngineUtilities.setEngineMetadata(engine, map);

 }

 @Test
 public void testSetNounStore() {
    reactor.setNounStore(nounStore);
 }

 @Test
 public void testSetQs() {
    reactor.setQs(qs);
 }

 @Test
 public void testGetName() {
    String name = reactor.getName();
    assertEquals("Delete", name);
 }

}
