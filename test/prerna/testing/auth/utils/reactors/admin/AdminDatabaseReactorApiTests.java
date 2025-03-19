package prerna.testing.auth.utils.reactors.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.reactors.admin.AdminDatabaseReactor;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestUtils;

public class AdminDatabaseReactorApiTests extends AbstractBaseSemossApiTests {

    @Test
    public void testCreateQueryStructWithAdminUser() {

        String pixel = ApiSemossTestUtils.buildPixelCall(AdminDatabaseReactor.class, "database", "engineId");
        NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
        AbstractQueryStruct qs = (AbstractQueryStruct) nm.getValue();

        assertNotNull(qs);
        assertEquals("engineId", qs.getEngineId(), "The engine ID should be engineId");
        assertEquals(SelectQueryStruct.QUERY_STRUCT_TYPE.ENGINE, qs.getQsType(), "The query struct type should be ENGINE");
    }

    @Test
    public void testCreateQueryStructWithNullEngineId() {

    	SemossPixelException exception = assertThrows(SemossPixelException.class, () -> {
            String pixel = ApiSemossTestUtils.buildPixelCall(AdminDatabaseReactor.class, "database", null);
            ApiSemossTestUtils.processPixel(pixel);
        });

        String expectedMessage = "The engine id cannot be null for this operation";
        String actualMessage = exception.getMessage();
        assertTrue(actualMessage.contains(expectedMessage));
    }

    @Test
    public void testCreateQueryStructWithEmptyEngineId() {

    	SemossPixelException exception = assertThrows(SemossPixelException.class, () -> {
            String pixel = ApiSemossTestUtils.buildPixelCall(AdminDatabaseReactor.class, "database", "");
            ApiSemossTestUtils.processPixel(pixel);
        });

        String expectedMessage = "The engine id cannot be null for this operation";
        String actualMessage = exception.getMessage();
        assertTrue(actualMessage.contains(expectedMessage));
    }
}
