package prerna.testing.query.querystruct.update;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import prerna.query.querystruct.update.UpdateQueryStruct;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestUtils;

import prerna.query.querystruct.update.reactors.UpdateReactor;

public class UpdateReactorApiTests extends AbstractBaseSemossApiTests {

	@Test
    public void testCreateQueryStruct() {
        String pixel = ApiSemossTestUtils.buildPixelCall(UpdateReactor.class, "columns", "column1,column2", "values", "value1,value2");
        System.out.println(pixel);
        NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
        UpdateQueryStruct qs = (UpdateQueryStruct) nm.getValue();

        assertEquals("column1,column2", ( qs.getSelectors().get(0)).toString());

        assertEquals("value1,value2", qs.getValues().get(0));
    }
}
