package prerna.testing.query.querystruct.update.reactors;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import prerna.query.querystruct.update.UpdateQueryStruct;
import prerna.query.querystruct.update.reactors.UpdateReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestUtils;

public class UpdateReactorApiTests extends AbstractBaseSemossApiTests {

	@Test
    public void testCreateQueryStruct() {
        String pixel = ApiSemossTestUtils.buildPixelCall(UpdateReactor.class, "columns", "column1,column2", "values", "value1,value2");
        NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
        UpdateQueryStruct qs = (UpdateQueryStruct) nm.getValue();

        assertEquals("column1,column2", ( qs.getSelectors().get(0)).toString());

        assertEquals("value1,value2", qs.getValues().get(0), "The first update value should be value1");
    }
}
