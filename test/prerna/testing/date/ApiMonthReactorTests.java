package prerna.testing.date;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import prerna.date.reactor.MonthReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.ApiSemossTestUtils;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.date.SemossMonth;

public class ApiMonthReactorTests extends AbstractBaseSemossApiTests {

    @Test
    public void getMonth() {
        String pixel = ApiSemossTestUtils.buildPixelCall(MonthReactor.class, "months", "12");
        NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
        SemossMonth month = (SemossMonth) nm.getValue();
        assertEquals(12, month.getNumMonths());
    }

    @Test
    public void getMonth2() {
        // sanity check to make sure multiple tests run fine :)
        String pixel = ApiSemossTestUtils.buildPixelCall(MonthReactor.class, "months", "6");
        NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
        SemossMonth month = (SemossMonth) nm.getValue();
        assertEquals(6, month.getNumMonths());
    }
}
