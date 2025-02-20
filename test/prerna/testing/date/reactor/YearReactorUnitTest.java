package prerna.testing.date.reactor;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import prerna.date.SemossYear;
import prerna.date.reactor.YearReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestUtils;

public class YearReactorUnitTest extends AbstractBaseSemossApiTests {
    @Test
    public void getYear() {
        String pixel = ApiSemossTestUtils.buildPixelCall(YearReactor.class, "years", "2025");
        NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
        SemossYear year = (SemossYear) nm.getValue();

        assertEquals(2025, year);
    }
}
