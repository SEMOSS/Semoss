package prerna.testing.date.reactor;

import static org.junit.Assert.assertEquals;

import org.junit.jupiter.api.Test;

import prerna.date.SemossWeek;
import prerna.date.reactor.WeekReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestUtils;

public class WeekReactorUnitTest extends AbstractBaseSemossApiTests {
    @Test
    public void getWeek() {
        String pixel = ApiSemossTestUtils.buildPixelCall(WeekReactor.class, "weeks", "52");
        NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
        SemossWeek week = (SemossWeek) nm.getValue();

        assertEquals(52, week);
    }
}
