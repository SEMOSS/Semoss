package prerna.testing.date.reactor;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import prerna.date.reactor.TimestampReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.date.SemossDate;
import prerna.testing.ApiSemossTestUtils;
import prerna.testing.AbstractBaseSemossApiTests;

public class TimestampReactorApiTests extends AbstractBaseSemossApiTests {

    @Test
    public void getDefaultTimestamp() {
        String pixel = ApiSemossTestUtils.buildPixelCall(TimestampReactor.class, "date", null);
        NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
        SemossDate date = (SemossDate) nm.getValue();
        // Assuming the default format is "yyyy/MM/dd HH:mm:ss"
        String expectedPattern = "yyyy-MM-dd HH:mm:ss";
        assertEquals(expectedPattern, date.getPattern());
    }

    @Test
    public void getTimestampWithDateOnly() {
        String pixel = ApiSemossTestUtils.buildPixelCall(TimestampReactor.class, "date", "2022-03-19 01:20:12");
        NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
        SemossDate date = (SemossDate) nm.getValue();
        String expectedDate = "2022-03-19 01:20:12";
        assertEquals(expectedDate, date.getFormattedDate());
    }

    @Test
    public void getTimestampWithDateAndFormat() {
        String pixel = ApiSemossTestUtils.buildPixelCall(TimestampReactor.class, "date", "2022/03/19 01:20:12", "format", "yyyy/MM/dd HH:mm:ss");
        NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
        SemossDate date = (SemossDate) nm.getValue();
        String expectedDate = "2022/03/19 01:20:12";
        assertEquals(expectedDate, date.getFormattedDate());
    }
}
