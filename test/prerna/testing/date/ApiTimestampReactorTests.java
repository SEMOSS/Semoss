package prerna.testing.date;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import prerna.date.reactor.TimestampReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.date.SemossDate;
import prerna.testing.ApiSemossTestUtils;
import prerna.testing.AbstractBaseSemossApiTests;

public class ApiTimestampReactorTests extends AbstractBaseSemossApiTests {

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
    public void getCustomFormattedTimestamp() {
        String pixel = ApiSemossTestUtils.buildPixelCall(TimestampReactor.class, "date", "2022-03-19", "format", "MM/dd/yyyy");
        NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
        SemossDate date = (SemossDate) nm.getValue();
        String expectedPattern = "MM/dd/yyyy";
        assertEquals(expectedPattern, date.getPattern());
    }

    @Test
    public void getTimestampWithDateOnly() {
        String pixel = ApiSemossTestUtils.buildPixelCall(TimestampReactor.class, "date", "2022-03-19");
        NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
        SemossDate date = (SemossDate) nm.getValue();
        String expectedDate = "2022-03-19";
        assertEquals(expectedDate, date.getFormattedDate());
    }

    @Test
    public void getTimestampWithDateAndFormat() {
        String pixel = ApiSemossTestUtils.buildPixelCall(TimestampReactor.class, "date", "2022/03/19", "format", "yyyy/MM/dd");
        NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
        SemossDate date = (SemossDate) nm.getValue();
        String expectedDate = "2022/03/19";
        assertEquals(expectedDate, date.getFormattedDate());
    }
}
