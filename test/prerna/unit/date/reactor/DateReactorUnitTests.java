package prerna.unit.date.reactor;
import static org.junit.Assert.assertEquals;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.Calendar;
import java.util.Map;
import java.text.SimpleDateFormat;

import prerna.date.reactor.DateReactor;
import prerna.date.SemossDate;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DateReactorUnitTests {

   /* private DateReactor reactor;
    private Map<String, String> keyValue;

    @BeforeEach
    public void setUp() {
        reactor = new DateReactor();
        keyValue = reactor.keyValue;
    }

    @Test
    public void getDate() {
        keyValue.put("date", "2022-03-19");
        NounMetadata nm = reactor.execute();
        assertEquals(PixelDataType.CONST_DATE, nm.getNounType());
        SemossDate date = (SemossDate) nm.getValue();
        assertEquals(2022-03-19, date.getDate());
    }

    @Test
    public void getDate2() {
        //sanity check to make sure multiple tests run fine :)
    	keyValue.put("date", "2022-03-19");
        NounMetadata nm = reactor.execute();
        assertEquals(PixelDataType.CONST_DATE, nm.getNounType());
        SemossDate date = (SemossDate) nm.getValue();
        assertEquals(2022-03-19, date.getDate());
    }

    @Test
    public void getDateWithCustomFormat() {
    	keyValue.put("date", "2022-03-19");
    	keyValue.put("format", "dd/MM/yyyy");
        NounMetadata nm = reactor.execute();
        assertEquals(PixelDataType.CONST_DATE, nm.getNounType());
        SemossDate date = (SemossDate) nm.getValue();
        assertEquals(2022-03-19, date.getDate());

        assertEquals("19/03/2022", date);
    }

    @Test
    public void getDateWithNoInput() {
        NounMetadata nm = reactor.execute();
        String date = nm.getValue().toString();
        String today = new SimpleDateFormat("yyyy-MM-dd").format(Calendar.getInstance().getTime());

        assertEquals(today, date);
    } */
}
