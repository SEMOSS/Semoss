package prerna.date;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

public class SemossDateUnitTests {
    SemossDate reactor;
    ZoneId zoneId = ZoneId.systemDefault();

    @Test
    void constructors() {
        Date date = new Date();
        ZonedDateTime time = ZonedDateTime.now();

        reactor = new SemossDate(time);
        assertEquals(time.getYear(), reactor.getZonedDateTime().getYear());
        assertEquals(time.getMonth(), reactor.getZonedDateTime().getMonth());
        assertEquals(time.getDayOfMonth(), reactor.getZonedDateTime().getDayOfMonth());

        reactor = new SemossDate(time, "yyyy-MM-dd'T'HH:mm:ss");
        assertEquals("yyyy-MM-dd'T'HH:mm:ss", reactor.getPattern());
        assertEquals(time.getYear(), reactor.getZonedDateTime().getYear());
        assertEquals(time.getMonth(), reactor.getZonedDateTime().getMonth());
        assertEquals(time.getDayOfMonth(), reactor.getZonedDateTime().getDayOfMonth());

        reactor = new SemossDate(date, "yyyy-MM-dd", null);
        assertEquals("yyyy-MM-dd", reactor.getPattern());
        assertEquals(time.getYear(), reactor.getZonedDateTime().getYear());
        assertEquals(time.getMonth(), reactor.getZonedDateTime().getMonth());
        assertEquals(time.getDayOfMonth(), reactor.getZonedDateTime().getDayOfMonth());
    
        time = ZonedDateTime.now();
        reactor = new SemossDate(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")).toString(), "yyyy-MM-dd'T'HH:mm:ss", null);
        assertEquals("yyyy-MM-dd'T'HH:mm:ss", reactor.getPattern());
        assertEquals(time.getYear(), reactor.getZonedDateTime().getYear());
        assertEquals(time.getMonth(), reactor.getZonedDateTime().getMonth());
        assertEquals(time.getDayOfMonth(), reactor.getZonedDateTime().getDayOfMonth());

        Instant instant = Instant.parse("2025-01-01T00:00:00.00Z");
        time = ZonedDateTime.ofInstant(instant, zoneId);
        reactor = new SemossDate(instant, null);
        assertEquals("yyyy-MM-dd HH:mm:ss", reactor.getPattern());
        assertEquals(time.getYear(), reactor.getZonedDateTime().getYear());
        assertEquals(time.getMonth(), reactor.getZonedDateTime().getMonth());
        assertEquals(time.getDayOfMonth(), reactor.getZonedDateTime().getDayOfMonth());
        assertEquals(time.getHour(), reactor.getZonedDateTime().getHour());
        assertEquals(time.getMinute(), reactor.getZonedDateTime().getMinute());
        assertEquals(time.getSecond(), reactor.getZonedDateTime().getSecond());

        Timestamp stamp = new Timestamp(System.currentTimeMillis());
        time = ZonedDateTime.ofInstant(stamp.toLocalDateTime().toInstant(ZonedDateTime.now(zoneId).getOffset()), zoneId);
        reactor = new SemossDate(stamp, null, "yyyy-MM-dd HH:mm:ss");
        assertEquals("yyyy-MM-dd HH:mm:ss", reactor.getPattern());
        assertEquals(time.getYear(), reactor.getZonedDateTime().getYear());
        assertEquals(time.getMonth(), reactor.getZonedDateTime().getMonth());
        assertEquals(time.getDayOfMonth(), reactor.getZonedDateTime().getDayOfMonth());

        reactor = new SemossDate(System.currentTimeMillis(), zoneId);
        assertEquals("yyyy-MM-dd HH:mm:ss", reactor.getPattern());

        stamp = new Timestamp(System.currentTimeMillis());
        time = ZonedDateTime.ofInstant(stamp.toLocalDateTime().toInstant(ZonedDateTime.now(zoneId).getOffset()), zoneId);
        reactor = new SemossDate(stamp.getTime(), true, null);
        assertEquals("yyyy-MM-dd HH:mm:ss", reactor.getPattern());
        assertEquals(time.getYear(), reactor.getZonedDateTime().getYear());
        assertEquals(time.getMonth(), reactor.getZonedDateTime().getMonth());
        assertEquals(time.getDayOfMonth(), reactor.getZonedDateTime().getDayOfMonth());

        long milSec = System.currentTimeMillis();
        time = Instant.ofEpochMilli(milSec).atZone(zoneId);
        reactor = new SemossDate(System.currentTimeMillis(), false, null);
        assertEquals("yyyy-MM-dd", reactor.getPattern());
        assertEquals(time.getYear(), reactor.getZonedDateTime().getYear());
        assertEquals(time.getMonth(), reactor.getZonedDateTime().getMonth());
        assertEquals(time.getDayOfMonth(), reactor.getZonedDateTime().getDayOfMonth());

        reactor = new SemossDate(System.currentTimeMillis(), "yyyy-MM-dd", null);
        assertEquals("yyyy-MM-dd", reactor.getPattern());
        assertEquals(time.getYear(), reactor.getZonedDateTime().getYear());
        assertEquals(time.getMonth(), reactor.getZonedDateTime().getMonth());
        assertEquals(time.getDayOfMonth(), reactor.getZonedDateTime().getDayOfMonth());

        LocalDate localDate = LocalDate.now();
        time = localDate.atStartOfDay(zoneId);
        reactor = new SemossDate(localDate, null);
        assertEquals("yyyy-MM-dd", reactor.getPattern());
        assertEquals(time.getYear(), reactor.getZonedDateTime().getYear());
        assertEquals(time.getMonth(), reactor.getZonedDateTime().getMonth());
        assertEquals(time.getDayOfMonth(), reactor.getZonedDateTime().getDayOfMonth());

        LocalDateTime localDateTime = LocalDateTime.now();
        time = localDateTime.atZone(zoneId);
        reactor = new SemossDate(localDateTime, null);
        assertEquals("yyyy-MM-dd HH:mm:ss", reactor.getPattern());
        assertEquals(time.getYear(), reactor.getZonedDateTime().getYear());
        assertEquals(time.getMonth(), reactor.getZonedDateTime().getMonth());
        assertEquals(time.getDayOfMonth(), reactor.getZonedDateTime().getDayOfMonth());
    }

    @Test
    void getZonedDateTimeNullReturn() {
        reactor = new SemossDate("", null, null);
        assertNull(reactor.getZonedDateTime());

        reactor = new SemossDate(LocalDateTime.now().toString(), "yyyy-MM-dd'T'HH:mm:ss", null);
        assertNull(reactor.getZonedDateTime());
    }

    @Test
    void getFormattedDate() {
        ZonedDateTime time = ZonedDateTime.now();
        
        reactor = new SemossDate("", null, null);
        assertNull(reactor.getLocalDate());
        assertNull(reactor.getLocalDateTime());

        reactor = new SemossDate(time);
        assertEquals(time.toLocalDate(), reactor.getLocalDate());
        assertEquals(time.toLocalDateTime(), reactor.getLocalDateTime());
    }

    @Test
    void getFormatted() {
        ZonedDateTime time = ZonedDateTime.now();
        
        reactor = new SemossDate("", null, null);
        assertNull(reactor.getFormatted("yyyy-MM-dd"));

        reactor = new SemossDate(time);
        assertEquals(time.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")).toString(), reactor.getFormatted("yyyy-MM-dd"));
    }

    @Test
    void patternHasTime() {
        ZonedDateTime time = ZonedDateTime.now();        
        reactor = new SemossDate(time);
        assertTrue(reactor.patternHasTime());

        reactor = new SemossDate(time, "yyyy-MM-dd");
        assertFalse(reactor.patternHasTime());
    }

    @Test
    void dateHasTimeNotZero() {
        ZonedDateTime time = ZonedDateTime.now();
        reactor = new SemossDate("", null, null);
        assertFalse(reactor.dateHasTimeNotZero());
        
        reactor = new SemossDate(time);
        assertTrue(reactor.dateHasTimeNotZero());
    }

    @Test
    void toStringTest() {
        ZonedDateTime time = ZonedDateTime.now();
        reactor = new SemossDate(time);
        assertEquals(time.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)).toString(), reactor.toString());
    }

    @Test
    void testToString() {
        reactor = new SemossDate("2025-01-01", "yyyy-MM-dd", zoneId);
        assertEquals("2025-01-01 ::: yyyy-MM-dd", reactor.testToString());
    }

    @Test
    void genDateObj() {
        assertNull(SemossDate.genDateObj(null, zoneId));

        SemossDate date = SemossDate.genDateObj("01/01/2025", zoneId);
        assertNotNull(date);
        assertEquals("01/01/2025", date.toString());
        
        date = SemossDate.genDateObj("2025-01-01", zoneId);
        assertNotNull(date);
        assertEquals("2025-01-01", date.toString());
        
        date = SemossDate.genDateObj("abcd", zoneId);
        assertNull(date);
    }

    @Test
    void genTimeStampDateObj() {
        assertNull(SemossDate.genTimeStampDateObj(null, zoneId));
        
        SemossDate date = SemossDate.genTimeStampDateObj("12/31/2025 23:59:59", zoneId);
        assertNotNull(date);
        assertEquals("12/31/2025 23:59:59", date.toString());
        
        date = SemossDate.genTimeStampDateObj("2025-12-31 23:59:59", zoneId);
        assertNotNull(date);
        assertEquals("2025-12-31 23:59:59", date.toString());
        
        date = SemossDate.genTimeStampDateObj("abcd", zoneId);
        assertNull(date);
    }

    @Test
    void compareTo() {
        ZonedDateTime time = ZonedDateTime.now();
        
        SemossDate compareTo = new SemossDate("", null, null);
        reactor = new SemossDate("", null, null);
        assertEquals(-1, reactor.compareTo(compareTo));

        reactor = new SemossDate(time);
        assertEquals(1, reactor.compareTo(compareTo));

        ZonedDateTime time2 = ZonedDateTime.now();
        compareTo = new SemossDate(time2);
        assertEquals(time.compareTo(time2), reactor.compareTo(compareTo));
    }

    @Test
    void equals() {
        ZonedDateTime time = ZonedDateTime.now();
        reactor = new SemossDate("", null, null);
        
        assertFalse(reactor.equals(null));
        
        
        SemossDate compareTo = new SemossDate("", null, null);
        assertTrue(reactor.equals(compareTo));
        
        reactor = new SemossDate(time);
        assertFalse(reactor.equals(compareTo));

        compareTo = new SemossDate(time);
        assertTrue(reactor.equals(compareTo));

    }

    @Test
    void hashCodeTest() {
        ZonedDateTime time = ZonedDateTime.now();
        reactor = new SemossDate(time);
        assertNotNull(reactor.hashCode());
        assertInstanceOf(Integer.class, reactor.hashCode());
    }
}