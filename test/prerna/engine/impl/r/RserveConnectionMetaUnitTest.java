package prerna.engine.impl.r;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class RserveConnectionMetaUnitTest {
	
	///////// Test equals

    @Test
    public void testEqualsSameHostPort() {
        RserveConnectionMeta a = new RserveConnectionMeta("127.0.0.1", 6311);
        RserveConnectionMeta b = new RserveConnectionMeta("127.0.0.1", 6311);

        assertTrue(a.equals(b));
        assertTrue(b.equals(a));
    }

    @Test
    public void testEqualsNullOrDifferentClass() {
        RserveConnectionMeta a = new RserveConnectionMeta("127.0.0.1", 6311);

        assertFalse(a.equals(null));
        assertFalse(a.equals("not a connection"));
    }

    @Test
    public void testEqualsDifferentHostOrPort() {
        RserveConnectionMeta a = new RserveConnectionMeta("127.0.0.1", 6311);
        RserveConnectionMeta differentHost = new RserveConnectionMeta("10.0.0.1", 6311);
        RserveConnectionMeta differentPort = new RserveConnectionMeta("127.0.0.1", 9999);

        assertFalse(a.equals(differentHost));
        assertFalse(a.equals(differentPort));
    }
}
