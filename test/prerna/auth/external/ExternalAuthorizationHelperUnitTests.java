package prerna.auth.external;

import org.junit.jupiter.api.Test;
import prerna.auth.User;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ExternalAuthorizationHelperUnitTests {

    @Test
    void testUpdateException() {
        User u = new User();
        Exception e = assertThrows(Exception.class,
                () -> ExternalAuthorizationHelper.updateEnginePermissionsBasedOnApiCall(u));
        assertEquals("Array index out of range: 0", e.getMessage());
    }
}
