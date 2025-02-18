package prerna.testing.auth.utils.reactors.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.om.Insight;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.auth.utils.reactors.admin.AdminEngineInfoReactor;

public class AdminEngineInfoReactorUnitTests {

    private AdminEngineInfoReactor reactor;
    private Insight insight;
    private User user;

    @BeforeEach
    void setup(){
        reactor = new AdminEngineInfoReactor();
        insight = mock(Insight.class);
        user = mock(User.class);
        reactor.setInsight(insight);
        when(insight.getUser()).thenReturn(user);
    }

    @Test
    void testAdminUtilsNull(){
        try(MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)){
            sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(null);

            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
            assertEquals("User must be an admin to perform this function", e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void testEngineIdNull(){
        try(MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)){
            SecurityAdminUtils s = new SecurityAdminUtils();
            sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
            assertEquals("Must input an engine id", e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    void testValidEngineId(){
        try(MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)){
            SecurityAdminUtils s = new SecurityAdminUtils();
            sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
            
            

            when(reactor.(ReactorKeysEnum.ENGINE.getKey()) ).thenReturn("f36110b0-b1a6-47d7-82e2-62568cc17874");
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
            assertEquals("Could not find any engine data", e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
