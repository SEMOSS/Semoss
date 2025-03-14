package prerna.unit.auth.utils.reactors.admin;

import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Map;
import java.util.ArrayList;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.auth.utils.reactors.admin.AdminGetAllEngineUsageReactor;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.om.Insight;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AdminGetAllEngineUsageReactorUnitTests {
	private User user;
	private Insight insight;
	private AdminGetAllEngineUsageReactor reactor;
	private Map<String, String> keyValues;

	@BeforeEach
    void setUp() {
        reactor = new AdminGetAllEngineUsageReactor();
		keyValues = reactor.keyValue;
		insight = mock(Insight.class);
        user = mock(User.class);
		
        reactor.setInsight(insight);

        when(insight.getUser()).thenReturn(user);
    }

	@Test
	public void notAdmin() {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(null);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("User must be an admin to perform this function", e.getMessage());
		}
	}

	@Test
	public void noEngine() {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(new SecurityAdminUtils());

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Must input an engine id", e.getMessage());
		}
	}

	@Test
	public void test() {
		keyValues.put(ReactorKeysEnum.ENGINE.getKey(), "engine");
		keyValues.put(ReactorKeysEnum.LIMIT.getKey(), "limit");
		keyValues.put(ReactorKeysEnum.OFFSET.getKey(), "offset");

        try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
            MockedStatic<SecurityQueryUtils> squ = Mockito.mockStatic(SecurityQueryUtils.class);
            MockedStatic<ModelInferenceLogsUtils> modelInference = Mockito.mockStatic(ModelInferenceLogsUtils.class)) {
                sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(new SecurityAdminUtils());
                squ.when(() -> SecurityQueryUtils.testUserEngineIdForAlias(user, "engine")).thenReturn("engine");
                modelInference.when(() -> 
                        ModelInferenceLogsUtils.getOverAllEngineUsageFromModelInferenceLogs("engine", "limit", "offset", ReactorKeysEnum.START_DATE.getKey(), ReactorKeysEnum.END_DATE.getKey())
                    ).thenReturn(new ArrayList<Map<String, Object>>());

                NounMetadata nm = reactor.execute();

                assertNotNull(nm);
                assertEquals(PixelDataType.FORMATTED_DATA_SET, nm.getNounType());
                assertEquals(new ArrayList<Map<String, Object>>(), nm.getValue());
		}
	}
}
