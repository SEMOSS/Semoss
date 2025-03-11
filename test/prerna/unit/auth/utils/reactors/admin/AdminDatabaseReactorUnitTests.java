package prerna.unit.auth.utils.reactors.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.reactors.admin.AdminDatabaseReactor;
import prerna.om.Insight;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AdminDatabaseReactorUnitTests {

    private AdminDatabaseReactor reactor;
    private Insight insight;
    private User user;
    private SelectQueryStruct qs;
    private NounStore ns;

    @BeforeEach
    void setup() {
        reactor = new AdminDatabaseReactor();
        insight = mock(Insight.class);
        user = mock(User.class);
        qs = mock(SelectQueryStruct.class);
        ns = mock(NounStore.class);

        reactor.setNounStore(ns);
        reactor.setInsight(insight);

        when(insight.getUser()).thenReturn(user);
    }

    @Test
    void testAdminUtilsNull() {
        try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
            sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(null);

            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
            assertEquals("User must be an admin to perform this function", e.getMessage());
        }
    }

    @Test
    void testEngineIdEmpty() {
        reactor.keyValue.put(ReactorKeysEnum.DATABASE.getKey(), "");

        try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
            SecurityAdminUtils s = new SecurityAdminUtils();
            sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

            NullPointerException e = assertThrows(NullPointerException.class, reactor::execute);
            assertEquals("The engine id cannot be null for this operation", e.getMessage());
        }
    }

    @Test
	void testEngineIdNull() {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = new SecurityAdminUtils();
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			NullPointerException e = assertThrows(NullPointerException.class, reactor::execute);
			assertEquals("The engine id cannot be null for this operation", e.getMessage());
		}
	}

    @Test
    void testExecute() {
        reactor.keyValue.put(ReactorKeysEnum.DATABASE.getKey(), "testEngineId");

        try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
            SecurityAdminUtils s = new SecurityAdminUtils();
            sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

            when(qs.getQsType()).thenReturn(SelectQueryStruct.QUERY_STRUCT_TYPE.ENGINE);

            NounMetadata result = reactor.execute();

            assertNotNull(result);
        }
    }
    @Test
    void testRawEngineQuery() {
        reactor.keyValue.put(ReactorKeysEnum.DATABASE.getKey(), "testEngineId");

        try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
            SecurityAdminUtils s = new SecurityAdminUtils();
            sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
            
            qs.setQsType(SelectQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY);
            System.out.println(qs.getQsType());

            when(qs.getQsType()).thenReturn(SelectQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY);

            reactor.execute();

            verify(qs).setQsType(SelectQueryStruct.QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY);
        }
    }
}
