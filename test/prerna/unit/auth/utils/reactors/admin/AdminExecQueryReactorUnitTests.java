package prerna.unit.auth.utils.reactors.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.auth.utils.reactors.admin.AdminEngineInfoReactor;
import prerna.auth.utils.reactors.admin.AdminExecQueryReactor;
import prerna.engine.api.IDatabaseEngine;
import prerna.om.Insight;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.query.querystruct.HardSelectQueryStruct;

public class AdminExecQueryReactorUnitTests {
	private AdminExecQueryReactor reactor;
    private Insight insight;
    private User user;
    private SecurityAdminUtils adminUtils;
    private IDatabaseEngine engine;
    private AbstractQueryStruct queryStruct;

	@BeforeEach
	void setup() {
		reactor = new AdminExecQueryReactor();
		queryStruct = mock(AbstractQueryStruct.class);
		engine = mock();
		insight = mock(Insight.class);
		user = mock(User.class);
		reactor.setInsight(insight);
		when(insight.getUser()).thenReturn(user);
		
	}
	
//	@Test
//	void testQueryStructInstanceOfAbstractQueryStruct() {
//		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
//			
//			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
//			
//			qs.getQsType() == QUERY_STRUCT_TYPE.ENGINE || qs.getQsType() == QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY;
//
//			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
//			assertEquals("Input to admin exec query requires a query struct on an engine", e.getMessage());
//		}
//	}
	
	@Test
	void testQueryStructInstanceOfAbstractQueryStruct() {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = new SecurityAdminUtils();
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Input to exec query requires a query struct", e.getMessage());
		}
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
    void testQueryStructNull() {
    	User user = this.insight.getUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		
        reactor.setQueryStruct(null);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
        assertEquals("Input to exec query requires a query struct", e.getMessage());
    }

    @Test
    void testEngineNull() {
        reactor.setQueryStruct(new NounMetadata(queryStruct, PixelDataType.QUERY_STRUCT));
        when(queryStruct.getQsType()).thenReturn(AbstractQueryStruct.QUERY_STRUCT_TYPE.ENGINE);
        when(queryStruct.retrieveQueryStructEngine()).thenReturn(null);

        NullPointerException e = assertThrows(NullPointerException.class, reactor::execute);
        assertEquals("No engine passed in to execute the query", e.getMessage());
    }

}
