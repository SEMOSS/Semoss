package prerna.unit.auth.utils.reactors.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.reactors.admin.AdminExecQueryReactor;
import prerna.engine.api.IDatabaseEngine;
import prerna.om.Insight;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AdminExecQueryReactorUnitTests {
	private AdminExecQueryReactor reactor;
	private Insight insight;
	private User user;
	private SecurityAdminUtils adminUtils;
	private IDatabaseEngine engine;
	private AbstractQueryStruct queryStruct;
	
	private Map<String, String> keyValues;
	
	private String userId;

	@BeforeEach
	void setup() {
		reactor = new AdminExecQueryReactor();
		keyValues = reactor.keyValue;
		queryStruct = mock(AbstractQueryStruct.class);
		engine = mock();
		insight = mock(Insight.class);
		user = mock(User.class);
		reactor.setInsight(insight);
		when(insight.getUser()).thenReturn(user);
		
		
		List<AuthProvider> aps = new ArrayList<>();
		AuthProvider ap = AuthProvider.NATIVE;
		aps.add(ap);
		
		when(user.getLogins()).thenReturn(aps);
		
		userId = "userid";
		AccessToken at = new AccessToken();
		at.setId(userId);
		when(user.getAccessToken(ap)).thenReturn(at);
	}

	@Test
	void testAdminUtilsNull() {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(null);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("User must be an admin to perform this function", e.getMessage());
		}
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
	void testQueryStructNotInstanceOfAbstractQueryStruct() {
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = new SecurityAdminUtils();
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
			

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Input to exec query requires a query struct", e.getMessage());
		}
	}
	
	@Test
	void testQueryStructNotEngine() {
		NounStore ns = mock(NounStore.class);
		reactor.setNounStore(ns);
		GenRowStruct grs = mock(GenRowStruct.class);
		when(ns.getNoun(PixelDataType.QUERY_STRUCT.getKey())).thenReturn(grs);
		

		AbstractQueryStruct qs = mock(AbstractQueryStruct.class);
		NounMetadata nm = new NounMetadata(qs, PixelDataType.QUERY_STRUCT);
		when(grs.getNoun(0)).thenReturn(nm);
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = new SecurityAdminUtils();
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
			

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Input to admin exec query requires a query struct on an engine", e.getMessage());
		}
	}
	
	
	@ParameterizedTest
	@EnumSource(value = QUERY_STRUCT_TYPE.class, names = {"ENGINE", "RAW_ENGINE_QUERY"})
	void testQueryStructNoEngine(QUERY_STRUCT_TYPE qsType) {
		NounStore ns = mock(NounStore.class);
		reactor.setNounStore(ns);
		GenRowStruct grs = mock(GenRowStruct.class);
		when(ns.getNoun(PixelDataType.QUERY_STRUCT.getKey())).thenReturn(grs);
		

		AbstractQueryStruct qs = mock(AbstractQueryStruct.class);
		NounMetadata nm = new NounMetadata(qs, PixelDataType.QUERY_STRUCT);
		when(grs.getNoun(0)).thenReturn(nm);
		
		when(qs.getQsType()).thenReturn(qsType);
		
		when(qs.retrieveQueryStructEngine()).thenReturn(null);
		
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = new SecurityAdminUtils();
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
			

			NullPointerException e = assertThrows(NullPointerException.class, reactor::execute);
			assertEquals("No engine passed in to execute the query", e.getMessage());
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
