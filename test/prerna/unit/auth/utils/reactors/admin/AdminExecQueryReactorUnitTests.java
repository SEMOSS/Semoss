package prerna.unit.auth.utils.reactors.admin;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.auth.utils.reactors.admin.AdminExecQueryReactor;
import prerna.engine.api.IDatabaseEngine;
import prerna.om.Insight;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.delete.DeleteSqlInterpreter;
import prerna.query.querystruct.update.UpdateQueryStruct;
import prerna.query.querystruct.update.UpdateSqlInterpreter;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.stream.Stream;

import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;



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
		engine = mock(IDatabaseEngine.class);
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

	static Stream<Arguments> notRdbmsOrRdfDb() {
		return Stream.of(
			// Examples of invalids
			arguments(QUERY_STRUCT_TYPE.ENGINE, IDatabaseEngine.ACTION_TYPE.ADD_STATEMENT),
			arguments(QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY, IDatabaseEngine.DATABASE_TYPE.TINKER)
		);
	}
	
	static Stream<Arguments> RdbmsOrRdfDb() {
		return Stream.of(
			arguments(QUERY_STRUCT_TYPE.ENGINE, IDatabaseEngine.DATABASE_TYPE.RDBMS),
			arguments(QUERY_STRUCT_TYPE.ENGINE, IDatabaseEngine.DATABASE_TYPE.SESAME),
			arguments(QUERY_STRUCT_TYPE.ENGINE, IDatabaseEngine.DATABASE_TYPE.JENA),
			arguments(QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY, IDatabaseEngine.DATABASE_TYPE.RDBMS),
			arguments(QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY, IDatabaseEngine.DATABASE_TYPE.SESAME),
			arguments(QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY, IDatabaseEngine.DATABASE_TYPE.JENA),
		);
	}

	@ParameterizedTest
	@MethodSource("notRdbmsOrRdfDb")
	void testDatabaseIsRDBMSOrRDF(QUERY_STRUCT_TYPE qsType, IDatabaseEngine.DATABASE_TYPE dbType) {
		NounStore ns = mock(NounStore.class);
		reactor.setNounStore(ns);
		GenRowStruct grs = mock(GenRowStruct.class);
		when(ns.getNoun(PixelDataType.QUERY_STRUCT.getKey())).thenReturn(grs);
		
		AbstractQueryStruct qs = mock(AbstractQueryStruct.class);
		NounMetadata nm = new NounMetadata(qs, PixelDataType.QUERY_STRUCT);
		when(grs.getNoun(0)).thenReturn(nm);

		when(qs.getQsType()).thenReturn(qsType);
		when(qs.retrieveQueryStructEngine()).thenReturn(engine);
		when(engine.getDatabaseType()).thenReturn(dbType);
		
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = new SecurityAdminUtils();
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			if (!(dbType == IDatabaseEngine.DATABASE_TYPE.RDBMS || dbType == IDatabaseEngine.DATABASE_TYPE.SESAME || dbType == IDatabaseEngine.DATABASE_TYPE.JENA)) {
				IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
				assertEquals("Query update/deletes only works for rdbms/rdf databases", e.getMessage());
			}
		}
	}

	@ParameterizedTest
	@MethodSource("RdbmsOrRdfDb")
	void testHardSelectQueryStruct(QUERY_STRUCT_TYPE qsType, IDatabaseEngine.DATABASE_TYPE dbType) {
		NounStore ns = mock(NounStore.class);
		reactor.setNounStore(ns);
		GenRowStruct grs = mock(GenRowStruct.class);
		when(ns.getNoun(PixelDataType.QUERY_STRUCT.getKey())).thenReturn(grs);

		HardSelectQueryStruct qs = mock(HardSelectQueryStruct.class);
		NounMetadata nm = new NounMetadata(qs, PixelDataType.QUERY_STRUCT);
		when(grs.getNoun(0)).thenReturn(nm);

		when(qs.getQsType()).thenReturn(qsType);
		when(qs.retrieveQueryStructEngine()).thenReturn(engine);
		when(engine.getDatabaseType()).thenReturn(dbType);
		when(qs.getQuery()).thenReturn("SELECT * FROM table");
		
		// Simulate an exception being thrown by the insertData method
	    doThrow(new RuntimeException("Database error")).when(engine).insertData(anyString());

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = new SecurityAdminUtils();
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			SemossPixelException e = assertThrows(SemossPixelException.class, reactor::execute);
	        assertEquals("An error occurred trying to execute the query in the database: Database error", e.getMessage());
		}
	}

	@ParameterizedTest
	@MethodSource("RdbmsOrRdfDb")
	void testUpdateQueryStruct(QUERY_STRUCT_TYPE qsType, IDatabaseEngine.DATABASE_TYPE dbType) {
		NounStore ns = mock(NounStore.class);
		reactor.setNounStore(ns);
		GenRowStruct grs = mock(GenRowStruct.class);
		when(ns.getNoun(PixelDataType.QUERY_STRUCT.getKey())).thenReturn(grs);

		UpdateQueryStruct qs = mock(UpdateQueryStruct.class);
		NounMetadata nm = new NounMetadata(qs, PixelDataType.QUERY_STRUCT);
		when(grs.getNoun(0)).thenReturn(nm);

		when(qs.getQsType()).thenReturn(qsType);
		when(qs.retrieveQueryStructEngine()).thenReturn(engine);
		when(engine.getDatabaseType()).thenReturn(dbType);
		
		UpdateSqlInterpreter interp = mock(UpdateSqlInterpreter.class);
		when(interp.composeQuery()).thenReturn("UPDATE table SET column = value");

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = new SecurityAdminUtils();
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			reactor.execute();
			
			ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
			verify(engine, times(1)).insertData(captor.capture());
			
			String capturedArgument = captor.getValue();
			assertEquals("UPDATE table SET column = value", capturedArgument);
		}
	}

	@ParameterizedTest
	@MethodSource("RdbmsOrRdfDb")
	void testSelectQueryStruct(QUERY_STRUCT_TYPE qsType, IDatabaseEngine.DATABASE_TYPE dbType) {
		NounStore ns = mock(NounStore.class);
		reactor.setNounStore(ns);
		GenRowStruct grs = mock(GenRowStruct.class);
		when(ns.getNoun(PixelDataType.QUERY_STRUCT.getKey())).thenReturn(grs);

		SelectQueryStruct qs = mock(SelectQueryStruct.class);
		NounMetadata nm = new NounMetadata(qs, qsType);
		when(grs.getNoun(0)).thenReturn(nm);

		when(qs.getQsType()).thenReturn(QUERY_STRUCT_TYPE.ENGINE);
		when(qs.retrieveQueryStructEngine()).thenReturn(engine);
		when(engine.getDatabaseType()).thenReturn(IDatabaseEngine.DATABASE_TYPE.RDBMS);
		
		DeleteSqlInterpreter interp = mock(dbType);
		when(interp.composeQuery()).thenReturn("DELETE FROM table WHERE condition");

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = new SecurityAdminUtils();
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			reactor.execute();
			
			ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
			verify(engine, times(1)).insertData(captor.capture());
			
			String capturedArgument = captor.getValue();
			assertEquals("DELETE FROM table WHERE condition", capturedArgument);
		}
	}

	@ParameterizedTest
	@MethodSource("RdbmsOrRdfDb")
	void testQueryExecutionException() {
		NounStore ns = mock(NounStore.class);
		reactor.setNounStore(ns);
		GenRowStruct grs = mock(GenRowStruct.class);
		when(ns.getNoun(PixelDataType.QUERY_STRUCT.getKey())).thenReturn(grs);

		HardSelectQueryStruct qs = mock(HardSelectQueryStruct.class);
		NounMetadata nm = new NounMetadata(qs, PixelDataType.QUERY_STRUCT);
		when(grs.getNoun(0)).thenReturn(nm);

		when(qs.getQsType()).thenReturn(QUERY_STRUCT_TYPE.ENGINE);
		when(qs.retrieveQueryStructEngine()).thenReturn(engine);
		when(engine.getDatabaseType()).thenReturn(IDatabaseEngine.DATABASE_TYPE.RDBMS);
		when(qs.getQuery()).thenReturn("SELECT * FROM table");
		doThrow(new RuntimeException("Database error")).when(engine).insertData(anyString());

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = new SecurityAdminUtils();
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			SemossPixelException e = assertThrows(SemossPixelException.class, reactor::execute);
			assertEquals("An error occurred trying to execute the query in the database: Database error", e.getMessage());
		}
	}

	@ParameterizedTest
	@MethodSource("RdbmsOrRdfDb")
	void testValidAdminExecQuery(QUERY_STRUCT_TYPE qsType, IDatabaseEngine.DATABASE_TYPE dbType) {
		NounStore ns = mock(NounStore.class);
		reactor.setNounStore(ns);
		GenRowStruct grs = mock(GenRowStruct.class);
		when(ns.getNoun(PixelDataType.QUERY_STRUCT.getKey())).thenReturn(grs);
		
		AbstractQueryStruct qs = mock(AbstractQueryStruct.class);
		NounMetadata nm = new NounMetadata(qs, PixelDataType.QUERY_STRUCT);
		when(grs.getNoun(0)).thenReturn(nm);

		when(qs.getQsType()).thenReturn(qsType);
		when(qs.retrieveQueryStructEngine()).thenReturn(engine);
		when(engine.getDatabaseType()).thenReturn(dbType);
		
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = new SecurityAdminUtils();
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			if (dbType == IDatabaseEngine.DATABASE_TYPE.RDBMS || dbType == IDatabaseEngine.DATABASE_TYPE.SESAME || dbType == IDatabaseEngine.DATABASE_TYPE.JENA) {
				reactor.execute();
				
	            ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
	            verify(engine, times(1)).insertData(captor.capture());
	            
	            String capturedArgument = captor.getValue();
	            assertNotNull(capturedArgument); // Ensure the argument is not null
	            // Additional assertions can be added here to verify the content of the captured argument
	       
	            
			}
		}
	}
	
	
	
	
	
	
}
