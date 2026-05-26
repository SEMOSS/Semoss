/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.auth.utils.reactors.admin;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.om.Insight;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.delete.DeleteSqlInterpreter;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.update.UpdateQueryStruct;
import prerna.query.querystruct.update.UpdateSqlInterpreter;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
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
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
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
		when(ns.getGenRowStruct(PixelDataType.QUERY_STRUCT.getKey())).thenReturn(grs);

		AbstractQueryStruct qs = mock(AbstractQueryStruct.class);
		NounMetadata nm = new NounMetadata(qs, PixelDataType.QUERY_STRUCT);
		when(grs.getNoun(0)).thenReturn(nm);
		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
			assertEquals("Input to admin exec query requires a query struct on an engine", e.getMessage());
		}
	}

	@ParameterizedTest
	@EnumSource(value = QUERY_STRUCT_TYPE.class, names = { "ENGINE", "RAW_ENGINE_QUERY" })
	void testQueryStructNoEngine(QUERY_STRUCT_TYPE qsType) {
		NounStore ns = mock(NounStore.class);
		reactor.setNounStore(ns);
		GenRowStruct grs = mock(GenRowStruct.class);
		when(ns.getGenRowStruct(PixelDataType.QUERY_STRUCT.getKey())).thenReturn(grs);

		AbstractQueryStruct qs = mock(AbstractQueryStruct.class);
		NounMetadata nm = new NounMetadata(qs, PixelDataType.QUERY_STRUCT);
		when(grs.getNoun(0)).thenReturn(nm);

		when(qs.getQsType()).thenReturn(qsType);

		when(qs.retrieveQueryStructEngine()).thenReturn(null);

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			NullPointerException e = assertThrows(NullPointerException.class, reactor::execute);
			assertEquals("No engine passed in to execute the query", e.getMessage());
		}
	}

	static Stream<Arguments> notRdbmsOrRdfDb() {
		return Stream.of(
				// Examples of invalids
				arguments(QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY, IDatabaseEngine.DATABASE_TYPE.TINKER));
	}

	static Stream<Arguments> RdbmsOrRdfDb() {
		return Stream.of(arguments(QUERY_STRUCT_TYPE.ENGINE, IDatabaseEngine.DATABASE_TYPE.RDBMS),
				arguments(QUERY_STRUCT_TYPE.ENGINE, IDatabaseEngine.DATABASE_TYPE.SESAME),
				arguments(QUERY_STRUCT_TYPE.ENGINE, IDatabaseEngine.DATABASE_TYPE.JENA),
				arguments(QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY, IDatabaseEngine.DATABASE_TYPE.RDBMS),
				arguments(QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY, IDatabaseEngine.DATABASE_TYPE.SESAME),
				arguments(QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY, IDatabaseEngine.DATABASE_TYPE.JENA));
	}

	@ParameterizedTest
	@MethodSource("notRdbmsOrRdfDb")
	void testDatabaseIsNotRDBMSOrRDF(QUERY_STRUCT_TYPE qsType, IDatabaseEngine.DATABASE_TYPE dbType) {
		NounStore ns = mock(NounStore.class);
		reactor.setNounStore(ns);
		GenRowStruct grs = mock(GenRowStruct.class);
		when(ns.getGenRowStruct(PixelDataType.QUERY_STRUCT.getKey())).thenReturn(grs);

		AbstractQueryStruct qs = mock(AbstractQueryStruct.class);
		NounMetadata nm = new NounMetadata(qs, PixelDataType.QUERY_STRUCT);
		when(grs.getNoun(0)).thenReturn(nm);

		when(qs.getQsType()).thenReturn(qsType);
		when(qs.retrieveQueryStructEngine()).thenReturn(engine);
		when(engine.getDatabaseType()).thenReturn(dbType);

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			if (!(dbType == IDatabaseEngine.DATABASE_TYPE.RDBMS || dbType == IDatabaseEngine.DATABASE_TYPE.SESAME
					|| dbType == IDatabaseEngine.DATABASE_TYPE.JENA)) {
				IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reactor::execute);
				assertEquals("Query update/deletes only works for rdbms/rdf databases", e.getMessage());
			}
		}
	}

//	@Test
//	void testNullInsertDataIntoEngine(QUERY_STRUCT_TYPE qsType, IDatabaseEngine.DATABASE_TYPE dbType) {
//		NounStore ns = mock(NounStore.class);
//		reactor.setNounStore(ns);
//		GenRowStruct grs = mock(GenRowStruct.class);
//		when(ns.getNoun(PixelDataType.QUERY_STRUCT.getKey())).thenReturn(grs);
//
//		HardSelectQueryStruct qs = mock(HardSelectQueryStruct.class);
//		NounMetadata nm = new NounMetadata(qs, PixelDataType.QUERY_STRUCT);
//		when(grs.getNoun(0)).thenReturn(nm);
//
//		when(qs.getQsType()).thenReturn(QUERY_STRUCT_TYPE.ENGINE);
//		when(qs.retrieveQueryStructEngine()).thenReturn(engine);
//		when(engine.getDatabaseType()).thenReturn(IDatabaseEngine.DATABASE_TYPE.RDBMS);
//		
//		// Change the  behavior of qs.getQuery() to a simple string
//		when(qs.getQuery()).thenReturn("SELECT * FROM table");
//		
////		insertData(<the string you returned from getQuery())
//		String query = null;
//		try {
//			doThrow(new SemossPixelException(query)).when(engine).insertData("SELECT * FROM table");
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
//		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
//			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
//			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);
//
//			SemossPixelException e = assertThrows(SemossPixelException.class, reactor::execute);
//	        assertEquals("An error occurred trying to execute the query in the database", e.getMessage());
//		}
//	}

	@Test
	void testEmptyInsertDataIntoEngine() {
		NounStore ns = mock(NounStore.class);
		reactor.setNounStore(ns);
		GenRowStruct grs = mock(GenRowStruct.class);
		when(ns.getGenRowStruct(PixelDataType.QUERY_STRUCT.getKey())).thenReturn(grs);

		HardSelectQueryStruct qs = mock(HardSelectQueryStruct.class);
		NounMetadata nm = new NounMetadata(qs, PixelDataType.QUERY_STRUCT);
		when(grs.getNoun(0)).thenReturn(nm);

		when(qs.getQsType()).thenReturn(QUERY_STRUCT_TYPE.ENGINE);
		when(qs.retrieveQueryStructEngine()).thenReturn(engine);
		when(engine.getDatabaseType()).thenReturn(IDatabaseEngine.DATABASE_TYPE.RDBMS);

		// Change the behavior of qs.getQuery() to a simple string
		when(qs.getQuery()).thenReturn("SELECT * FROM TABLE");

//		insertData(<the string you returned from getQuery())
//		when(engine.insertData("")).thenThrow(new SemossPixelException(""));
		try {
			doThrow(new SemossPixelException("")).when(engine).insertData("SELECT * FROM TABLE");
		} catch (Exception e) {
			// TODO Auto-generated catch block
		}

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			SemossPixelException e = assertThrows(SemossPixelException.class, reactor::execute);
			assertEquals("An error occurred trying to execute the query in the database", e.getMessage());
		}
	}

	@Test
	void testInvalidInsertDataIntoEngine() {
		NounStore ns = mock(NounStore.class);
		reactor.setNounStore(ns);
		GenRowStruct grs = mock(GenRowStruct.class);
		when(ns.getGenRowStruct(PixelDataType.QUERY_STRUCT.getKey())).thenReturn(grs);

		HardSelectQueryStruct qs = mock(HardSelectQueryStruct.class);
		NounMetadata nm = new NounMetadata(qs, PixelDataType.QUERY_STRUCT);
		when(grs.getNoun(0)).thenReturn(nm);

		when(qs.getQsType()).thenReturn(QUERY_STRUCT_TYPE.ENGINE);
		when(qs.retrieveQueryStructEngine()).thenReturn(engine);
		when(engine.getDatabaseType()).thenReturn(IDatabaseEngine.DATABASE_TYPE.RDBMS);

		// Change the behavior of qs.getQuery() to a simple string
		when(qs.getQuery()).thenReturn("SELECT * FROM table");

		try {
			doThrow(new SemossPixelException("Database error")).when(engine).insertData("SELECT * FROM table");
		} catch (Exception e) {
			// TODO Auto-generated catch block
		}

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			SemossPixelException e = assertThrows(SemossPixelException.class, reactor::execute);
			assertEquals("An error occurred trying to execute the query in the database: Database error",
					e.getMessage());
		}
	}

	@Test
	void testSelectQueryStruct() {
		NounStore ns = mock(NounStore.class);
		reactor.setNounStore(ns);
		GenRowStruct grs = mock(GenRowStruct.class);
		when(ns.getGenRowStruct(PixelDataType.QUERY_STRUCT.getKey())).thenReturn(grs);

		// Mock behavior of SelectQueryStructs
		SelectQueryStruct qs = mock(SelectQueryStruct.class);
		QueryColumnSelector qcs = mock(QueryColumnSelector.class);
		when(qcs.getTable()).thenReturn("table_name");
		List<IQuerySelector> selectors = new ArrayList<>();
		selectors.add(qcs);
		when(qs.getSelectors()).thenReturn(selectors);

		GenRowFilters grfs = mock(GenRowFilters.class);
		when(qs.getCombinedFilters()).thenReturn(grfs);
		when(grfs.getFilters()).thenReturn(new ArrayList<IQueryFilter>());
		// -- end of stuff I added -- Jeff

		NounMetadata nm = new NounMetadata(qs, PixelDataType.QUERY_STRUCT);
		when(grs.getNoun(0)).thenReturn(nm);

		when(qs.getQsType()).thenReturn(QUERY_STRUCT_TYPE.ENGINE);
		when(qs.retrieveQueryStructEngine()).thenReturn(engine);
		when(engine.getDatabaseType()).thenReturn(IDatabaseEngine.DATABASE_TYPE.RDBMS);

		DeleteSqlInterpreter interp = mock(DeleteSqlInterpreter.class);
		when(interp.composeQuery()).thenReturn("DELETE FROM table_name WHERE condition");

		try {
			doThrow(new SemossPixelException("Database error")).when(engine).insertData(interp.composeQuery());
		} catch (Exception e) {

		}

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<DeleteSqlInterpreter> deleteSqlInterpreterMockedStatic = Mockito
						.mockStatic(DeleteSqlInterpreter.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			SemossPixelException e = assertThrows(SemossPixelException.class, reactor::execute);
			assertEquals("Database error", e.getMessage());
		}
	}

	@Test
	void testUpdateQueryStruct() {
		NounStore ns = mock(NounStore.class);
		reactor.setNounStore(ns);
		GenRowStruct grs = mock(GenRowStruct.class);
		when(ns.getGenRowStruct(PixelDataType.QUERY_STRUCT.getKey())).thenReturn(grs);

		UpdateQueryStruct qs = mock(UpdateQueryStruct.class);
		QueryColumnSelector qcs = mock(QueryColumnSelector.class);
		when(qcs.getTable()).thenReturn("table_name");
		List<IQuerySelector> selectors = new ArrayList<>();
		selectors.add(qcs);
		when(qs.getSelectors()).thenReturn(selectors);

		GenRowFilters grfs = mock(GenRowFilters.class);
		when(qs.getCombinedFilters()).thenReturn(grfs);
		when(grfs.getFilters()).thenReturn(new ArrayList<IQueryFilter>());

		NounMetadata nm = new NounMetadata(qs, PixelDataType.QUERY_STRUCT);
		when(grs.getNoun(0)).thenReturn(nm);

		when(qs.getQsType()).thenReturn(QUERY_STRUCT_TYPE.ENGINE);
		when(qs.retrieveQueryStructEngine()).thenReturn(engine);
		when(engine.getDatabaseType()).thenReturn(IDatabaseEngine.DATABASE_TYPE.RDBMS);

		UpdateSqlInterpreter interp = mock(UpdateSqlInterpreter.class);
		when(interp.composeQuery()).thenReturn("UPDATE table_name SET column = value");

		try {
			doThrow(new SemossPixelException("Database error")).when(engine).insertData(interp.composeQuery());
		} catch (Exception e) {

		}

		try (MockedStatic<SecurityAdminUtils> sau = Mockito.mockStatic(SecurityAdminUtils.class);
				MockedStatic<UpdateSqlInterpreter> updateSqlInterpreterMockedStatic = Mockito
						.mockStatic(UpdateSqlInterpreter.class)) {
			SecurityAdminUtils s = mock(SecurityAdminUtils.class);
			sau.when(() -> SecurityAdminUtils.getInstance(user)).thenReturn(s);

			SemossPixelException e = assertThrows(SemossPixelException.class, reactor::execute);
			assertEquals("Database error", e.getMessage());
		}
	}
}
