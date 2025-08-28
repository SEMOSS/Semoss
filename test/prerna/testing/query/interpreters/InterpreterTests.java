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
package prerna.testing.query.interpreters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.testing.ApiSemossTestUtils;
import prerna.testing.ApiTestsSemossConstants;
import prerna.testing.PixelChain;
import prerna.testing.PixelQueryTestUtils;
import prerna.testing.reactor.imports.ImportTestUtility;

public class InterpreterTests {

  public static void testLimitOffset(
      String databaseId, String frameType, String frameAlias, boolean override) {
    // import movie data to frame
    ITableDataFrame frame =
        ImportTestUtility.createMovieFrame(databaseId, frameType, frameAlias, override);
    // create pixel query to test interpreter
    PixelChain framePC = PixelQueryTestUtils.frame(frameAlias);
    String[] cols = new String[] {ApiTestsSemossConstants.GENRE};
    String[] alias = new String[] {"x"};
    PixelChain select = PixelQueryTestUtils.select(cols, alias);
    PixelChain sort =
        PixelQueryTestUtils.sort(new String[] {ApiTestsSemossConstants.GENRE}, "desc");
    PixelChain limit = PixelQueryTestUtils.limit(2);
    PixelChain offset = PixelQueryTestUtils.offset(3);
    PixelChain collectAll = PixelQueryTestUtils.collectAll();

    String pixel =
        ApiSemossTestUtils.buildPixelChain(framePC, select, sort, limit, offset, collectAll);
    NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
    BasicIteratorTask task = (BasicIteratorTask) nm.getValue();
    List<Map<String, Object>> list = PixelQueryTestUtils.flushTaskToList(task, 2);
    ApiSemossTestUtils.print(list);
    // test to get sorted values
    int i = 0;
    {
      Map<String, Object> mapInfo = list.get(i++);
      assertEquals("Documentary", mapInfo.get("x"));
    }
    {
      Map<String, Object> mapInfo = list.get(i++);
      assertEquals("Comedy-Musical", mapInfo.get("x"));
    }
  }

  public static void testSort(
      String databaseId, String frameType, String frameAlias, boolean override) {
    // import movie data to frame
    ITableDataFrame frame =
        ImportTestUtility.createMovieFrame(databaseId, frameType, frameAlias, override);
    // create pixel query to test interpreter
    PixelChain framePC = PixelQueryTestUtils.frame(frameAlias);
    String[] cols = new String[] {ApiTestsSemossConstants.GENRE};
    String[] alias = new String[] {"x"};
    PixelChain select = PixelQueryTestUtils.select(cols, alias);
    PixelChain sort =
        PixelQueryTestUtils.sort(new String[] {ApiTestsSemossConstants.GENRE}, "desc");
    PixelChain collectAll = PixelQueryTestUtils.collectAll();

    String pixel = ApiSemossTestUtils.buildPixelChain(framePC, select, sort, collectAll);
    NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
    BasicIteratorTask task = (BasicIteratorTask) nm.getValue();
    List<Map<String, Object>> list = PixelQueryTestUtils.flushTaskToList(task, 6);
    ApiSemossTestUtils.print(list);
    // test to get sorted values
    int i = 0;
    {
      Map<String, Object> mapInfo = list.get(i++);
      assertEquals("Thriller-Horror", mapInfo.get("x"));
    }
    {
      Map<String, Object> mapInfo = list.get(i++);
      assertEquals("Family-Animation", mapInfo.get("x"));
    }
    {
      Map<String, Object> mapInfo = list.get(i++);
      assertEquals("Drama", mapInfo.get("x"));
    }
    {
      Map<String, Object> mapInfo = list.get(i++);
      assertEquals("Documentary", mapInfo.get("x"));
    }
    {
      Map<String, Object> mapInfo = list.get(i++);
      assertEquals("Comedy-Musical", mapInfo.get("x"));
    }

    {
      Map<String, Object> mapInfo = list.get(i++);
      assertEquals("Action-Adventure", mapInfo.get("x"));
    }
  }

  /**
   * Test that the interpreter creates an alias
   *
   * @param frameType
   * @param frameAlias
   * @param override
   */
  public static void testAlias(
      String databaseId, String frameType, String frameAlias, boolean override) {
    // import movie data to frame
    ITableDataFrame frame =
        ImportTestUtility.createMovieFrame(databaseId, frameType, frameAlias, override);
    // create pixel query to test interpreter
    PixelChain framePC = PixelQueryTestUtils.frame(frameAlias);
    String[] cols = new String[] {ApiTestsSemossConstants.GENRE, ApiTestsSemossConstants.STUDIO};
    String[] alias = new String[] {"x", "y"};
    PixelChain select = PixelQueryTestUtils.select(cols, alias);
    PixelChain collectAll = PixelQueryTestUtils.collectAll();

    String pixel = ApiSemossTestUtils.buildPixelChain(framePC, select, collectAll);
    NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
    BasicIteratorTask task = (BasicIteratorTask) nm.getValue();
    List<Map<String, Object>> list = PixelQueryTestUtils.flushTaskToList(task, 105);
    Map<String, Object> info = list.get(0);

    assertTrue(info.containsKey("x"));
    assertTrue(info.containsKey("y"));
  }

  public static void testGroupBy(
      String databaseId, String frameType, String frameAlias, boolean override) {
    // import movie data to frame
    ITableDataFrame frame =
        ImportTestUtility.createMovieFrame(databaseId, frameType, frameAlias, override);
    // create pixel query to test interpreter
    PixelChain framePC = PixelQueryTestUtils.frame(frameAlias);
    String[] cols =
        new String[] {
          ApiTestsSemossConstants.GENRE, "Count(" + ApiTestsSemossConstants.TITLE + ")"
        };
    String[] alias = new String[] {"gen", "count_gen"};

    PixelChain select = PixelQueryTestUtils.select(cols, alias);
    PixelChain groupBy = PixelQueryTestUtils.groupBy(new String[] {ApiTestsSemossConstants.GENRE});
    PixelChain sort = PixelQueryTestUtils.sort(new String[] {ApiTestsSemossConstants.GENRE}, "asc");
    PixelChain collectAll = PixelQueryTestUtils.collectAll();

    String pixel = ApiSemossTestUtils.buildPixelChain(framePC, select, groupBy, sort, collectAll);
    NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
    BasicIteratorTask task = (BasicIteratorTask) nm.getValue();
    List<Map<String, Object>> list = PixelQueryTestUtils.flushTaskToList(task, 6);
    int i = 0;
    {
      Map<String, Object> mapInfo = list.get(i++);
      assertEquals("Action-Adventure", mapInfo.get("gen"));
      assertEquals(90L, (Long) mapInfo.get("count_gen"));
    }
    {
      Map<String, Object> mapInfo = list.get(i++);
      assertEquals("Comedy-Musical", mapInfo.get("gen"));
      assertEquals(116L, (Long) mapInfo.get("count_gen"));
    }
    {
      Map<String, Object> mapInfo = list.get(i++);
      assertEquals("Documentary", mapInfo.get("gen"));
      assertEquals(1L, (Long) mapInfo.get("count_gen"));
    }
    {
      Map<String, Object> mapInfo = list.get(i++);
      assertEquals("Drama", mapInfo.get("gen"));
      assertEquals(131L, (Long) mapInfo.get("count_gen"));
    }
    {
      Map<String, Object> mapInfo = list.get(i++);
      assertEquals("Family-Animation", mapInfo.get("gen"));
      assertEquals(34L, (Long) mapInfo.get("count_gen"));
    }
    {
      Map<String, Object> mapInfo = list.get(i++);
      assertEquals("Thriller-Horror", mapInfo.get("gen"));
      assertEquals(69L, (Long) mapInfo.get("count_gen"));
    }
  }

  public static void testDistinct(
      String databaseId, String frameType, String frameAlias, boolean override) {
    // import movie data to frame
    ITableDataFrame frame =
        ImportTestUtility.createMovieFrame(databaseId, frameType, frameAlias, override);
    // create pixel query to test interpreter
    PixelChain framePC = PixelQueryTestUtils.frame(frameAlias);
    String[] cols = new String[] {ApiTestsSemossConstants.GENRE};
    String[] alias = new String[] {"x"};
    PixelChain select = PixelQueryTestUtils.select(cols, alias);
    PixelChain dis = PixelQueryTestUtils.distinct(false);

    PixelChain collectAll = PixelQueryTestUtils.collectAll();

    String pixel = ApiSemossTestUtils.buildPixelChain(framePC, select, dis, collectAll);
    NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
    BasicIteratorTask task = (BasicIteratorTask) nm.getValue();
    List<Map<String, Object>> list = PixelQueryTestUtils.flushTaskToList(task, 441);
    Map<String, Object> info = list.get(0);
    assertTrue(info.containsKey("x"));

    // test distinct true
    dis = PixelQueryTestUtils.distinct(true);
    pixel = ApiSemossTestUtils.buildPixelChain(framePC, select, dis, collectAll);
    nm = ApiSemossTestUtils.processPixel(pixel);
    task = (BasicIteratorTask) nm.getValue();
    list = PixelQueryTestUtils.flushTaskToList(task, 6);
    info = list.get(0);
    assertTrue(info.containsKey("x"));
  }

  public static void testFilter(
      String databaseId, String frameType, String frameAlias, boolean override) {
    // import movie data to frame
    ITableDataFrame frame =
        ImportTestUtility.createMovieFrame(databaseId, frameType, frameAlias, override);
    // create pixel query to test interpreter
    PixelChain framePC = PixelQueryTestUtils.frame(frameAlias);
    String[] cols =
        new String[] {
          ApiTestsSemossConstants.GENRE, "Count(" + ApiTestsSemossConstants.TITLE + ")"
        };
    String[] alias = new String[] {"gen", "count_gen"};

    PixelChain select = PixelQueryTestUtils.select(cols, alias);
    PixelChain filter =
        PixelQueryTestUtils.filter(ApiTestsSemossConstants.GENRE, "==", "Comedy-Musical");

    PixelChain groupBy = PixelQueryTestUtils.groupBy(new String[] {ApiTestsSemossConstants.GENRE});
    PixelChain sort = PixelQueryTestUtils.sort(new String[] {ApiTestsSemossConstants.GENRE}, "asc");
    PixelChain collectAll = PixelQueryTestUtils.collectAll();

    String pixel =
        ApiSemossTestUtils.buildPixelChain(framePC, select, filter, groupBy, sort, collectAll);
    NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
    BasicIteratorTask task = (BasicIteratorTask) nm.getValue();
    List<Map<String, Object>> list = PixelQueryTestUtils.flushTaskToList(task, 1);
    int i = 0;
    {
      Map<String, Object> mapInfo = list.get(i++);
      assertEquals("Comedy-Musical", mapInfo.get("gen"));
      assertEquals(116L, (Long) mapInfo.get("count_gen"));
    }

    // test or filter
    String filterStr =
        PixelQueryTestUtils.buildFilter(ApiTestsSemossConstants.GENRE, "==", "Comedy-Musical");
    String filterStr2 =
        PixelQueryTestUtils.buildFilter(ApiTestsSemossConstants.GENRE, "==", "Drama");
    filter = new PixelChain("Filter((" + filterStr + " OR " + filterStr2 + "))");

    pixel = ApiSemossTestUtils.buildPixelChain(framePC, select, filter, groupBy, sort, collectAll);
    nm = ApiSemossTestUtils.processPixel(pixel);
    task = (BasicIteratorTask) nm.getValue();
    list = PixelQueryTestUtils.flushTaskToList(task, 2);
    i = 0;
    {
      Map<String, Object> mapInfo = list.get(i++);
      assertEquals("Comedy-Musical", mapInfo.get("gen"));
      assertEquals(116L, (Long) mapInfo.get("count_gen"));
    }
    {
      Map<String, Object> mapInfo = list.get(i++);
      assertEquals("Drama", mapInfo.get("gen"));
      assertEquals(131L, (Long) mapInfo.get("count_gen"));
    }
  }

  public static void testGroupFilter(
      String databaseId, String frameType, String frameAlias, boolean override) {
    // import movie data to frame
    ITableDataFrame frame =
        ImportTestUtility.createMovieFrame(databaseId, frameType, frameAlias, override);
    // create pixel query to test interpreter
    PixelChain framePC = PixelQueryTestUtils.frame(frameAlias);
    String[] cols = new String[] {ApiTestsSemossConstants.GENRE};
    String[] alias = new String[] {"gen"};

    PixelChain select = PixelQueryTestUtils.select(cols, alias);
    PixelChain filter =
        PixelQueryTestUtils.filter(ApiTestsSemossConstants.GENRE, "==", "Comedy-Musical");

    PixelChain groupBy = PixelQueryTestUtils.groupBy(new String[] {ApiTestsSemossConstants.GENRE});
    PixelChain collectAll = PixelQueryTestUtils.collectAll();

    String pixel = ApiSemossTestUtils.buildPixelChain(framePC, select, filter, groupBy, collectAll);
    NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
    BasicIteratorTask task = (BasicIteratorTask) nm.getValue();
    List<Map<String, Object>> list = PixelQueryTestUtils.flushTaskToList(task, 1);
    int i = 0;
    {
      Map<String, Object> mapInfo = list.get(i++);
      assertEquals("Comedy-Musical", mapInfo.get("gen"));
    }

    cols = new String[] {ApiTestsSemossConstants.GENRE, ApiTestsSemossConstants.NOMINATED};
    alias = new String[] {"gen", "nom"};

    select = PixelQueryTestUtils.select(cols, alias);
    filter = PixelQueryTestUtils.filter(ApiTestsSemossConstants.GENRE, "==", "Comedy-Musical");

    groupBy =
        PixelQueryTestUtils.groupBy(
            new String[] {ApiTestsSemossConstants.GENRE, ApiTestsSemossConstants.NOMINATED});

    pixel = ApiSemossTestUtils.buildPixelChain(framePC, select, filter, groupBy, collectAll);
    nm = ApiSemossTestUtils.processPixel(pixel);
    task = (BasicIteratorTask) nm.getValue();
    list = PixelQueryTestUtils.flushTaskToList(task, 2);
    ApiSemossTestUtils.print(list);
    i = 0;
    {
      Map<String, Object> mapInfo = list.get(i++);
      assertEquals("Comedy-Musical", mapInfo.get("gen"));
      assertEquals("N", mapInfo.get("nom"));
    }
    {
      Map<String, Object> mapInfo = list.get(i++);
      assertEquals("Comedy-Musical", mapInfo.get("gen"));
      assertEquals("Y", mapInfo.get("nom"));
    }
  }
}
