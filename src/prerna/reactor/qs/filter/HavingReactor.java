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
package prerna.reactor.qs.filter;

import java.util.List;
import java.util.Vector;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AbstractListFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class HavingReactor extends FilterReactor {

  public HavingReactor() {
    this.keysToGet = new String[] {ReactorKeysEnum.FILTERS.getKey()};
  }

  protected AbstractQueryStruct createQueryStruct() {
    List<Object> filters = this.curRow.getValuesOfType(PixelDataType.FILTER);
    if (filters.isEmpty()) {
      throw new IllegalArgumentException("No filter founds to append into the query");
    }
    for (int i = 0; i < filters.size(); i++) {
      IQueryFilter nextFilter = (IQueryFilter) filters.get(i);
      if (nextFilter != null) {
        if (nextFilter.getQueryFilterType() == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
          if (isValidFilter((SimpleQueryFilter) nextFilter)) {
            // try to fix the selectors if it is a query struct
            // when in reality it is just a function column selector
            qs.addHavingFilter(processSimpleFilter((SimpleQueryFilter) nextFilter));
          }
        } else {
          // need to recursively try to fix
          // the selectors if they are a query struct
          // when in reality it is just a function column selector
          qs.addHavingFilter(processQueryFilter((AbstractListFilter) nextFilter));
        }
      }
    }
    return qs;
  }

  /**
   * Process the query filter for the case that we have more than just one simple filter
   *
   * @param filter
   * @return
   */
  private AbstractListFilter processQueryFilter(AbstractListFilter filter) {
    List<IQueryFilter> newList = new Vector<IQueryFilter>();

    List<IQueryFilter> oldList = filter.getFilterList();
    for (IQueryFilter f : oldList) {
      if (f instanceof AbstractListFilter) {
        newList.add(processQueryFilter((AbstractListFilter) f));
      } else {
        // it is simple
        newList.add(processSimpleFilter((SimpleQueryFilter) f));
      }
    }

    filter.getFilterList().clear();
    filter.setFilterList(newList);
    return filter;
  }

  /**
   * Shift the query struct if it is just an aggregation
   *
   * @param filter
   * @return
   */
  private SimpleQueryFilter processSimpleFilter(SimpleQueryFilter filter) {
    NounMetadata lComp = filter.getLComparison();
    NounMetadata rComp = filter.getRComparison();

    NounMetadata newL = null;
    if (lComp.getValue() instanceof SelectQueryStruct) {
      SelectQueryStruct query = (SelectQueryStruct) lComp.getValue();
      if (query.getCombinedFilters().isEmpty()
          && query.getRelations().isEmpty()
          && query.getSelectors().size() == 1) {
        newL = new NounMetadata(query.getSelectors().get(0), PixelDataType.COLUMN);
      } else {
        newL = lComp;
      }
    } else {
      newL = lComp;
    }

    NounMetadata newR = null;
    if (rComp.getValue() instanceof SelectQueryStruct) {
      SelectQueryStruct query = (SelectQueryStruct) rComp.getValue();
      if (query.getCombinedFilters().isEmpty()
          && query.getRelations().isEmpty()
          && query.getSelectors().size() == 1) {
        newR = new NounMetadata(query.getSelectors().get(0), PixelDataType.COLUMN);
      } else {
        newR = rComp;
      }
    } else {
      newR = rComp;
    }

    return new SimpleQueryFilter(newL, filter.getComparator(), newR);
  }

  public String getName() {
    return "Having";
  }
}
