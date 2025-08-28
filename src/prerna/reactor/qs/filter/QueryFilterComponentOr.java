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

import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter.QUERY_FILTER_TYPE;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class QueryFilterComponentOr extends FilterReactor {

  @Override
  public NounMetadata execute() {
    // we want to return a filter object
    // so it can be integrated with the query struct
    OrQueryFilter filter = new OrQueryFilter();

    // validate if they are all AND or SIMPLE filters
    boolean consolidate = true;
    int size = this.curRow.size();
    CONSOLIDATE_LOOP:
    for (int i = 0; i < size; i++) {
      Object v = this.curRow.get(i);
      if (v instanceof IQueryFilter) {
        QUERY_FILTER_TYPE fType = ((IQueryFilter) v).getQueryFilterType();
        if (fType != IQueryFilter.QUERY_FILTER_TYPE.SIMPLE
            && fType != IQueryFilter.QUERY_FILTER_TYPE.OR) {
          consolidate = false;
          break CONSOLIDATE_LOOP;
        }
      }
    }

    if (consolidate) {
      for (int i = 0; i < size; i++) {
        Object v = this.curRow.get(i);
        if (v instanceof SimpleQueryFilter) {
          filter.addFilter((SimpleQueryFilter) v);
        } else if (v instanceof OrQueryFilter) {
          filter.addFilter(((OrQueryFilter) v).getFilterList());
        }
      }
    } else {
      for (int i = 0; i < size; i++) {
        Object v = this.curRow.get(i);
        if (v instanceof IQueryFilter) {
          filter.addFilter((IQueryFilter) v);
        }
      }
    }

    return new NounMetadata(filter, PixelDataType.FILTER);
  }

  @Override
  public void mergeUp() {
    // merge this reactor into the parent reactor
    if (parentReactor != null) {
      // filters are added to curRow
      parentReactor.getCurRow().add(execute());
    }
  }
}
