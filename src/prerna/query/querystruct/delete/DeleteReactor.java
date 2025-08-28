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
package prerna.query.querystruct.delete;

import java.util.List;
import java.util.Vector;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;

public class DeleteReactor extends AbstractQueryStructReactor {

  @Override
  public AbstractQueryStruct createQueryStruct() {
    SelectQueryStruct qs = new SelectQueryStruct();
    // merge any existing values
    if (this.qs != null) {
      qs.merge(this.qs);
      qs.setQsType(this.qs.getQsType());
    }

    // table
    GenRowStruct tab_grs = this.store.getNoun("from");
    List<IQuerySelector> selectors = new Vector<IQuerySelector>();

    QueryColumnSelector sel = new QueryColumnSelector(tab_grs.get(0).toString());
    selectors.add(sel);

    qs.setSelectors(selectors);

    this.qs = qs;
    return qs;
  }

  public void setNounStore(NounStore ns) {
    this.store = ns;
  }

  public void setQs(SelectQueryStruct qs) {
    this.qs = qs;
  }

  public String getName() {
    return "Delete";
  }
}
