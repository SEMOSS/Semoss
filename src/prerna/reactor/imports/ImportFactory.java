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
package prerna.reactor.imports;

import java.util.Iterator;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.TinkerFrame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.ds.py.PandasFrame;
import prerna.ds.r.RDataTable;
import prerna.ds.rdbms.AbstractRdbmsFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.SelectQueryStruct;

public class ImportFactory {

  public static IImporter getImporter(ITableDataFrame frame, SelectQueryStruct qs) {
    if (frame instanceof AbstractRdbmsFrame) {
      return new RdbmsImporter((AbstractRdbmsFrame) frame, qs);
    } else if (frame instanceof TinkerFrame) {
      return new TinkerImporter((TinkerFrame) frame, qs);
    } else if (frame instanceof RDataTable) {
      return new RImporter((RDataTable) frame, qs);
    } else if (frame instanceof PandasFrame) {
      return new PandasImporter((PandasFrame) frame, qs);
    } else if (frame instanceof NativeFrame) {
      return new NativeImporter((NativeFrame) frame, qs);
    }
    return null;
  }

  public static IImporter getImporter(
      ITableDataFrame frame, SelectQueryStruct qs, Iterator<IHeadersDataRow> it) {
    if (frame instanceof AbstractRdbmsFrame) {
      return new RdbmsImporter((AbstractRdbmsFrame) frame, qs, it);
    } else if (frame instanceof TinkerFrame) {
      return new TinkerImporter((TinkerFrame) frame, qs, it);
    } else if (frame instanceof RDataTable) {
      return new RImporter((RDataTable) frame, qs, it);
    } else if (frame instanceof PandasFrame) {
      return new PandasImporter((PandasFrame) frame, qs, it);
    } else if (frame instanceof NativeFrame) {
      return new NativeImporter((NativeFrame) frame, qs, it);
    }
    return null;
  }
}
