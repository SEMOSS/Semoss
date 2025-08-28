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
package prerna.query.querystruct.selectors;

import java.io.Serializable;

public abstract class AbstractQuerySelector implements IQuerySelector, Serializable {

  protected String alias;

  /** Default constructor */
  public AbstractQuerySelector() {
    // we want the alias to be an empty string
    // since we dont want to get null pointers
    // when we to the equals when we merge selectors
    this.alias = "";
  }

  @Override
  public void setAlias(String alias) {
    // cannot have __ in the alias
    if (alias != null) {
      if (alias.contains("__")) {
        this.alias = alias.split("__")[1];
      } else {
        this.alias = alias;
      }
    }
  }

  @Override
  public String toString() {
    return this.getQueryStructName();
  }
}
