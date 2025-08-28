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
package prerna.rdf.engine.wrappers;

import java.util.HashSet;
import java.util.Set;
import org.openrdf.query.algebra.Avg;
import org.openrdf.query.algebra.Count;
import org.openrdf.query.algebra.MathExpr;
import org.openrdf.query.algebra.Max;
import org.openrdf.query.algebra.Min;
import org.openrdf.query.algebra.Sum;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

public class CustomSparqlAggregationParser extends QueryModelVisitorBase<Exception> {

  public Set<String> values = new HashSet<String>();

  public Set<String> getValue() {
    return values;
  }

  @Override
  public void meet(Avg node) {
    values.add(
        node.getArg()
            .getParentNode()
            .getParentNode()
            .getSignature()
            .replace("ExtensionElem (", "")
            .replace(")", ""));
  }

  @Override
  public void meet(Max node) {
    values.add(
        node.getArg()
            .getParentNode()
            .getParentNode()
            .getSignature()
            .replace("ExtensionElem (", "")
            .replace(")", ""));
  }

  @Override
  public void meet(Min node) {
    values.add(
        node.getArg()
            .getParentNode()
            .getParentNode()
            .getSignature()
            .replace("ExtensionElem (", "")
            .replace(")", ""));
  }

  @Override
  public void meet(Sum node) {
    values.add(
        node.getArg()
            .getParentNode()
            .getParentNode()
            .getSignature()
            .replace("ExtensionElem (", "")
            .replace(")", ""));
  }

  @Override
  public void meet(Count node) {
    values.add(
        node.getArg()
            .getParentNode()
            .getParentNode()
            .getSignature()
            .replace("ExtensionElem (", "")
            .replace(")", ""));
  }

  @Override
  public void meet(MathExpr node) throws Exception {
    values.add(node.getParentNode().getSignature().replace("ExtensionElem (", "").replace(")", ""));
  }
}
