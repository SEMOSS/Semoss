/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.query.querystruct;

// set operation is typically used for

public class OpExpression extends SelectQueryStruct {

  boolean composite = false;

  Object rightItem = null;
  Object leftItem = null;

  String rightExpr = null;
  String leftExpr = null;
  String on = null;

  String expression = null;
  SelectQueryStruct item = null;
  String comparator = null;

  public void setRightExpresion(Object rightItem) {
    this.rightItem = rightItem;
  }

  public void setLeftExpresion(Object leftItem) {
    this.leftItem = leftItem;
  }

  public void setExpression(String expression) {
    this.expression = expression;
  }

  public void setFromItem(SelectQueryStruct item) {
    this.item = item;
  }

  public void setComposite(boolean composite) {
    this.composite = composite;
  }

  public void setLeftExpr(String expr) {
    this.leftExpr = expr;
  }

  public void setRightExpr(String expr) {
    this.rightExpr = expr;
  }

  public void setOn(String on) {
    this.expression = on;
  }
}
