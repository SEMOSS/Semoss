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
package prerna.sablecc2.om;

import java.util.HashSet;
import java.util.Set;
import prerna.reactor.IReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class Expression {

  private String expression = null;
  private Set<String> inputs = null;

  private Object left; // could be a string, decimal, an expression, or a lambda
  private Object right; // could be a string, decimal, an expression, or a lamda
  private String operation = ""; // +, -, *, /, etc.

  public Expression(String expression, String[] inputs) {
    this.expression = expression;
    this.inputs = new HashSet<String>();
    for (String input : inputs) {
      this.inputs.add(input);
    }
  }

  /**
   * @return Builds the expression by recursively evaluating its left and right sub expressions and
   *     returns the string representation which will feed directly to java
   */
  public String getExpression() {
    // build the left side
    String leftSide = getEvaluatedString(left);
    // build the right side
    String rightSide = getEvaluatedString(right);
    return leftSide + " " + operation + " " + rightSide;
  }

  /**
   * @param expr
   * @return This function evaluates one side of the expression and returns the string
   *     representation which can be put into java
   */
  private String getEvaluatedString(Object expr) {

    // The expr object can either be an expression, a lambda, or a constant

    // if we have a lambda (Or IReactor) execute and get the string representation of the execution
    // Ex: Sum(MovieBudget) -> 1324792349
    if (expr instanceof IReactor) {
      Object result = ((IReactor) expr).execute();
      NounMetadata resultNoun = (NounMetadata) result;
      return resultNoun.getValue().toString();
    }

    // if we have an expression then get the evaluated Expression String from the instance
    // Ex: 3 + Sum(MovieBudget) -> 3 + 1324792349
    else if (expr instanceof Expression) {
      return "("
          + ((Expression) expr).getExpression()
          + ")"; // add the parenthesis to preserve order of operations
    }

    // otherwise just return it as a string
    // Ex: 2 or a
    else {
      return expr.toString().trim();
    }
  }

  /**
   * @param left
   *     <p>Set the left side of the expression
   */
  public void setLeft(Object left) {
    this.left = left;
  }

  /**
   * @param right
   *     <p>Set the right side of the expression
   */
  public void setRight(Object right) {
    this.right = right;
  }

  /**
   * @param operation
   *     <p>What operator are we using for this expression *, /, +, -
   */
  public void setOperation(String operation) {
    this.operation = operation;
  }

  // TODO : this needs to recursively get all the inputs of all the expressions
  /**
   * @return This method will return this expression's inputs as well as the inputs of all of its
   *     subExpressions
   */
  public String[] getInputs() {
    Set<String> exprInputs = new HashSet<>();

    // add THIS expressions inputs to the set
    exprInputs.addAll(this.inputs);

    // add left side's inputs
    addInputs(exprInputs, this.left);

    // add right side's inputs
    addInputs(exprInputs, this.right);
    return exprInputs.toArray(new String[exprInputs.size()]);
  }

  /**
   * @param exprInputs
   * @param obj
   *     <p>Recurively iterates down the subexpressions to get inputs
   *     <p>If obj is an expression, add the inputs and go down left and ride side If obj is not an
   *     expression we do not need to go further
   */
  private void addInputs(Set<String> exprInputs, Object obj) {
    if (obj instanceof Expression) {
      Expression expr = (Expression) obj;
      exprInputs.addAll(expr.inputs);
      addInputs(exprInputs, expr.left);
      addInputs(exprInputs, expr.right);
    }
  }

  /** Used for debugging to easily see what expression is being used */
  public String toString() {
    return this.expression;
  }
}
