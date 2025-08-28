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

import java.util.List;
import prerna.util.gson.AbstractSemossTypeAdapter;
import prerna.util.gson.QueryArithmeticSelectorAdapter;
import prerna.util.gson.QueryColumnSelectorAdapter;
import prerna.util.gson.QueryConstantSelectorAdapter;
import prerna.util.gson.QueryFunctionSelectorAdapter;
import prerna.util.gson.QueryIfSelectorAdapter;
import prerna.util.gson.QueryOpaqueSelectorAdapter;
import prerna.util.gson.QueryTypedColumnSelectorAdapter;

public interface IQuerySelector {

  String PRIM_KEY_PLACEHOLDER = "PRIM_KEY_PLACEHOLDER";

  enum SELECTOR_TYPE {
    OPAQUE,
    COLUMN,
    FUNCTION,
    ARITHMETIC,
    CONSTANT,
    IF_ELSE,
    TYPED_COLUMN
  }

  /**
   * Determine the type of the selector
   *
   * @return
   */
  SELECTOR_TYPE getSelectorType();

  /**
   * Get the display name for the selector
   *
   * @return
   */
  String getAlias();

  /**
   * Set the display name for the selector
   *
   * @param alias
   */
  void setAlias(String alias);

  /**
   * Determine if it is a derived selector
   *
   * @return
   */
  boolean isDerived();

  /**
   * Return the predicted data type of the column
   *
   * @return
   */
  String getDataType();

  /**
   * Get the pixel component that generated the selector
   *
   * @return
   */
  String getQueryStructName();

  /**
   * Determine all the columns used within an expression
   *
   * @return
   */
  List<QueryColumnSelector> getAllQueryColumns();

  ////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////
  ////////////////////////////////////////////////////////

  /*
   *
   * Methods around serialization
   *
   */

  static AbstractSemossTypeAdapter getAdapterForSelector(SELECTOR_TYPE type) {
    if (type == SELECTOR_TYPE.OPAQUE) {
      return new QueryOpaqueSelectorAdapter();
    } else if (type == SELECTOR_TYPE.COLUMN) {
      return new QueryColumnSelectorAdapter();
    } else if (type == SELECTOR_TYPE.FUNCTION) {
      return new QueryFunctionSelectorAdapter();
    } else if (type == SELECTOR_TYPE.ARITHMETIC) {
      return new QueryArithmeticSelectorAdapter();
    } else if (type == SELECTOR_TYPE.CONSTANT) {
      return new QueryConstantSelectorAdapter();
    } else if (type == SELECTOR_TYPE.IF_ELSE) {
      return new QueryIfSelectorAdapter();
    } else if (type == SELECTOR_TYPE.TYPED_COLUMN) {
      return new QueryTypedColumnSelectorAdapter();
    }

    return null;
  }

  /**
   * Convert string to SELECTOR_TYPE
   *
   * @param s
   * @return
   */
  static SELECTOR_TYPE convertStringToSelectorType(String s) {
    if (s.equals(SELECTOR_TYPE.OPAQUE.toString())) {
      return SELECTOR_TYPE.OPAQUE;
    } else if (s.equals(SELECTOR_TYPE.COLUMN.toString())) {
      return SELECTOR_TYPE.COLUMN;
    } else if (s.equals(SELECTOR_TYPE.FUNCTION.toString())) {
      return SELECTOR_TYPE.FUNCTION;
    } else if (s.equals(SELECTOR_TYPE.ARITHMETIC.toString())) {
      return SELECTOR_TYPE.ARITHMETIC;
    } else if (s.equals(SELECTOR_TYPE.CONSTANT.toString())) {
      return SELECTOR_TYPE.CONSTANT;
    } else if (s.equals(SELECTOR_TYPE.IF_ELSE.toString())) {
      return SELECTOR_TYPE.IF_ELSE;
    } else if (s.equals(SELECTOR_TYPE.TYPED_COLUMN.toString())) {
      return SELECTOR_TYPE.TYPED_COLUMN;
    }
    return null;
  }

  /**
   * Get the class for each selector type
   *
   * @param type
   * @return
   */
  static Class getQuerySelectorClassFromType(SELECTOR_TYPE type) {
    if (type == SELECTOR_TYPE.OPAQUE) {
      return QueryOpaqueSelector.class;
    } else if (type == SELECTOR_TYPE.COLUMN) {
      return QueryColumnSelector.class;
    } else if (type == SELECTOR_TYPE.FUNCTION) {
      return QueryFunctionSelector.class;
    } else if (type == SELECTOR_TYPE.ARITHMETIC) {
      return QueryArithmeticSelector.class;
    } else if (type == SELECTOR_TYPE.CONSTANT) {
      return QueryConstantSelector.class;
    } else if (type == SELECTOR_TYPE.IF_ELSE) {
      return QueryIfSelector.class;
    } else if (type == SELECTOR_TYPE.TYPED_COLUMN) {
      return QueryTypedColumnSelector.class;
    }

    return null;
  }
}
