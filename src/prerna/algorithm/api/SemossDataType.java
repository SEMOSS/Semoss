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
package prerna.algorithm.api;

import prerna.sablecc2.om.PixelDataType;
import prerna.util.Utility;

public enum SemossDataType {
  BOOLEAN,
  INT,
  DOUBLE,
  STRING,
  DATE,
  TIMESTAMP,
  FACTOR;

  public static boolean isNotString(SemossDataType type) {
    if (type == STRING || type == FACTOR) {
      return false;
    }
    return true;
  }

  public static boolean isNotString(String typeStr) {
    SemossDataType type = convertStringToDataType(typeStr);
    if (type == STRING || type == FACTOR) {
      return false;
    }
    return true;
  }

  public static SemossDataType convertStringToDataType(String dataType) {
    if (dataType == null) {
      return null;
    }
    if (dataType.startsWith("TYPE:")) {
      dataType = dataType.substring("TYPE:".length());
    }

    if (Utility.isBoolean(dataType)) {
      return SemossDataType.BOOLEAN;
    } else if (Utility.isIntegerType(dataType)) {
      return SemossDataType.INT;
    } else if (Utility.isDoubleType(dataType)) {
      return SemossDataType.DOUBLE;
    } else if (Utility.isDateType(dataType)) {
      return SemossDataType.DATE;
    } else if (Utility.isTimeStamp(dataType)) {
      return SemossDataType.TIMESTAMP;
    } else if (Utility.isFactorType(dataType)) {
      return SemossDataType.FACTOR;
    } else {
      return SemossDataType.STRING;
    }
  }

  /**
   * Convert between {@link prerna.sablecc2.om.PixelDataType} and SemossDataType
   *
   * @param type
   * @return
   */
  public static SemossDataType convertFromSemossDataType(PixelDataType type) {
    if (type == PixelDataType.BOOLEAN) {
      return BOOLEAN;
    } else if (type == PixelDataType.CONST_INT) {
      return INT;
    } else if (type == PixelDataType.CONST_DECIMAL) {
      return DOUBLE;
    } else if (type == PixelDataType.CONST_STRING) {
      return STRING;
    } else if (type == PixelDataType.CONST_DATE) {
      return DATE;
    } else if (type == PixelDataType.CONST_TIMESTAMP) {
      return TIMESTAMP;
    }

    return null;
  }

  public static PixelDataType convertToPixelDataType(SemossDataType type) {
    if (type == BOOLEAN) {
      return PixelDataType.BOOLEAN;
    } else if (type == INT) {
      return PixelDataType.CONST_INT;
    } else if (type == DOUBLE) {
      return PixelDataType.CONST_DECIMAL;
    } else if (type == STRING) {
      return PixelDataType.CONST_STRING;
    } else if (type == DATE) {
      return PixelDataType.CONST_DATE;
    } else if (type == TIMESTAMP) {
      return PixelDataType.CONST_TIMESTAMP;
    }

    return null;
  }

  public static String convertDataTypeToString(SemossDataType dataType) {
    if (dataType == null) {
      return null;
    }

    if (dataType == SemossDataType.STRING || dataType == SemossDataType.FACTOR) {
      return "STRING";
    } else if (dataType == SemossDataType.INT) {
      return "INT";
    } else if (dataType == SemossDataType.DOUBLE) {
      return "DOUBLE";
    } else if (dataType == SemossDataType.DATE) {
      return "DATE";
    } else if (dataType == SemossDataType.TIMESTAMP) {
      return "TIMESTAMP";
    } else if (dataType == SemossDataType.BOOLEAN) {
      return "BOOLEAN";
    }

    return null;
  }

  public static String[] convertSemossDataTypeArrToStringArr(SemossDataType[] dataTypes) {
    if (dataTypes == null) {
      return null;
    }

    String[] retArr = new String[dataTypes.length];
    for (int i = 0; i < dataTypes.length; i++) {
      retArr[i] = convertDataTypeToString(dataTypes[i]);
    }

    return retArr;
  }
}
