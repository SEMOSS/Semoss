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
package prerna.poi.main.helper;

import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import prerna.algorithm.api.SemossDataType;

public class FileHelperUtil {

  private FileHelperUtil() {}

  /**
   * Convenience method to get the 2 maps we usually use within CsvQueryStruct
   *
   * @param headers
   * @param predictions
   * @return
   */
  public static Map[] generateDataTypeMapsFromPrediction(String[] headers, Object[][] predictions) {
    Map[] retArray = new Map[2];
    Map<String, String> dataTypeMap = new LinkedHashMap<>();
    Map<String, String> additionalDataTypeMap = new LinkedHashMap<>();
    retArray[0] = dataTypeMap;
    retArray[1] = additionalDataTypeMap;

    int numHeaders = headers.length;
    for (int i = 0; i < numHeaders; i++) {
      Object[] pred = predictions[i];
      SemossDataType type = (SemossDataType) pred[0];
      dataTypeMap.put(headers[i], type.toString());
      if (pred[1] != null) {
        additionalDataTypeMap.put(headers[i], pred[1] + "");
      }
    }
    return retArray;
  }

  /**
   * Convenience method to just return the data types
   *
   * @param predictions
   * @return
   */
  public static String[] generateDataTypeArrayFromPrediction(Object[][] predictions) {
    int numHeaders = predictions.length;
    String[] returnTypes = new String[numHeaders];

    for (int i = 0; i < numHeaders; i++) {
      Object[] pred = predictions[i];
      SemossDataType type = (SemossDataType) pred[0];
      returnTypes[i] = type.toString();
    }
    return returnTypes;
  }

  /**
   * Determine date additional formatting
   *
   * @param type
   * @param formatTracker
   * @return
   */
  public static Object[] determineDateFormatting(
      SemossDataType type, Map<String, Integer> formatTracker) {
    Object[] result = new Object[2];
    result[0] = type;
    if (formatTracker.size() == 1) {
      result[1] = formatTracker.keySet().iterator().next();
    } else {
      // trying to figure out the best match for the format
      // taking into consideration formats that are basically the same
      // but may contain 2 value (i.e. 11th day) vs 1 value (i.e. 1st day)
      // which matches to different patterns
      if (type == SemossDataType.DATE || type == SemossDataType.TIMESTAMP) {
        reconcileDateFormats(formatTracker);
      }

      // now just choose the most occuring one
      String mostOccuringFormat =
          Collections.max(formatTracker.entrySet(), Comparator.comparingInt(Map.Entry::getValue))
              .getKey();
      result[1] = mostOccuringFormat;
    }
    return result;
  }

  /**
   * Try to reconcile different date formats
   *
   * @param formats
   * @return
   */
  public static void reconcileDateFormats(Map<String, Integer> formats) {
    int numFormats = formats.size();
    if (numFormats == 1) {
      return;
    }

    // loop and compare every format to every other format
    // once we have a match, we will recalculate
    String[] formatPaterns = formats.keySet().toArray(new String[numFormats]);
    char[] charsToFind = new char[] {'M', 'd', 'H', 'h', 'm', 's'};

    for (int i = 0; i < numFormats; i++) {
      String thisFormat = formatPaterns[i];
      // get the regex form of this
      String regexThisFormat = thisFormat;
      Pattern doubleCharRegex = null;
      Matcher matcher = null;

      for (char c : charsToFind) {
        if (!regexThisFormat.contains(c + "")) {
          continue;
        }
        // trim the format first
        // so MM or dd becomes just M or d
        doubleCharRegex = Pattern.compile(c + "{1,2}");
        matcher = doubleCharRegex.matcher(regexThisFormat);
        regexThisFormat = matcher.replaceAll(c + "");

        int indexToFind = regexThisFormat.lastIndexOf(c);
        int len = regexThisFormat.length();
        regexThisFormat =
            regexThisFormat.substring(0, indexToFind + 1)
                + "{1,2}"
                + regexThisFormat.substring(indexToFind + 1, len);
      }

      Pattern pattern = Pattern.compile(regexThisFormat);
      for (int j = i + 1; j < numFormats; j++) {
        String otherFormat = formatPaterns[j];
        matcher = pattern.matcher(otherFormat);

        if (matcher.find()) {
          // they are equivalent
          String largerFormat =
              thisFormat.length() > otherFormat.length() ? thisFormat : otherFormat;
          int c1 = formats.remove(thisFormat);
          int c2 = formats.remove(otherFormat);
          formats.put(largerFormat, c1 + c2);
          // recursively go back and recalculate
          reconcileDateFormats(formats);
          return;
        }
      }
    }
  }
}
