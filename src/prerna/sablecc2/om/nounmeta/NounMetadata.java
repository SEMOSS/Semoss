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
package prerna.sablecc2.om.nounmeta;

import com.google.gson.Gson;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import prerna.date.SemossDate;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.util.gson.GsonUtility;

public class NounMetadata implements Serializable {

  Object value;
  PixelDataType noun;
  List<PixelOperationType> opType = new Vector<PixelOperationType>();

  String explanation = "";
  List<NounMetadata> additionalReturns = new Vector<NounMetadata>();

  /** Default constructor for preset nouns */
  NounMetadata() {}

  public NounMetadata(Object value, PixelDataType noun) {
    this(value, noun, PixelOperationType.OPERATION);
  }

  public NounMetadata(Object value, PixelDataType noun, PixelOperationType opType) {
    this.noun = noun;
    this.value = value;
    this.opType.add(opType);
  }

  public NounMetadata(Object value, PixelDataType noun, PixelOperationType... opType) {
    this.noun = noun;
    this.value = value;
    for (PixelOperationType op : opType) {
      this.opType.add(op);
    }
  }

  public NounMetadata(Object value, PixelDataType noun, List<PixelOperationType> opType) {
    this.noun = noun;
    this.value = value;
    this.opType.addAll(opType);
  }

  public Object getValue() {
    return this.value;
  }

  public PixelDataType getNounType() {
    return this.noun;
  }

  public List<PixelOperationType> getOpType() {
    return this.opType;
  }

  public void addAdditionalOpTypes(PixelOperationType... opType) {
    for (PixelOperationType op : opType) {
      this.opType.add(op);
    }
  }

  public void addAdditionalOpTypes(List<PixelOperationType> opType) {
    this.opType.addAll(opType);
  }

  public void addAdditionalReturn(NounMetadata noun) {
    this.additionalReturns.add(noun);
  }

  public void addAllAdditionalReturn(Collection<NounMetadata> nouns) {
    this.additionalReturns.addAll(nouns);
  }

  public List<NounMetadata> getAdditionalReturn() {
    return this.additionalReturns;
  }

  public void setExplanation(String explanation) {
    this.explanation = explanation;
  }

  public String getExplanation() {
    return this.explanation;
  }

  /** To help w/ debugging */
  public String toString() {
    return "NOUN META DATA ::: " + this.value + "";
  }

  public NounMetadata copy() {
    // I cannot copy a null noun
    if (this.noun == PixelDataType.NULL_VALUE) {
      return this;
    }

    Gson gson = GsonUtility.getDefaultGson();
    String str = gson.toJson(this);
    NounMetadata n = gson.fromJson(str, NounMetadata.class);
    return n;
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////

  /*
   * Static noun constructors
   */

  /**
   * Utility to get back a noun metadata with a warning message
   *
   * @param message
   * @param additionalOps The default op type will be warning but can add additional ones
   * @return
   */
  public static NounMetadata getSuccessNounMessage(
      String message, PixelOperationType... additionalOps) {
    NounMetadata noun =
        new NounMetadata(message, PixelDataType.CONST_STRING, PixelOperationType.SUCCESS);
    noun.addAdditionalOpTypes(additionalOps);
    return noun;
  }

  /**
   * Utility to get back a noun metadata with a warning message
   *
   * @param message
   * @param additionalOps The default op type will be warning but can add additional ones
   * @return
   */
  public static NounMetadata getWarningNounMessage(
      String message, PixelOperationType... additionalOps) {
    NounMetadata noun =
        new NounMetadata(message, PixelDataType.CONST_STRING, PixelOperationType.WARNING);
    noun.addAdditionalOpTypes(additionalOps);
    return noun;
  }

  /**
   * Utility to get back a noun metadata with an error message
   *
   * @param message
   * @param additionalOps The default op type will be error but can add additional ones
   * @return
   */
  public static NounMetadata getErrorNounMessage(
      String message, PixelOperationType... additionalOps) {
    NounMetadata noun = new NounMetadata(message, PixelDataType.ERROR, PixelOperationType.ERROR);
    noun.addAdditionalOpTypes(additionalOps);
    return noun;
  }

  /**
   * Utility to get back a noun metadata with an error message
   *
   * @param details
   * @param additionalOps The default op type will be error but can add additional ones
   * @return
   */
  public static NounMetadata getErrorNounMessage(Map details, PixelOperationType... additionalOps) {
    NounMetadata noun = new NounMetadata(details, PixelDataType.ERROR, PixelOperationType.ERROR);
    noun.addAdditionalOpTypes(additionalOps);
    return noun;
  }

  /**
   * @param object
   * @return
   */
  public static NounMetadata predictNounMetadata(Object object) {
    PixelDataType predictedType = null;
    if (object == null) {
      predictedType = PixelDataType.NULL_VALUE;
    } else if (object instanceof String) {
      predictedType = PixelDataType.CONST_STRING;
    } else if (object instanceof Integer || object instanceof Long) {
      predictedType = PixelDataType.CONST_INT;
    } else if (object instanceof Number) {
      predictedType = PixelDataType.CONST_DECIMAL;
    } else if (object instanceof Boolean) {
      predictedType = PixelDataType.BOOLEAN;
    } else if (object instanceof SemossDate) {
      predictedType = PixelDataType.CONST_DATE;
    } else if (object instanceof List) {
      predictedType = PixelDataType.VECTOR;
    } else if (object instanceof Map) {
      predictedType = PixelDataType.MAP;
    } else if (object instanceof PixelRunner) {
      predictedType = PixelDataType.PIXEL_RUNNER;
    } else {
      predictedType = PixelDataType.CUSTOM_DATA_STRUCTURE;
    }

    return new NounMetadata(object, predictedType);
  }
}
