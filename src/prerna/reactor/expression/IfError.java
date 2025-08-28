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
package prerna.reactor.expression;

import java.util.List;
import prerna.reactor.JavaExecutable;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class IfError extends OpBasic {

  @Override
  protected NounMetadata evaluate(Object[] values) {
    return this.nouns[0];
  }

  // TODO: implement this, just returning 1 for now
  public String getJavaSignature() {

    StringBuilder javaSignature = new StringBuilder();
    List<NounMetadata> inputs = getJavaInputs();
    NounMetadata tryInput = inputs.get(0);

    Object tryObj = tryInput.getValue();
    String tryString;

    if (tryObj instanceof JavaExecutable) {
      tryString = ((JavaExecutable) tryObj).getJavaSignature();
    } else if (tryInput.getNounType() == PixelDataType.CONST_DECIMAL) {
      tryString = tryObj.toString();
    } else if (tryInput.getNounType() == PixelDataType.CONST_STRING) {
      tryString = "\"" + tryObj.toString() + "\"";
    } else {
      tryString = tryObj.toString();
    }

    NounMetadata defaultInput = inputs.get(1);
    Object defaultObj = tryInput.getValue();
    String defaultString;

    if (defaultObj instanceof JavaExecutable) {
      defaultString = ((JavaExecutable) tryObj).getJavaSignature();

    } else if (defaultInput.getNounType() == PixelDataType.CONST_DECIMAL) {

    } else if (defaultInput.getNounType() == PixelDataType.CONST_STRING) {

    } else {

    }

    //		return javaSignature.toString();
    return "1";
  }

  @Override
  public String getReturnType() {
    return "double";
  }
}
