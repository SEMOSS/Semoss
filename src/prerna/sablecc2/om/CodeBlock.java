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

import java.util.Hashtable;
import prerna.reactor.IReactor;
import prerna.util.Utility;

public class CodeBlock {

  // primary building block for the code to be assimilated
  // couple of things here
  // imports what are the things we need to import
  // The Language of code - is this java / R / Python
  // What are the selectors it is using for operation
  // What type of code is it - Map / Reduce or something else
  // What are the options for the code

  public enum LANG {
    JAVA,
    R,
    PYTHON
  };

  Hashtable<String, Object> options = new Hashtable<String, Object>();
  String imports = null;
  String code =
      null; // this is the code that will go into the map block or reduce block or something else
  LANG language = LANG.JAVA; // default yeah baby
  IReactor.TYPE type = IReactor.TYPE.MAP;
  String methodName = Utility.getRandomString(8);

  public void setLanguage(LANG language) {
    this.language = language;
  }

  public LANG getLanguage() {
    return this.language;
  }

  public void setType(IReactor.TYPE type) {
    this.type = type;
  }

  public void addImport(String importPackage) {
    if (imports == null) imports = importPackage;
    else imports = imports + importPackage;
  }

  public void setImports(String imports) {
    this.imports = imports;
  }

  public void addCode(String lineOfCode) {
    if (code == null) code = lineOfCode;
    else code = code + lineOfCode;
  }

  public void addOption(String key, Object value) {
    options.put(key, value);
  }

  public void setOptions(Hashtable<String, Object> options) {
    this.options = options;
  }

  public void setName(String methodName) {
    this.methodName = methodName;
  }

  public Class makeCode() {
    // this is the final call that makes the code to be executed
    return null;
  }

  public String[] getCode() {
    String[] retString = new String[2];
    String method = "public void run" + methodName + "(Object [] row) \n{";
    method = method + "\n" + code;
    method = method + "\n } \n";
    retString[0] = this.methodName;
    retString[1] = method;
    return retString;
  }
}
