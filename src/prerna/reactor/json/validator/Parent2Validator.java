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
package prerna.reactor.json.validator;

import prerna.reactor.json.GreedyJsonReactor;

public class Parent2Validator extends GreedyJsonReactor {

  public Parent2Validator() {
    emailFields = new String[] {"email"};
    npiFields = new String[] {"npi"};
    phoneFields = new String[] {"phone"};
    ssnFields = new String[] {"ssn"};
    zipFields = new String[] {"zipcode"};
    mandatoryFields = new String[] {"somerandom", "somerandom2"};
  }

  // the method to implement here is validate
  // In our case the super parent will not throw any error
  public void process() {

    // shallow validation
    // do this only if you want to
    // if we want to do it for all.. I will move to the base method
    shallowValidate();

    // data validation
    // business rule validation
    System.out.println("parent value" + this.getValue("hello", true));

    // I will throw a random error here
    addError("Hello", "Child should behave like one.. right now it is not");
    // also add a stage here
    // addErrorWithStage("OMG", "Child should behave like one.. right now it is not",
    // "BRE_VALIDATION");
  }
}
