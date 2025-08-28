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
package prerna.util.edi.po850;

import prerna.util.edi.IX12Format;

public interface IPO850PER extends IX12Format {

  /**
   * @return
   */
  String getPer01();

  /**
   * @param per01
   * @return
   */
  IPO850PER setPer01(String per01);

  /**
   * @param per01
   * @return
   */
  IPO850PER setContactFunctionCode(String per01);

  /**
   * @return
   */
  String getPer02();

  /**
   * @param per02
   * @return
   */
  IPO850PER setPer02(String per02);

  /**
   * @param per02
   * @return
   */
  IPO850PER setContactName(String per02);

  /**
   * @return
   */
  String getPer04();

  /**
   * @param per04
   * @return
   */
  IPO850PER setPer04(String per04);

  /**
   * @param per04
   * @return
   */
  IPO850PER setTelephone(String per04);

  /**
   * @return
   */
  String getPer06();

  /**
   * @param per06
   * @return
   */
  IPO850PER setPer06(String per06);

  /**
   * @param per06
   * @return
   */
  IPO850PER setEmail(String per06);
}
