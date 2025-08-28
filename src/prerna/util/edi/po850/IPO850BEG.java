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

import java.time.LocalDateTime;
import prerna.util.edi.IX12Format;
import prerna.util.edi.po850.enums.PO850BEGQualifierIdEnum;

public interface IPO850BEG extends IX12Format {

  /**
   * @return
   */
  String getBeg01();

  /**
   * @param beg01
   * @return
   */
  IPO850BEG setBeg01(String beg01);

  /**
   * @param beg01
   * @return
   */
  IPO850BEG setTransactionPurposeCode(String beg01);

  /**
   * @return
   */
  public String getBeg02();

  /**
   * @param beg02
   * @return
   */
  IPO850BEG setBeg02(PO850BEGQualifierIdEnum beg02);

  /**
   * @param beg02
   * @return
   */
  IPO850BEG setPurchaseOrderTypeCode(PO850BEGQualifierIdEnum beg02);

  /**
   * @return
   */
  public String getBeg03();

  /**
   * @param beg03
   * @return
   */
  IPO850BEG setBeg03(String beg03);

  /**
   * @param beg03
   * @return
   */
  IPO850BEG setPurchaseOrderNumber(String beg03);

  //	/**
  //	 *
  //	 * @return
  //	 */
  //	String getBeg04();
  //
  //	/**
  //	 *
  //	 * @param beg04
  //	 * @return
  //	 */
  //	IPO850BEG setBeg04(String beg04);

  /**
   * @return
   */
  IPO850BEG setDateAndTime();

  /**
   * @param now
   * @return
   */
  IPO850BEG setDateAndTime(LocalDateTime now);

  /**
   * @return
   */
  public String getBeg05();

  /**
   * @param beg05
   * @return
   */
  IPO850BEG setBeg05(String beg05);

  /**
   * @param beg05
   * @return
   */
  IPO850BEG setDate(String beg05);
}
