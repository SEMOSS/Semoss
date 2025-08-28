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
package prerna.util.edi;

public interface IPO850IEA extends IX12Format {

  /**
   * @return
   */
  String getIea01();

  /**
   * @param iea01
   * @return
   */
  IPO850IEA setIea01(String iea01);

  /**
   * @param iea01
   * @return
   */
  IPO850IEA setTotalGroups(String iea01);

  /**
   * @return
   */
  String getIea02();

  /**
   * @param iea02
   * @return
   */
  IPO850IEA setIea02(String iea02);

  /**
   * @param iea02
   * @return
   */
  IPO850IEA setInterchangeControlNumber(String iea02);
}
