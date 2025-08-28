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

public interface IPO850CTT extends IX12Format {

	/**
	 * @return
	 */
	int getCtt01();

	/**
	 * @param ctt01
	 * @return
	 */
	IPO850CTT setCtt01(int ctt01);

	/**
	 * @param ctt01
	 * @return
	 */
	IPO850CTT setNumPO1Segments(int ctt01);

	/**
	 * @return
	 */
	int getCtt02();

	/**
	 * @param ctt02
	 * @return
	 */
	IPO850CTT setCtt02(int ctt02);

	/**
	 * @param ctt02
	 * @return
	 */
	IPO850CTT setSumPO102Qualities(int ctt02);
}
