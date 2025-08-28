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
package prerna.util.edi.po850;

import java.util.List;
import prerna.util.edi.IPO850FunctionalGroup;
import prerna.util.edi.IPO850IEA;
import prerna.util.edi.IPO850ISA;
import prerna.util.edi.IX12Format;

public interface IPO850 extends IX12Format {

	/**
	 * Get the element delimiter
	 *
	 * @return
	 */
	String getElementDelimiter();

	/**
	 * Set the element delimiter
	 *
	 * @param elementDelimiter
	 * @return
	 */
	IPO850 setElementDelimiter(String elementDelimiter);

	/**
	 * Get the segment delimiter
	 *
	 * @return
	 */
	String getSegmentDelimiter();

	/**
	 * Set the segment delimiter
	 *
	 * @param segmentDelimiter
	 * @return
	 */
	IPO850 setSegmentDelimiter(String segmentDelimiter);

	/**
	 * Add a functional group to the 850
	 *
	 * @param fg
	 * @return
	 */
	IPO850 addFunctionalGroup(IPO850FunctionalGroup fg);

	/**
	 * Get the list of functional groups
	 *
	 * @return
	 */
	List<IPO850FunctionalGroup> getFunctionalGroups();

	/**
	 * @return
	 */
	IPO850ISA getIsa();

	/**
	 * @param isa
	 * @return
	 */
	IPO850 setIsa(IPO850ISA isa);

	/**
	 * @return
	 */
	IPO850IEA getIea();

	/**
	 * @param iea
	 * @return
	 */
	IPO850 setIea(IPO850IEA iea);
}
