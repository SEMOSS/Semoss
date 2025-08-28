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
package prerna.util.edi.impl.ghx.po850.writer.loop.po1loop;

import java.util.ArrayList;
import java.util.List;
import prerna.util.edi.IX12Format;
import prerna.util.edi.po850.enums.PO850PO1QualifierIdEnum;

public class PO850PO1Loop implements IX12Format {

	private List<PO850PO1Entity> po1list = new ArrayList<>();

	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {
		String builder = "";
		for (PO850PO1Entity loop : po1list) {
			builder += loop.generateX12(elementDelimiter, segmentDelimiter);
		}
		return builder;
	}

	public PO850PO1Loop addPO1(PO850PO1Entity po1) {
		po1list.add(po1);
		return this;
	}

	public List<PO850PO1Entity> getPO1List() {
		return this.po1list;
	}

	public int getNumSegments() {
		int counter = 0;
		for (PO850PO1Entity loop : po1list) {
			counter += loop.getNumSegments();
		}
		return counter;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		PO850PO1Loop po1loop = new PO850PO1Loop().addPO1(new PO850PO1Entity().setPo1(new PO850PO1().setUniqueId("1") // 1
																														// -
																														// unique
																														// id
				.setQuantityOrdered(10) // 2 - quantity ordered
				.setUnitOfMeasure("BX") // 3 - unit measurement
				.setUnitPrice(27.50) // 4 unit price (not total)
				.addQualifierAndValue(PO850PO1QualifierIdEnum.VC, "BXTS1040")
				.addQualifierAndValue(PO850PO1QualifierIdEnum.IN, "299176"))
				.setPid(new PO850PID().setItemDescription(
						"CARDINAL HEALTH™ WOUND CLOSURE STRIP, REINFORCED, 0.125 X 3IN, FOB (Destination), Manufacturer (CARDINAL HEALTH 200, LLC); BOX of 50")));

		System.out.println(po1loop.generateX12("^", "~\n"));
	}
}
