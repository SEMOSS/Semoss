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
package prerna.util.edi.impl.ghx.po850.writer.loop.n1loop;

import prerna.util.edi.IX12Format;

public class PO850N3 implements IX12Format {

	private String n301 = ""; // address information
	private String n302 = ""; // address information

	@Override
	public String generateX12(String elementDelimiter, String segmentDelimiter) {

		String builder = "N3" + elementDelimiter + n301 + elementDelimiter + n302 + segmentDelimiter;

		return builder;
	}

	// setters/getter

	public String getN301() {
		return n301;
	}

	public PO850N3 setN301(String n301) {
		this.n301 = n301;
		return this;
	}

	public PO850N3 setAddressInfo1(String n301) {
		return setN301(n301);
	}

	public String getN302() {
		return n302;
	}

	public PO850N3 setN302(String n302) {
		this.n302 = n302;
		return this;
	}

	public PO850N3 setAddressInfo2(String n302) {
		return setN302(n302);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		PO850N3 n3 = new PO850N3().setAddressInfo1("1201 N Muldoon Rd") // 1
		;

		System.out.println(n3.generateX12("^", "~\n"));
	}
}
