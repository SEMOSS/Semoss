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
package prerna.util.edi.po850.enums;

public enum PO850BEGQualifierIdEnum {
	NE("NE", "NEW ORDER"), ST("ST", "STANDING ORDER"), SA("SA", "STANDALONE PO"), BK("BK", "BLANKET PO"),;

	private String id;
	// this is not to be used, just for human readability
	private String description;

	PO850BEGQualifierIdEnum(String id, String description) {
		this.id = id;
		this.description = description;
	}

	public String getId() {
		return this.id;
	}
}
