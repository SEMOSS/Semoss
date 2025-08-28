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

public interface IPO850N1 extends IX12Format {

	String getN101();

	PO850N1 setN101(String n101);

	PO850N1 setEntityCode(String n101);

	String getN102();

	PO850N1 setN102(String n102);

	PO850N1 setName(String n102);

	String getN103();

	PO850N1 setN103(String n103);

	PO850N1 setIdentificationCodeQualifier(String n103);

	String getN104();

	PO850N1 setN104(String n104);

	PO850N1 setIdentificationCode(String n104);
}
