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
package prerna.engine.api;

import java.util.Map;

public interface ISelectStatement extends IHeadersDataRow {

	public Object getVar(Object var);

	public void setVar(Object key, Object value);

	public void setRawVar(Object key, Object value);

	public Object getRawVar(Object var);

	public void setPropHash(Map propHash);

	public void setRPropHash(Map rawPropHash);

	public Map getPropHash();

	public Map getRPropHash();

	public boolean equals(Object other);

	public int hashCode();
}
