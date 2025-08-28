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
package prerna.reactor.export;

public class FormatFactory {

	public static IFormatter getFormatter(String formatType) {

		switch (formatType.toUpperCase()) {
			case TableFormatter.FORMAT_TYPE : {
				return new TableFormatter();
			}

			case "GRID" : {
				return new TableFormatter();
			}

			case "GRAPH" : {
				return new GraphFormatter();
			}

			case "JSON" : {
				return new JsonFormatter();
			}

			case "KEYVALUE" : {
				return new KeyValueFormatter();
			}

			case "CLUSTERGRAMMAR" : {
				return new ClustergramFormatter();
			}

			case "CLUSTERGRAM" : {
				return new ClustergramFormatter();
			}

			case "HIERARCHY" : {
				return new HierarchyFormatter();
			}

			default : {
				return new TableFormatter();
			}
		}
	}
}
