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
package prerna.ds.export.gexf;

public interface IGexfIterator {

	/**
	 * Get the start string for the gexf file
	 *
	 * @return
	 */
	String getStartString();

	/**
	 * Get the end string for the gexf file
	 *
	 * @return
	 */
	String getEndString();

	/**
	 * Get the start string for the node portion of the gexf file
	 *
	 * @return
	 */
	String getNodeStart();

	/**
	 * Get the end string for the node portion of the gexf file
	 *
	 * @return
	 */
	String getNodeEnd();

	/**
	 * Get the start string for the edge portion of the gexf file
	 *
	 * @return
	 */
	String getEdgeStart();

	/**
	 * Get the end string for the edge portion of the gexf file
	 *
	 * @return
	 */
	String getEdgeEnd();

	/**
	 * Boolean if there are more nodes to output
	 *
	 * @return
	 */
	boolean hasNextNode();

	/**
	 * String containing the next node with its properties
	 *
	 * @return
	 */
	String getNextNodeString();

	/**
	 * Boolean if there are more edges to output
	 *
	 * @return
	 */
	boolean hasNextEdge();

	/**
	 * String containing the next edge with its properties
	 *
	 * @return
	 */
	String getNextEdgeString();
}
