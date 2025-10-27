/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.engine.api;

import java.util.Iterator;

/**
 * Wrapper interface for handling SPARQL CONSTRUCT query results from RDF databases.
 * 
 * <p>This interface extends both {@link IEngineWrapper} and {@link Iterator} to provide
 * a unified way to iterate through RDF triple statements returned by CONSTRUCT queries.
 * CONSTRUCT queries in SPARQL are used to create new RDF graphs based on patterns
 * matched in the source data.</p>
 * 
 * <p>The wrapper provides streaming access to constructed RDF statements, allowing
 * efficient processing of large result sets without loading all triples into memory
 * at once. Each iteration returns an {@link IConstructStatement} representing a
 * subject-predicate-object triple.</p>
 * 
 * <p>Typical usage pattern:</p>
 * <pre>
 * IConstructWrapper wrapper = rdfEngine.construct(constructQuery);
 * while (wrapper.hasNext()) {
 *     IConstructStatement statement = wrapper.next();
 *     // Process the RDF triple
 * }
 * </pre>
 * 
 * @see {@link IConstructStatement} for RDF triple representation
 * @see {@link IEngineWrapper} for base wrapper functionality
 * @see {@link IRDFDatabase} for RDF database operations
 * @author SEMOSS
 */
public interface IConstructWrapper extends IEngineWrapper, Iterator{
	
	/**
	 * Returns the next RDF construct statement in the iteration.
	 * 
	 * <p>This method overrides the generic {@link Iterator#next()} method to
	 * provide a more specific return type of {@link IConstructStatement} rather
	 * than a generic Object. This ensures type safety when iterating through
	 * CONSTRUCT query results.</p>
	 * 
	 * @return The next {@link IConstructStatement} in the result set
	 * @throws java.util.NoSuchElementException If there are no more elements
	 * @see {@link IConstructStatement} for RDF triple structure
	 */
	public IConstructStatement next();

}
