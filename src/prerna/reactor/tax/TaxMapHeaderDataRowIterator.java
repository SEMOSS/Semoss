/***************************************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components: Licensed
 * under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ***************************************************************************************************/
package prerna.reactor.tax;
// package prerna.sablecc2.reactor.storage;
//
// import java.util.Hashtable;
// import java.util.Iterator;
// import java.util.Map;
//
// import prerna.engine.api.IHeadersDataRow;
// import prerna.engine.impl.rdf.HeadersDataRow;
// import prerna.sablecc2.om.InMemStore;
// import prerna.sablecc2.om.NounMetadata;
// import prerna.sablecc2.om.TaxMapStore;
//
// public class TaxMapHeaderDataRowIterator implements
// Iterator<IHeadersDataRow>{
//
// private TaxMapStore store = null;
// private Iterator<String> keysIterator = null;
//
// public TaxMapHeaderDataRowIterator(TaxMapStore store) {
// this.store = store;
// this.keysIterator = store.getKeys().iterator();
// }
//
// @Override
// public boolean hasNext() {
// return keysIterator.hasNext();
// }
//
// @Override
// public IHeadersDataRow next() {
// // scenario name
// String key = keysIterator.next();
// // scenario in-mem map
// InMemStore<String, NounMetadata> scenarioMap = (InMemStore<String,
// NounMetadata>)store.get(key).getValue();
//
// // loop through the scenario keys and flush out to map
// Map<Object, Object> scenarioValues = new Hashtable<Object, Object>();
// Iterator<String> scenarioKeysIt = scenarioMap.getKeys().iterator();
// while(scenarioKeysIt.hasNext()) {
// String scenarioKey = scenarioKeysIt.next();
// scenarioValues.put(scenarioKey, scenarioMap.get(scenarioKey).getValue());
// }
//
// String[] header = new String[]{key.toString()};
// Object[] data = new Object[]{scenarioValues};
// return new HeadersDataRow(header, data, data);
// }
//
// }
