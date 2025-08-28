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
package prerna.reactor.masterdatabase;
// package prerna.sablecc2.reactor.masterdatabase;
//
// import java.util.List;
//
// import prerna.auth.utils.AbstractSecurityUtils;
// import prerna.auth.utils.SecurityAppUtils;
// import prerna.auth.utils.SecurityQueryUtils;
// import prerna.nameserver.utility.MasterDatabaseUtility;
// import prerna.sablecc2.om.GenRowStruct;
// import prerna.sablecc2.om.PixelDataType;
// import prerna.sablecc2.om.PixelOperationType;
// import prerna.sablecc2.om.ReactorKeysEnum;
// import prerna.sablecc2.om.nounmeta.NounMetadata;
// import prerna.sablecc2.reactor.AbstractReactor;
//
// public class GetLogicalNamesReactor extends AbstractReactor {
//
// public GetLogicalNamesReactor() {
// this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(),
// ReactorKeysEnum.CONCEPT.getKey()};
// }
//
// @Override
// public NounMetadata execute() {
// String engineId = getEngineId();
//
// if(AbstractSecurityUtils.securityEnabled()) {
// engineId =
// SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(),
// engineId);
// if(!SecurityAppUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
// throw new IllegalArgumentException("App does not exist or user does not have
// access to edit
// database");
// }
// } else {
// engineId = MasterDatabaseUtility.testEngineIdIfAlias(engineId);
// }
//
// if(!SecurityQueryUtils.getEngineIds().contains(engineId)) {
// throw new IllegalArgumentException("App id does not exist");
// }
//
// String concept = getConcept();
// List<String> logicalNames = MasterDatabaseUtility.getLogicalNames(engineId,
// concept);
// return new NounMetadata(logicalNames, PixelDataType.CUSTOM_DATA_STRUCTURE,
// PixelOperationType.ENTITY_LOGICAL_NAMES);
// }
//
// ///////////////////////////////////////////////////////////
// ///////////////////////////////////////////////////////////
// ///////////// GRAB INPUTS FROM PIXEL REACTOR //////////////
// ///////////////////////////////////////////////////////////
// ///////////////////////////////////////////////////////////
//
// private String getEngineId() {
// GenRowStruct instanceGrs = this.store.getNoun(keysToGet[0]);
// if (instanceGrs != null && !instanceGrs.isEmpty()) {
// String engine = (String) instanceGrs.get(0);
// if (engine.length() > 0) {
// return engine;
// }
// }
// throw new IllegalArgumentException("Need to define " + keysToGet[0]);
// }
//
// private String getConcept() {
// GenRowStruct instanceGrs = this.store.getNoun(keysToGet[1]);
// if (instanceGrs != null && !instanceGrs.isEmpty()) {
// String concept = (String) instanceGrs.get(0);
// if (concept.length() > 0) {
// return concept;
// }
// }
// throw new IllegalArgumentException("Need to define " + keysToGet[1]);
// }
//
// }
