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
package prerna.reactor.planner.graph;
// package prerna.sablecc2.reactor.planner.graph;
//
// import java.util.List;
// import java.util.Vector;
//
// import org.apache.log4j.LogManager;
// import org.apache.log4j.Logger;
//
// import prerna.sablecc2.GreedyTranslation;
// import prerna.sablecc2.PkslUtility;
// import prerna.sablecc2.om.GenRowStruct;
// import prerna.sablecc2.om.NounMetadata;
// import prerna.sablecc2.om.PkslDataTypes;
// import prerna.sablecc2.reactor.PKSLPlanner;
//
// public class ExecuteGraphPlannerReactor extends AbstractPlannerReactor {
//
// private static final Logger LOGGER =
// LogManager.getLogger(ExecuteGraphPlannerReactor.class.getName());
//
// @Override
// public NounMetadata execute()
// {
// long start = System.currentTimeMillis();
//
// PKSLPlanner planner = getPlanner();
// List<String> pksls = new Vector<String>();
//
// // using the root vertices
// // iterate down all the other vertices and add the signatures
// // for the desired travels in the appropriate order
// // note: this is adding to the list of undefined variables
// // calculated at beginning of class
// traverseDownstreamVertsProcessor(planner, pksls);
//
// GreedyTranslation translation = new GreedyTranslation();
// translation.planner = planner;
// PkslUtility.addPkslToTranslation(translation, pksls);
//
// long end = System.currentTimeMillis();
// LOGGER.info("**************** END RUN PLANNER "+(end - start)+"ms
// *************************");
//
// return new NounMetadata(translation.planner, PkslDataTypes.PLANNER);
// }
//
// private PKSLPlanner getPlanner() {
// GenRowStruct allNouns =
// getNounStore().getNoun(PkslDataTypes.PLANNER.toString());
// PKSLPlanner planner = null;
// if(allNouns != null) {
// planner = (PKSLPlanner) allNouns.get(0);
// return planner;
// } else {
// return this.planner;
// }
// }
// }
