/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.nameserver;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.impl.BigDataEngine;
import prerna.util.DIHelper;

public class ModifyMasterDB implements IMasterDatabase{
	
	protected static final Logger logger = LogManager.getLogger(ModifyMasterDB.class.getName());

	protected String masterDBName = "MasterDatabase";
	protected BigDataEngine masterEngine;
	
	public ModifyMasterDB() {
		this.masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);
	}
	
	public ModifyMasterDB(String masterDBName) {
		this.masterDBName = masterDBName;
		this.masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);
	}
}
