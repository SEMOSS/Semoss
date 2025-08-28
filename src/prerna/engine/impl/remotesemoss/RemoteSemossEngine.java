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
package prerna.engine.impl.remotesemoss;

import java.util.Vector;
import prerna.engine.impl.AbstractDatabaseEngine;
import prerna.util.Constants;

// TODO >>>timb: REST - either replace with rest remote or remove this
public class RemoteSemossEngine extends AbstractDatabaseEngine {

  private String remoteAddress;

  @Override
  public void open(String smssFilePath) {
    setSmssFilePath(smssFilePath);
    // get id & name
    this.engineId = this.smssProp.getProperty(Constants.ENGINE);
    this.engineName = this.smssProp.getProperty(Constants.ENGINE_ALIAS);
    this.remoteAddress = this.smssProp.getProperty("REMOTE_ADDRESS");
  }

  @Override
  public Object execQuery(String query) {
    // TODO Auto-generated method stub
    return null;
  }

  //	@Override
  //	public Vector<Insight> getInsight(String... questionIDs) {
  //		System.out.println("need to implement this in rest");
  //		return null;
  //	}

  @Override
  public void insertData(String query) {
    System.out.println("need to implement this in rest");
  }

  @Override
  public void removeData(String query) {
    System.out.println("need to implement this in rest");
  }

  @Override
  public void commit() {
    System.out.println("need to implement this in rest");
  }

  @Override
  public boolean holdsFileLocks() {
    return false;
  }

  @Override
  public DATABASE_TYPE getDatabaseType() {
    return DATABASE_TYPE.REMOTE_SEMOSS;
  }

  //////////////////////////////////////////////////////

  // I am not sure this is really used much...
  // need to check some reactors that touch the engines directly
  // like x-ray

  @Override
  public Vector<Object> getEntityOfType(String type) {
    // TODO Auto-generated method stub
    return null;
  }
}
