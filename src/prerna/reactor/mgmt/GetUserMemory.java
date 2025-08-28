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
package prerna.reactor.mgmt;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetUserMemory extends AbstractReactor {

  @Override
  public NounMetadata execute() {
    // TODO Auto-generated method stub
    String message = "No user found";
    User user = this.insight.getUser();
    if (user != null) {
      Process rProcess = user.getrProcess();
      Process pyProcess = user.getPyProcess();

      long memory = 0;
      int rPid = 0;

      // get the pid
      if (rProcess != null) {
        rPid = MgmtUtil.getPidByPort(this.insight.getUser().getrPort());
        if (rPid != -1) memory = memory + MgmtUtil.memoryUtilizationPerProcess(rPid);
      }
      if (pyProcess != null) {
        int pid = MgmtUtil.getProcessID(pyProcess); // can replace this also at some point
        if (pid != rPid) {
          int pcPid = MgmtUtil.findChild(pid, "prerna.tcp.Server");
          memory = memory + MgmtUtil.memoryUtilizationPerProcess(pcPid);
        }
      }
      float mb = memory / (1024 * 1024);

      message = "Memory : " + mb + " MB";
    }
    return new NounMetadata(message, PixelDataType.CONST_STRING);
  }
}
