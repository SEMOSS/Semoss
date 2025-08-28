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
package prerna.sablecc2.comm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Vector;
import prerna.om.Insight;
import prerna.sablecc2.PixelRunner;

public class PixelJobManager {

  static PixelJobManager manager = new PixelJobManager();

  // obviously I assume the user wont run that many jobs to start with
  // I will adjust this to a random number generator later

  // hashtable of job id to stdOut messages
  private Map<String, List<String>> jobStdOut = new Hashtable<>();

  // hashtable of job id to offset
  private Map<String, Integer> stdOutOffset = new Hashtable<>();

  // hashtable of job id to error messages
  private Map<String, List<String>> jobError = new Hashtable<>();

  // hashtable of job id to offset
  private Map<String, Integer> errorOffset = new Hashtable<>();

  // output offset - this will eventually be needed for distributed processing
  private Map<String, Integer> outputOffset = new Hashtable<>();

  // keeps the job to thread
  private Map<String, PixelJobThread> threadPool = new Hashtable<>();

  // hashtable of job id to stdOut messages
  private Map<String, StringBuilder> jobPartialOut = new Hashtable<>();

  // hashtable of job id to offset
  private Map<String, Integer> jobPartialOutOffset = new Hashtable<>();

  private PixelJobManager() {}

  public static PixelJobManager getManager() {
    if (manager != null) {
      return manager;
    }

    synchronized (PixelJobManager.class) {
      if (manager == null) {
        manager = new PixelJobManager();
      }
    }
    return manager;
  }

  public PixelJobThread makeJob(Insight insight, String sessionId, String routeId) {
    String jobId = UUID.randomUUID().toString();
    PixelJobThread jt = new PixelJobThread(jobId, insight, sessionId, routeId);
    threadPool.put(jobId, jt);
    return jt;
  }

  public PixelJobThread makeJob(String jobId, Insight insight, String sessionId, String routeId) {
    PixelJobThread jt = new PixelJobThread(jobId, insight, sessionId, routeId);
    threadPool.put(jobId, jt);
    return jt;
  }

  public PixelJobThread removeJob(String jobId) {
    return threadPool.remove(jobId);
  }

  public PixelJobThread getJob(String jobId) {
    return threadPool.get(jobId);
  }

  public void addStdOut(String jobId, String stdOut) {
    List<String> outputList = new Vector<String>();
    if (jobStdOut.containsKey(jobId)) {
      outputList = jobStdOut.get(jobId);
    } else {
      jobStdOut.put(jobId, outputList);
    }
    synchronized (outputList) {
      outputList.add(stdOut);
    }
  }

  public void addStdErr(String jobId, String stdErr) {
    List<String> outputList = new Vector<String>();
    if (jobError.containsKey(jobId)) {
      outputList = jobError.get(jobId);
    } else {
      jobError.put(jobId, outputList);
    }
    synchronized (outputList) {
      outputList.add(stdErr);
    }
  }

  public List<String> getStdOut(String jobId) {
    int curOffset = 0;
    if (stdOutOffset.containsKey(jobId)) {
      curOffset = stdOutOffset.get(jobId);
    }
    return getStdOut(jobId, curOffset);
  }

  public List<String> getError(String jobId) {
    int curOffset = 0;
    if (errorOffset.containsKey(jobId)) {
      curOffset = errorOffset.get(jobId);
    }
    return getError(jobId, curOffset);
  }

  public List<String> getStdOut(String jobId, int offset) {
    List<String> outputList = jobStdOut.get(jobId);
    if (outputList == null || outputList.isEmpty()) {
      return new ArrayList<String>();
    }
    synchronized (outputList) {
      int size = outputList.size();
      List<String> output = new Vector<String>(outputList.subList(offset, size));
      int newOffset = offset + output.size();
      // update the offset
      stdOutOffset.put(jobId, newOffset);
      return output;
    }
  }

  public List<String> getError(String jobId, int offset) {
    List<String> outputList = jobError.get(jobId);
    if (outputList == null || outputList.isEmpty()) {
      return new ArrayList<String>();
    }
    synchronized (outputList) {
      int size = outputList.size();
      List<String> output = new Vector<String>(outputList.subList(offset, size));
      int newOffset = offset + output.size();
      // update the offset
      errorOffset.put(jobId, newOffset);
      return output;
    }
  }

  public void addPartialOut(String jobId, String stdOut) {
    StringBuilder builder = null;
    if (jobPartialOut.containsKey(jobId)) {
      builder = jobPartialOut.get(jobId);
    } else {
      builder = new StringBuilder();
      jobPartialOut.put(jobId, builder);
    }
    synchronized (builder) {
      builder.append(stdOut);
    }
  }

  public Map<String, String> getPartial(String jobId) {
    int curOffset = 0;
    if (jobPartialOutOffset.containsKey(jobId)) {
      curOffset = jobPartialOutOffset.get(jobId);
    }
    return getPartial(jobId, curOffset);
  }

  public Map<String, String> getPartial(String jobId, int offset) {
    StringBuilder builder = jobPartialOut.get(jobId);
    if (builder == null || builder.length() == 0) {
      return new HashMap<>();
    }
    synchronized (builder) {
      Map<String, String> retMap = new HashMap<>();
      int size = builder.length();
      retMap.put("total", builder.toString());
      String newMessage = builder.substring(offset, size);
      retMap.put("new", newMessage);
      // update the offset
      int newOffset = size;
      jobPartialOutOffset.put(jobId, newOffset);
      return retMap;
    }
  }

  public String getStatus(String jobId) {
    return threadPool.get(jobId).getStatus();
  }

  public void resetJob(String jobId) // trimming operation
      {
    // this is when I want to remove everything until current offset and set offset to zero
    int curOut = errorOffset.get(jobId);
    int curErr = stdOutOffset.get(jobId);

    List<String> errorList = jobError.get(jobId);
    List<String> outputList = jobStdOut.get(jobId);

    // trim the error list
    synchronized (errorList) {
      errorList = new Vector<String>(errorList.subList(curErr, errorList.size() - 1));
      jobError.put(jobId, errorList);
      errorOffset.put(jobId, 0);
    }

    // trim the outputlist
    synchronized (outputList) {
      outputList = new Vector<String>(outputList.subList(curOut, outputList.size() - 1));
      jobStdOut.put(jobId, outputList);
      stdOutOffset.put(jobId, 0);
    }
  }

  public void clearJob(String jobId) {
    jobError.remove(jobId);
    stdOutOffset.remove(jobId);
    errorOffset.remove(jobId);
    jobStdOut.remove(jobId);

    // partial as well
    jobPartialOut.remove(jobId);
    jobPartialOutOffset.remove(jobId);
  }

  public void flagStatus(String jobId, PixelJobStatus status) {
    threadPool.get(jobId).setStatus(status);
  }

  public void interruptThread(String jobId) {
    if (threadPool.get(jobId) != null) {
      PixelJobThread pixelThread = threadPool.get(jobId);
      pixelThread.interrupt();
      pixelThread.setStatus(PixelJobStatus.CANCELED);
    }
  }

  public PixelRunner getOutput(String jobId) {
    PixelJobThread jt = threadPool.get(jobId);
    return jt.getRunner();
  }
}
