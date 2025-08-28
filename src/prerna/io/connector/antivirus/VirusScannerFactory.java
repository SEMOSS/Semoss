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
package prerna.io.connector.antivirus;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.io.connector.antivirus.clamav.ClamAVScannerUtils;
import prerna.io.connector.antivirus.tika.ApacheTikaScannerUtils;
import prerna.io.connector.antivirus.virustotal.VirusTotalScannerUtils;
import prerna.util.Constants;
import prerna.util.Utility;

public class VirusScannerFactory {

  private static final Logger logger = LogManager.getLogger(VirusScannerFactory.class);

  private VirusScannerFactory() {}

  public static IVirusScanner getVirusScannerConnector() {
    if (Utility.isVirusScanningDisabled()) {
      return null;
    }
    String scanType = Utility.getDIHelperProperty(Constants.VIRUS_SCANNING_METHOD).toUpperCase();

    if (scanType.equals(IVirusScanner.VIRUS_SCANNER_TYPE.CLAM_AV.toString())) {
      return ClamAVScannerUtils.getInstance();
    } else if (scanType.equals(IVirusScanner.VIRUS_SCANNER_TYPE.VIRUS_TOTAL.toString())) {
      return VirusTotalScannerUtils.getInstance();
    } else if (scanType.equals(IVirusScanner.VIRUS_SCANNER_TYPE.APACHE_TIKA.toString())) {
      return new ApacheTikaScannerUtils();
    } else if (scanType.equalsIgnoreCase(IVirusScanner.CLAM_AV)) {
      logger.warn(
          "Using deprecated value - please update parameter value for "
              + Constants.VIRUS_SCANNING_METHOD
              + "to CLAM_AV");
      logger.warn(
          "Using deprecated value - please update parameter value for "
              + Constants.VIRUS_SCANNING_METHOD
              + "to CLAM_AV");
      logger.warn(
          "Using deprecated value - please update parameter value for "
              + Constants.VIRUS_SCANNING_METHOD
              + "to CLAM_AV");
      logger.warn(
          "Using deprecated value - please update parameter value for "
              + Constants.VIRUS_SCANNING_METHOD
              + "to CLAM_AV");
      logger.warn(
          "Using deprecated value - please update parameter value for "
              + Constants.VIRUS_SCANNING_METHOD
              + "to CLAM_AV");
      return ClamAVScannerUtils.getInstance();
    } else {
      logger.warn(
          "Virus Scanning is enabled but could not find type for input = '" + scanType + "'");
      return null;
    }
  }
}
