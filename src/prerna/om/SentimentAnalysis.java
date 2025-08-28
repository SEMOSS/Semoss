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
package prerna.om;

public class SentimentAnalysis {

  private String sentence;
  private double magnitude;
  private double score;

  public SentimentAnalysis() {}

  /*
   * This is just a struct
   * Define setters and getters for the class variables
   */

  public String getSentence() {
    return sentence;
  }

  public void setSentence(String sentence) {
    this.sentence = sentence;
  }

  public double getMagnitude() {
    return magnitude;
  }

  public void setMagnitude(double magnitude) {
    this.magnitude = magnitude;
  }

  public double getScore() {
    return score;
  }

  public void setScore(double score) {
    this.score = score;
  }
}
