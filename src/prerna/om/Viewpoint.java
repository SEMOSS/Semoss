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

// main class for any types of view point
// product review
// could be a sentence
//

public class Viewpoint {

  // {data: text, id:id,user_id:user.screen_name, user_name: user.name,location:user.location,
  // rt:retweet_count, fav: favorite_count, followers: user.followers_count }";

  String review = null;
  String author = null;
  String authorId = null;
  String location = null;
  int repeatCount = 0; // this is the number of times it has been retweeted
  int favCount = 0; // how many people favorited
  float assignedRating = 0; // for amazon type reviews it says how much stars this user gave
  float derivedRating = 0; // derviced through sentiment analysis if needed
  int followerCount = 0;

  public String getReview() {
    return review;
  }

  public void setReview(String review) {
    this.review = review;
  }

  public String getAuthor() {
    return author;
  }

  public void setAuthor(String author) {
    this.author = author;
  }

  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  public int getRepeatCount() {
    return repeatCount;
  }

  public void setRepeatCount(int repeatCount) {
    this.repeatCount = repeatCount;
  }

  public int getFavCount() {
    return favCount;
  }

  public void setFavCount(int favCount) {
    this.favCount = favCount;
  }

  public float getAssignedRating() {
    return assignedRating;
  }

  public void setAssignedRating(float assignedRating) {
    this.assignedRating = assignedRating;
  }

  public float getDerivedRating() {
    return derivedRating;
  }

  public void setDerivedRating(float derivedRating) {
    this.derivedRating = derivedRating;
  }

  public int getFollowerCount() {
    return followerCount;
  }

  public void setFollowerCount(int followerCount) {
    this.followerCount = followerCount;
  }

  public String getAuthorId() {
    return authorId;
  }

  public void setAuthorId(String authorId) {
    this.authorId = authorId;
  }
}
