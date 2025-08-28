/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.testing;

public class PixelChain {

  private Class<?> c;
  private Object[] args;
  private String rawPixel;
  private boolean isRawPixel;

  public PixelChain(Class<?> c, Object... args) {
    this.c = c;
    this.args = args;
    this.isRawPixel = false;
  }

  public PixelChain(String rawPixel) {
    this.rawPixel = rawPixel;
    this.isRawPixel = true;
  }

  public Class<?> getC() {
    return c;
  }

  public void setC(Class<?> c) {
    this.c = c;
  }

  public Object[] getArgs() {
    return args;
  }

  public void setArgs(Object[] args) {
    this.args = args;
  }

  public String getRawPixel() {
    return rawPixel;
  }

  public void setRawPixel(String rawPixel) {
    this.rawPixel = rawPixel;
  }

  public boolean isRawPixel() {
    return isRawPixel;
  }

  public void setRawPixel(boolean isRawPixel) {
    this.isRawPixel = isRawPixel;
  }
}
