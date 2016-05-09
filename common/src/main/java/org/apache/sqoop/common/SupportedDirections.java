/*
 * Copyright (C) 2016 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sqoop.common;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;

/**
 * Represents which Directions are supported.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class SupportedDirections implements Comparable<SupportedDirections> {
  private static final char SUPPORTED_DIRECTIONS_SEPARATOR = '/';

  private boolean from;
  private boolean to;

  public SupportedDirections(boolean from, boolean to) {
    this.from = from;
    this.to = to;
  }

  /**
   * Check if direction is supported.
   * @param direction
   * @return boolean
   */
  public boolean isDirectionSupported(Direction direction) {
    return direction == Direction.FROM && from
        || direction == Direction.TO && to;
  }

  /**
   * @return String "FROM", "TO", "FROM/TO", "".
   */
  public String toString() {
    StringBuffer buffer = new StringBuffer();

    if (isDirectionSupported(Direction.FROM)) {
      buffer.append(Direction.FROM);

      if (isDirectionSupported(Direction.TO)) {
        buffer.append(SUPPORTED_DIRECTIONS_SEPARATOR);
        buffer.append(Direction.TO);
      }
    } else if (isDirectionSupported(Direction.TO)) {
      buffer.append(Direction.TO);
    }

    return buffer.toString();
  }

  public static SupportedDirections fromString(String supportedDirections) {
    boolean from = false, to = false;

    if (supportedDirections != null && !supportedDirections.equals("")) {
      for (String direction : supportedDirections.split("/")) {
        switch (Direction.valueOf(direction)) {
          case FROM:
            from = true;
            break;

          case TO:
            to = true;
            break;
        }
      }
    }

    return new SupportedDirections(from, to);
  }

  public static SupportedDirections fromDirection(Direction direction) {
    boolean from = false, to = false;
    switch (direction) {
      case FROM:
        from = true;
        break;

      case TO:
        to = true;
        break;
    }
    return new SupportedDirections(from, to);
  }

  @Override
  public int compareTo(SupportedDirections o) {
    int hash = 0;
    if (this.isDirectionSupported(Direction.FROM)) {
      hash |= 1;
    }
    if (this.isDirectionSupported(Direction.TO)) {
      hash |= 2;
    }

    int oHash = 0;
    if (this.isDirectionSupported(Direction.FROM)) {
      oHash |= 1;
    }
    if (this.isDirectionSupported(Direction.TO)) {
      oHash |= 2;
    }

    return hash - oHash;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    SupportedDirections that = (SupportedDirections) o;

    if (from != that.from) return false;
    return to == that.to;

  }

  @Override
  public int hashCode() {
    int result = (from ? 1 : 0);
    result = 31 * result + (to ? 1 : 0);
    return result;
  }
}
