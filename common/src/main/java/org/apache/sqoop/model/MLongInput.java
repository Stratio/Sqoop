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
package org.apache.sqoop.model;

import org.apache.sqoop.classification.InterfaceAudience;
import org.apache.sqoop.classification.InterfaceStability;

import java.util.List;

/**
 * Long user input.
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class MLongInput extends MInput<Long> {

  public MLongInput(String name, boolean sensitive, InputEditable editable, String overrides, List<MValidator> mValidators) {
    super(name, sensitive, editable, overrides, mValidators);
  }

  @Override
  public String getUrlSafeValueString() {
    if(isEmpty()) {
      return "";
    }

    return getValue().toString();
  }

  @Override
  public void restoreFromUrlSafeValueString(String valueString) {
    if(valueString.isEmpty()) {
      setEmpty();
    }

    setValue(Long.valueOf(valueString));
  }

  @Override
  public MInputType getType() {
    return MInputType.LONG;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }

    if (!(other instanceof MLongInput)) {
      return false;
    }

    MLongInput i = (MLongInput) other;
    return getName().equals(i.getName());
  }

  @Override
  public int hashCode() {
    return 23 + 31 * getName().hashCode();
  }

  @Override
  public boolean isEmpty() {
    return getValue() == null;
  }

  @Override
  public void setEmpty() {
    setValue(null);
  }

  @Override
  public MLongInput clone(boolean cloneWithValue) {
    MLongInput copy = new MLongInput(getName(), isSensitive(), getEditable(), getOverrides(), getCloneOfValidators());
    copy.setPersistenceId(getPersistenceId());
    if(cloneWithValue) {
      copy.setValue(this.getValue());
    }
    return copy;
  }
}
