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
package org.apache.sqoop.connector.hdfs.security;

import org.apache.hadoop.io.Text;
import org.testng.annotations.Test;
import org.apache.hadoop.security.token.Token;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestSecurityUtils {

  @Test
  public void testTokenSerializationDeserialization() throws Exception {
    byte[] identifier = "identifier".getBytes();
    byte[] password = "password".getBytes();
    Text kind = new Text("kind");
    Text service = new Text("service");

    Token token = new Token(identifier, password, kind, service);
    String serializedForm = SecurityUtils.serializeToken(token);
    assertNotNull(serializedForm);

    Token deserializedToken = SecurityUtils.deserializeToken(serializedForm);
    assertNotNull(deserializedToken);

    assertEquals(identifier, deserializedToken.getIdentifier());
    assertEquals(password, deserializedToken.getPassword());
    assertEquals(kind.toString(), deserializedToken.getKind().toString());
    assertEquals(service.toString(), deserializedToken.getService().toString());
  }

}
