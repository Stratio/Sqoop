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
package org.apache.sqoop.connector.jdbc.oracle;

import org.apache.sqoop.connector.jdbc.oracle.util.OracleJdbcUrl;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleUtilities;
import org.apache.sqoop.connector.jdbc.oracle.util.OracleUtilities.JdbcOracleThinConnectionParsingError;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit tests for OraOopJdbcUrl.
 */
public class TestOracleJdbcUrl {

  @Test
  public void testParseJdbcOracleThinConnectionString() {

    OracleUtilities.JdbcOracleThinConnection actual;

    // Null JDBC URL...
    try {
      actual = new OracleJdbcUrl(null).parseJdbcOracleThinConnectionString();
      Assert.fail("An IllegalArgumentException should be been thrown.");
    } catch (IllegalArgumentException ex) {
      /* This is what we want to happen. */
    } catch (JdbcOracleThinConnectionParsingError ex) {
      Assert.fail("An IllegalArgumentException should be been thrown.");
    }

    // Empty JDBC URL...
    try {
      actual = new OracleJdbcUrl("").parseJdbcOracleThinConnectionString();
      Assert.fail("An IllegalArgumentException should be been thrown.");
    } catch (IllegalArgumentException ex) {
      /* This is what we want to happen. */
    } catch (JdbcOracleThinConnectionParsingError ex) {
      Assert.fail("An IllegalArgumentException should be been thrown.");
    }

    // Incorrect number of fragments in the URL...
    try {
      actual =
          new OracleJdbcUrl("jdbc:oracle:oci8:@dbname.domain")
              .parseJdbcOracleThinConnectionString();
      Assert.fail(
          "A JdbcOracleThinConnectionParsingError should be been thrown.");
    } catch (JdbcOracleThinConnectionParsingError ex) {
      // This is what we want to happen.
      Assert.assertTrue(
          ex.getMessage()
              .toLowerCase()
              .contains("the oracle \"thin\" jdbc driver is not being used."),
          "An exception should be thown that tells us there's an incorrect "
              + "number of fragments in the JDBC URL.");
    }

    // Incorrect driver-type (i.e. not using the "thin" driver)...
    try {
      actual =
          new OracleJdbcUrl(
              "jdbc:oracle:loremipsum:@hostname.domain.com.au:port1521:dbsid")
              .parseJdbcOracleThinConnectionString();
      Assert.fail(
          "A JdbcOracleThinConnectionParsingError should be been thrown.");
    } catch (JdbcOracleThinConnectionParsingError ex) {
      // This is what we want to happen.
      Assert.assertTrue(
          ex.getMessage().toLowerCase().contains(
              "oracle \"thin\" jdbc driver is not being used"),
          "An exception should be thown that refers to the fact that the thin "
          + "JDBC driver is not being used.");

      Assert.assertTrue(
          ex.getMessage().toLowerCase().contains("loremipsum"),
          "An exception should be thown that tells us which JDBC driver "
              + "was specified.");

    }

    // Invalid JDBC URL (unparsable port number)...
    try {
      actual =
          new OracleJdbcUrl(
              "jdbc:oracle:thin:@hostname.domain.com.au:port1521:dbsid")
              .parseJdbcOracleThinConnectionString();
      Assert.fail(
          "An JdbcOracleThinConnectionParsingError should be been thrown.");
    } catch (JdbcOracleThinConnectionParsingError ex) {
      Assert.assertTrue(
        ex.getMessage().toLowerCase().contains("port1521"),
        "The invalid port number should be included in the exception message.");
    }

    // Invalid JDBC URL (negative port number)...
    try {
      actual =
          new OracleJdbcUrl(
              "jdbc:oracle:thin:@hostname.domain.com.au:-1521:dbsid")
              .parseJdbcOracleThinConnectionString();
      Assert.fail(
          "An JdbcOracleThinConnectionParsingError should be been thrown.");
    } catch (JdbcOracleThinConnectionParsingError ex) {
      Assert.assertTrue(
        ex.getMessage().toLowerCase().contains("-1521"),
        "The invalid port number should be included in the exception message.");
    }

    // Valid JDBC URL...
    try {
      actual =
          new OracleJdbcUrl(
              "JDBC:Oracle:tHiN:@hostname.domain.com.au:1521:dbsid")
              .parseJdbcOracleThinConnectionString();
      Assert.assertEquals("hostname.domain.com.au", actual.getHost());
      Assert.assertEquals(1521, actual.getPort());
      Assert.assertEquals("dbsid", actual.getSid());
    } catch (JdbcOracleThinConnectionParsingError ex) {
      Assert.fail(ex.getMessage());
    }

    // Valid JDBC URL...
    try {
      actual =
          new OracleJdbcUrl(
              " JDBC : Oracle : tHiN : @hostname.domain.com.au : 1529 : dbsid")
              .parseJdbcOracleThinConnectionString();
      Assert.assertEquals("hostname.domain.com.au", actual.getHost());
      Assert.assertEquals(1529, actual.getPort());
      Assert.assertEquals("dbsid", actual.getSid());
    } catch (JdbcOracleThinConnectionParsingError ex) {
      Assert.fail(ex.getMessage());
    }

    // Valid (sid-based) JDBC URL with parameters...
    try {
      actual =
          new OracleJdbcUrl(
              "jdbc:oracle:thin:@hostname:1521:dbsid?param1=loremipsum")
              .parseJdbcOracleThinConnectionString();
      Assert.assertEquals("hostname", actual.getHost());
      Assert.assertEquals(1521, actual.getPort());
      Assert.assertEquals("dbsid", actual.getSid());
      Assert.assertEquals(null, actual.getService());
    } catch (JdbcOracleThinConnectionParsingError ex) {
      Assert.fail(ex.getMessage());
    }

    // Valid (service-based) JDBC URL...
    try {
      actual =
          new OracleJdbcUrl(
              "jdbc:oracle:thin:@hostname:1521/dbservice.dbdomain")
              .parseJdbcOracleThinConnectionString();
      Assert.assertEquals("hostname", actual.getHost());
      Assert.assertEquals(1521, actual.getPort());
      Assert.assertEquals(null, actual.getSid());
      Assert.assertEquals("dbservice.dbdomain", actual.getService());
    } catch (JdbcOracleThinConnectionParsingError ex) {
      Assert.fail(ex.getMessage());
    }

    // Valid (service-based) JDBC URL with slashes...
    try {
      actual =
          new OracleJdbcUrl(
              "jdbc:oracle:thin:@//hostname:1521/dbservice.dbdomain")
              .parseJdbcOracleThinConnectionString();
      Assert.assertEquals("hostname", actual.getHost());
      Assert.assertEquals(1521, actual.getPort());
      Assert.assertEquals(null, actual.getSid());
      Assert.assertEquals("dbservice.dbdomain", actual.getService());
    } catch (JdbcOracleThinConnectionParsingError ex) {
      Assert.fail(ex.getMessage());
    }

    // Valid (service-based) JDBC URL with parameters...
    try {
      actual = new OracleJdbcUrl(
         "jdbc:oracle:thin:@hostname:1521/dbservice.dbdomain?param1=loremipsum")
              .parseJdbcOracleThinConnectionString();
      Assert.assertEquals("hostname", actual.getHost());
      Assert.assertEquals(1521, actual.getPort());
      Assert.assertEquals(null, actual.getSid());
      Assert.assertEquals("dbservice.dbdomain", actual.getService());
    } catch (JdbcOracleThinConnectionParsingError ex) {
      Assert.fail(ex.getMessage());
    }

    // Valid (service-based) JDBC URL with slashes and parameters...
    try {
      actual = new OracleJdbcUrl(
       "jdbc:oracle:thin:@//hostname:1521/dbservice.dbdomain?param1=loremipsum")
              .parseJdbcOracleThinConnectionString();
      Assert.assertEquals("hostname", actual.getHost());
      Assert.assertEquals(1521, actual.getPort());
      Assert.assertEquals(null, actual.getSid());
      Assert.assertEquals("dbservice.dbdomain", actual.getService());
    } catch (JdbcOracleThinConnectionParsingError ex) {
      Assert.fail(ex.getMessage());
    }
  }

  @Test
  public void testGetConnectionUrl() {

    String actual;

    // Null JDBC URL...
    try {
      actual = new OracleJdbcUrl(null).getConnectionUrl();
      Assert.fail("An IllegalArgumentException should be been thrown.");
    } catch (IllegalArgumentException ex) {
      /* This is what we want to happen. */
    }

    // Empty JDBC URL...
    try {
      actual = new OracleJdbcUrl("").getConnectionUrl();
      Assert.fail("An IllegalArgumentException should be been thrown.");
    } catch (IllegalArgumentException ex) {
      /* This is what we want to happen. */
    }

    // JDBC URL...
    actual =
        new OracleJdbcUrl("jdbc:oracle:thin:@hostname.domain:1521:dbsid")
            .getConnectionUrl();
    Assert.assertEquals("jdbc:oracle:thin:@hostname.domain:1521:dbsid", actual);

  }

}
