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
package org.apache.sqoop.utils;

import static org.apache.sqoop.utils.ConnectorClassLoader.constructUrlsFromClasspath;
import static org.apache.sqoop.utils.ConnectorClassLoader.isSystemClass;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

public class TestConnectorClassLoader {
  private static File testDir = new File(System.getProperty("maven.build.directory",
      System.getProperty("java.io.tmpdir")), "connectorclassloader");

  @BeforeMethod(alwaysRun = true)
  public void setUp() {
    FileUtils.deleteQuietly(testDir);
    testDir.mkdirs();
  }

  @Test
  public void testConstructUrlsFromClasspath() throws Exception {
    File file = new File(testDir, "file");
    assertTrue(file.createNewFile(), "Create file");

    File dir = new File(testDir, "dir");
    assertTrue(dir.mkdir(), "Make dir");

    File jarsDir = new File(testDir, "jarsdir");
    assertTrue(jarsDir.mkdir(), "Make jarsDir");
    File nonJarFile = new File(jarsDir, "nonjar");
    assertTrue(nonJarFile.createNewFile(), "Create non-jar file");
    File jarFile = new File(jarsDir, "a.jar");
    assertTrue(jarFile.createNewFile(), "Create jar file");

    File nofile = new File(testDir, "nofile");
    // don't create nofile

    StringBuilder cp = new StringBuilder();
    cp.append(file.getAbsolutePath()).append(File.pathSeparator)
      .append(dir.getAbsolutePath()).append(File.pathSeparator)
      .append(jarsDir.getAbsolutePath() + "/*").append(File.pathSeparator)
      .append(nofile.getAbsolutePath()).append(File.pathSeparator)
      .append(nofile.getAbsolutePath() + "/*").append(File.pathSeparator);

    URL[] urls = constructUrlsFromClasspath(cp.toString());

    assertEquals(3, urls.length);
    assertEquals(file.toURI().toURL(), urls[0]);
    assertEquals(dir.toURI().toURL(), urls[1]);
    assertEquals(jarFile.toURI().toURL(), urls[2]);
    // nofile should be ignored
  }

  @Test
  public void testIsSystemClass() {
    testIsSystemClassInternal("");
  }

  @Test
  public void testIsSystemNestedClass() {
    testIsSystemClassInternal("$Klass");
  }

  private void testIsSystemClassInternal(String nestedClass) {
    assertFalse(isSystemClass("org.example.Foo" + nestedClass, null));
    assertTrue(isSystemClass("org.example.Foo" + nestedClass,
        classes("org.example.Foo")));
    assertTrue(isSystemClass("/org.example.Foo" + nestedClass,
        classes("org.example.Foo")));
    assertTrue(isSystemClass("org.example.Foo" + nestedClass,
        classes("org.example.")));
    assertTrue(isSystemClass("net.example.Foo" + nestedClass,
        classes("org.example.,net.example.")));
    assertFalse(isSystemClass("org.example.Foo" + nestedClass,
        classes("-org.example.Foo,org.example.")));
    assertTrue(isSystemClass("org.example.Bar" + nestedClass,
        classes("-org.example.Foo.,org.example.")));
    assertFalse(isSystemClass("org.example.Foo" + nestedClass,
        classes("org.example.,-org.example.Foo")));
    assertFalse(isSystemClass("org.example.Foo" + nestedClass,
        classes("org.example.Foo,-org.example.Foo")));
  }

  private List<String> classes(String classes) {
    return Lists.newArrayList(Splitter.on(',').split(classes));
  }

  @Test
  public void testGetResource() throws IOException {
    URL testJar = makeTestJar().toURI().toURL();

    ClassLoader currentClassLoader = getClass().getClassLoader();
    ClassLoader connectorClassloader = new ConnectorClassLoader(
        new URL[] { testJar }, currentClassLoader, null, false);

    assertNull(currentClassLoader.getResourceAsStream("resource.txt"),
        "Resource should be null for current classloader");

    InputStream in = connectorClassloader.getResourceAsStream("resource.txt");
    assertNotNull(in, "Resource should not be null for connector classloader");
    assertEquals("hello", IOUtils.toString(in));
  }

  private File makeTestJar() throws IOException {
    File jarFile = new File(testDir, "test.jar");
    JarOutputStream out = new JarOutputStream(new FileOutputStream(jarFile));
    ZipEntry entry = new ZipEntry("resource.txt");
    out.putNextEntry(entry);
    out.write("hello".getBytes());
    out.closeEntry();
    out.close();
    return jarFile;
  }
}
