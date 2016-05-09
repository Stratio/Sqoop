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
public class A {
  public String a;
  public int b;
  public int c;
  public int num;
  public Parent p;

  public A(String a) {
    num = 1;
    this.a = a;
  }
  public A(String a, Integer b, Integer c) {
    this(a);

    num = 3;
    this.b = b;
    this.c = c;
  }

  public A(Parent p) {
    this.p = p;
  }
}