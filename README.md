Stratio Sqoop
=============

Contents
--------

* Introduction
* Stratio Sqoop components
* Compile & Package
* FAQ

Introduction
------------
Apache Sqoop is a tool designed for efficiently transferring bulk data from structured datastores such as relational databases to different places. Sqoop works in batch mode and commonly it's used to import high volumes of data from big databases and store it in a data lake.
Apache Sqoop works on top of Hadoop but in Stratio we've adapted Sqoop to work also on top of Spark. At this way Sqoop on top of Spark is a perfect tool to import high volumes of data in a efficient way and send it for example to Kafka to be Ingested in your Big Data platform.

Stratio Sqoop components
------------------------
* Server
* [Shell](https://stratio.atlassian.net/wiki/display/SQOOP/Sqoop-shell+Syntax)
* [API Rest](https://stratio.atlassian.net/wiki/display/SQOOP/APIs)

Compile & Package
-----------------

```
$ mvn clean compile package -Ppackage
```

Sqoop server will be in server/target and Sqoop Shell in shell/target. You will find .deb, .rpm and .tar.gz packages 
ready to use depending your environment.
If you take a look at [documentation](https://stratio.atlassian.net/wiki/display/SQOOP0x2/Home) you will
 find more details about how to install the product, and some useful examples to get a better understanding about 
 Stratio Sqoop.

FAQ
---

**I would like to run my custom Sqoop job but I don’t know how to start.**

*You can take a look at [Get Started](https://stratio.atlassian.net/wiki/display/SQOOP/Get+Started) section. It’s a good starting point to understand better how to implement your custom business logic.*
 
**If Stratio Sqoop is an Open Source project. Where can I download the code?**

*You can get the code from https://github.com/Stratio/Sqoop/*
 
**I don’t know how to build a custom connector to use Stratio Sqoop with my database. What can I do?**

*You can take a look at [Sqoop 2 Connector Development](http://sqoop.apache.org/docs/1.99.6/ConnectorDevelopment.html). You have Java project documentation ready to begin with your custom development.*


Changelog
---------

See the [changelog](CHANGELOG.md) for changes. 
