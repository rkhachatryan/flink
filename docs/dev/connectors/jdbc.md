---
title: "JDBC Connector"
nav-title: JDBC
nav-parent_id: connectors
nav-pos: 9
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

* This will be replaced by the TOC
{:toc}


---
title: "JDBC Connector"
nav-title: JDBC
nav-parent_id: connectors
nav-pos: 9
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

* This will be replaced by the TOC
{:toc}


This connector provides sinks that write data into a JDBC database.

To use this connector, add the following dependency to your project (along with your JDBC-driver):

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-jdbc{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

Note that the streaming connectors are currently __NOT__ part of the binary distribution. See how to link with them for cluster execution [here]({{ site.baseurl}}/dev/projectsetup/dependencies.html).

JDBC sink usage depends on the use case and requirements.

## At-least-once delivery
This is the default Flink behaviour.

Usage:
{% highlight java %}
SinkFunction<Row> sink = FlinkJDBCFacade.sink(
    JDBCConnectionOptions.getBuilder()
        .withDriverName("org.apache.derby.jdbc.EmbeddedDriver")
        .withUrl("jdbc:derby:memory:test")
        .build(),
    JDBCInsertOptions.from("insert into books (id, title, author) values (?,?,?)", INTEGER, VARCHAR, VARCHAR),
    JDBCBatchOptions.defaults());
{% endhighlight %}

Please refer to the [API documentation]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/api/java/io/jdbc/JDBCOutputFormat.JDBCOutputFormatBuilder.html) for more details.

## Effectively exactly-once
In certain use cases exactly-once can be achieved by using upsert queries.

### Pre-requisites
1. Uniquely identifiable records
1. Database support for Upsert or equivalent

### Usage
{% highlight java %}
SinkFunction<Tuple2<Boolean, Row>> sink = FlinkJDBCFacade.upsertSink(
    JDBCConnectionOptions.getBuilder().build(),
    JDBCUpsertOptions.builder()
            .withTableName("books")
            .withFieldNames("id", "author", "title")
            .withDialect(JDBCDialects.JDBCDialectName.MYSQL)
            .build(),
    JDBCBatchOptions.defaults());
{% endhighlight %}
You should use a Flink-supported dialect by setting it explicitly with setDialect or implicitly in JDBC URL.

Please refer to the [API documentation]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/api/java/io/jdbc/JDBCUpsertOutputFormat.Builder.html) for more details.

### Drawbacks
1. Pre-requisites must be met
1. Index overhead
