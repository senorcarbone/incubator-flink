---
title: "Flink DataStream API Programming Guide"
is_beta: false
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

<a href="#top"></a>

DataStream programs in Flink are regular programs that implement transformations on data streams
(e.g., filtering, updating state, defining windows, aggregating). The data streams are initially created from certain
sources (e.g., message queues, socket streams, files). Results are returned via sinks, which may for
example write the data to (distributed) files, or to standard output (for example the command line
terminal). Flink programs run in a variety of contexts, standalone, or embedded in other programs.
The execution can happen in a local JVM, or on clusters of many machines.

In order to create your own Flink program, we encourage you to start with the
[program skeleton](#program-skeleton) and gradually add your own
[transformations](#transformations). The remaining sections act as references for additional
operations and advanced features.


* This will be replaced by the TOC
{:toc}


Example Program
---------------

The following program is a complete, working example of streaming window WordCount, that incrementally counts the
words coming from a web socket every 5 seconds. You can copy &amp; paste the code to run it locally.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

{% highlight java %}
public class WindowWordCount {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> dataStream = env
                .socketTextStream("localhost", 9999)
                .flatMap(new Splitter())
                .keyBy(0)
                .timeWindow(Time.of(5, TimeUnit.SECONDS))
                .sum(1);

        dataStream.print();

        env.execute("Socket Stream WordCount");
    }
    
    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
    
}
{% endhighlight %}

</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}

object WordCount {
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 9999)

    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .keyBy(0)
      .timeWindow(Time.of(5, TimeUnit.SECONDS))
      .sum(1);

    counts.print

    env.execute("Scala Socket Stream WordCount")
  }
}
{% endhighlight %}
</div>

</div>

To run the example program, start the input stream with netcat first from a terminal:

~~~bash
nc -lk 9999
~~~

The lines typed to this terminal will be the source data stream for your streaming job.

[Back to top](#top)


Linking with Flink
------------------

To write programs with Flink, you need to include the Flink DataStream library corresponding to
your programming language in your project.

The simplest way to do this is to use one of the quickstart scripts: either for
[Java]({{ site.baseurl }}/quickstart/java_api_quickstart.html) or for [Scala]({{ site.baseurl }}/quickstart/scala_api_quickstart.html). They
create a blank project from a template (a Maven Archetype), which sets up everything for you. To
manually create the project, you can use the archetype and create a project by calling:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight bash %}
mvn archetype:generate /
    -DarchetypeGroupId=org.apache.flink/
    -DarchetypeArtifactId=flink-quickstart-java /
    -DarchetypeVersion={{site.version }}
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight bash %}
mvn archetype:generate /
    -DarchetypeGroupId=org.apache.flink/
    -DarchetypeArtifactId=flink-quickstart-scala /
    -DarchetypeVersion={{site.version }}
{% endhighlight %}
</div>
</div>

The archetypes are working for stable releases and preview versions (`-SNAPSHOT`).

If you want to add Flink to an existing Maven project, add the following entry to your
*dependencies* section in the *pom.xml* file of your project:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-core</artifactId>
  <version>{{site.version }}</version>
</dependency>
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-clients</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-scala</artifactId>
  <version>{{site.version }}</version>
</dependency>
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-clients</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}
</div>
</div>

In order to create your own Flink program, we encourage you to start with the
[program skeleton](#program-skeleton) and gradually add your own
[transformations](#transformations).

[Back to top](#top)

Program Skeleton
----------------

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

As presented in the [example](#example-program), Flink DataStream programs look like regular Java
programs with a `main()` method. Each program consists of the same basic parts:

1. Obtaining a `StreamExecutionEnvironment`,
2. Connecting to data stream sources,
3. Specifying transformations on the data streams,
4. Specifying output for the processed data,
5. Executing the program.

We will now give an overview of each of those steps, please refer to the respective sections for
more details. 

The `StreamExecutionEnvironment` is the basis for all Flink DataSet programs. You can
obtain one using these static methods on class `StreamExecutionEnvironment`:

{% highlight java %}
getExecutionEnvironment()

createLocalEnvironment()
createLocalEnvironment(int parallelism)
createLocalEnvironment(int parallelism, Configuration customConfiguration)

createRemoteEnvironment(String host, int port, String... jarFiles)
createRemoteEnvironment(String host, int port, int parallelism, String... jarFiles)
{% endhighlight %}

Typically, you only need to use `getExecutionEnvironment()`, since this
will do the right thing depending on the context: if you are executing
your program inside an IDE or as a regular Java program it will create
a local environment that will execute your program on your local machine. If
you created a JAR file from you program, and invoke it through the [command line](cli.html)
or the [web interface](web_client.html),
the Flink cluster manager will execute your main method and `getExecutionEnvironment()` will return
an execution environment for executing your program on a cluster.

For specifying data sources the execution environment has several methods
to read from files, sockets, and external systems using various methods. To just read
data from a socket (useful also for debugging), you can use:

{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

DataStream<String> lines = env.socketTextStream("localhost", 9999)
{% endhighlight %}

This will give you a DataStream on which you can then apply transformations. For
more information on data sources and input formats, please refer to
[Data Sources](#data-sources).

Once you have a DataStream you can apply transformations to create a new
DataStream which you can then write to a socket, transform again,
combine with other DataSets, or push to an external system (e.g., a message queue, or a file system).
You apply transformations by calling
methods on DataStream with your own custom transformation function. For example,
a map transformation looks like this:

{% highlight java %}
DataStream<String> input = ...;

DataStream<Integer> intValues = input.map(new MapFunction<String, Integer>() {
    @Override
    public Integer map(String value) {
        return Integer.parseInt(value);
    }
});
{% endhighlight %}

This will create a new DataStream by converting every String in the original
set to an Integer. For more information and a list of all the transformations,
please refer to [Transformations](#transformations).

Once you have a DataStream containing your final results, you can push the result
to an external system (HDFS, Kafka, Elasticsearch), write it to a socket, write to a file,
or print it.

{% highlight java %}
writeAsText(String path, ...)
writeAsCsv(String path, ...)
writeToSocket(String hostname, int port, ...)

print()

addSink(...)
{% endhighlight %}

Once you specified the complete program you need to **trigger the program execution** by 
calling `execute(programName)` on `StreamExecutionEnvironment`. This will either execute on 
the local machine or submit the program for execution on a cluster, depending on the chosen execution environment.
        
{% highlight java %}
env.execute(programName)
{% endhighlight %}

</div>
<div data-lang="scala" markdown="1">

As presented in the [example](#example-program), Flink DataStream programs look like regular Scala
programs with a `main()` method. Each program consists of the same basic parts:

1. Obtaining a `StreamExecutionEnvironment`,
2. Connecting to data stream sources,
3. Specifying transformations on the data streams,
4. Specifying output for the processed data,
5. Executing the program.

We will now give an overview of each of those steps, please refer to the respective sections for
more details.

The `StreamExecutionEnvironment` is the basis for all Flink DataSet programs. You can
obtain one using these static methods on class `StreamExecutionEnvironment`:

{% highlight scala %}
def getExecutionEnvironment

def createLocalEnvironment(parallelism: Int =  Runtime.getRuntime.availableProcessors())

def createRemoteEnvironment(host: String, port: Int, jarFiles: String*)
def createRemoteEnvironment(host: String, port: Int, parallelism: Int, jarFiles: String*)
{% endhighlight %}

Typically, you only need to use `getExecutionEnvironment`, since this
will do the right thing depending on the context: if you are executing
your program inside an IDE or as a regular Java program it will create
a local environment that will execute your program on your local machine. If
you created a JAR file from you program, and invoke it through the [command line](cli.html)
or the [web interface](web_client.html),
the Flink cluster manager will execute your main method and `getExecutionEnvironment()` will return
an execution environment for executing your program on a cluster.

For specifying data sources the execution environment has several methods
to read from files, sockets, and external systems using various methods. To just read
data from a socket (useful also for debugginf), you can use:

{% highlight scala %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

DataStream<String> lines = env.socketTextStream("localhost", 9999)
{% endhighlight %}

This will give you a DataStream on which you can then apply transformations. For
more information on data sources and input formats, please refer to
[Data Sources](#data-sources).

Once you have a DataStream you can apply transformations to create a new
DataStream which you can then write to a file, transform again,
combine with other DataStreams, or push to an external system.
You apply transformations by calling
methods on DataStream with your own custom transformation function. For example,
a map transformation looks like this:

{% highlight scala %}
val input: DataStream[String] = ...

val mapped = input.map { x => x.toInt }
{% endhighlight %}

This will create a new DataStream by converting every String in the original
set to an Integer. For more information and a list of all the transformations,
please refer to [Transformations](#transformations).

Once you have a DataStream containing your final results, you can push the result
to an external system (HDFS, Kafka, Elasticsearch), write it to a socket, write to a file,
or print it.

{% highlight scala %}
writeAsText(path: String, ...)
writeAsCsv(path: String, ...)
writeToSocket(hostname: String, port: Int, ...)

print()

addSink(...)
{% endhighlight %}

Once you specified the complete program you need to **trigger the program execution** by
calling `execute(programName)` on `StreamExecutionEnvironment`. This will either execute on
the local machine or submit the program for execution on a cluster, depending on the chosen execution environment.

{% highlight scala %}
env.execute(programName)
{% endhighlight %}

</div>
</div>

[Back to top](#top)

DataStream abstraction
----------------------

A `DataStream` is a possibly unbounded immutable collection of data of a the same type.

Transformations may return different subtypes of `DataStream` allowing specialized transformations.
For example the `keyBy(…)` method returns a `KeyedDataStream` which is a stream of data that
is logically partitioned by a certain key, and can be further windowed.

[Back to top](#top)

Lazy Evaluation
---------------

All Flink programs are executed lazily: When the program's main method is executed, the data loading
and transformations do not happen directly. Rather, each operation is created and added to the
program's plan. The operations are actually executed when the execution is explicitly triggered by 
an `execute()` call on the `StreamExecutionEnvironment` object. Whether the program is executed locally 
or on a cluster depends on the type of `StreamExecutionEnvironment`.

The lazy evaluation lets you construct sophisticated programs that Flink executes as one
holistically planned unit.

[Back to top](#top)


Transformations
---------------

Data transformations transform one or more DataSets into a new DataSet. Programs can combine
multiple transformations into sophisticated topologies.

This section gives a description of all the available transformations.


<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

<br />

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
          <td><strong>Map</strong><br>DataStream &rarr; DataStream</td>
          <td>
            <p>Takes one element and produces one element. A map that doubles the values of the input stream:</p>
    {% highlight java %}
    dataStream.map(new MapFunction<Integer, Integer>() {
                @Override
                public Integer map(Integer value) throws Exception {
                    return 2 * value;
                }
            });
    {% endhighlight %}
          </td>
        </tr>

        <tr>
          <td><strong>FlatMap</strong><br>DataStream &rarr; DataStream</td>
          <td>
            <p>Takes one element and produces zero, one, or more elements. A flatmap that splits sentences to words:</p>
    {% highlight java %}
    dataStream.flatMap(new FlatMapFunction<String, String>() {
                @Override
                public void flatMap(String value, Collector<String> out)
                    throws Exception {
                    for(String word: value.split(" ")){
                        out.collect(word);
                    }
                }
            });
    {% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Filter</strong><br>DataStream &rarr; DataStream</td>
          <td>
            <p>Evaluates a boolean function for each element and retains those for which the function returns true.
    	<br/>
    	<br/>
            A filter that filters out zero values:
            </p>
    {% highlight java %}
    dataStream.filter(new FilterFunction<Integer>() {
                @Override
                public boolean filter(Integer value) throws Exception {
                    return value != 0;
                }
            });
    {% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>KeyBy</strong><br>DataStream &rarr; KeyedDataStream</td>
          <td>
            <p>Logically partition a stream into disjoint partitions, each partition containing elements of the same key. 
            Internally, this is implemented with hash partitioning. See <a href="#specifying-keys">keys</a> on how to specify keys.
            This transformations returns a `KeyedDataStream`.</p>
    {% highlight java %}
    dataStream.keyBy("someKey") // Key by field "someKey"
    dataStream.keyBy(0) // Key by the first element of a Tuple
    {% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Reduce</strong><br>KeyedDataStream &rarr; DataStream</td>
          <td>
            <p>A "rolling" reduce on a keyed data stream. Combines the last element with the last reduced value and
            emits the new value.
                    <br/>
            	<br/>
            A reduce function that creates a stream of partial sums:</p>
            {% highlight java %}
            keyedStream.reduce(new ReduceFunction<Integer>() {
                        @Override
                        public Integer reduce(Integer value1, Integer value2)
                        throws Exception {
                            return value1 + value2;
                        }
                    });
            {% endhighlight %}
            </p>
          </td>
        </tr>
        <tr>
          <td><strong>Fold</strong><br>DataStream &rarr; DataStream</td>
          <td>
          <p>A "rolling" fold on a keyed data stream with an initial value. 
          Combines the last element with the last folded value and
          emits the new value.
          <br/>
          <br/>
          A fold function that creates a stream of partial sums:</p>
          {% highlight java %}
          keyedStream.fold(0, new ReduceFunction<Integer>() {
                      @Override
                      public Integer fold(Integer accumulator, Integer value)
                      throws Exception {
                          return accumulator + value;
                      }
                  });
          {% endhighlight %}
          </p>
          </td>
        </tr>
        <tr>
          <td><strong>Window</strong><br>KeyedDataStream &rarr; KeyedWindowDataStream</td>
          <td>
            <p>Windows can be defined on already partitioned `KeyedDataStream`s. Windows group the data in each
            key according to some characteristic (e.g., the data that arrived within the last 5 seconds).
            See <a href="#windows">windows</a> for a description of windows.
    {% highlight java %}
    dataStream.keyBy(0).window(TumblingTimeWindows.of(5000)) // Last 5 seconds of data
    {% endhighlight %}
        </p>
          </td>
        </tr>
        <tr>
          <td><strong>WindowAll</strong><br>DataStream &rarr; AllWindowedDataStream</td>
          <td>
              <p>Windows can be defined on regular `DataStream`s. Windows group all the stream events 
              according to some characteristic (e.g., the data that arrived within the last 5 seconds).
              See <a href="#windows">windows</a> for a complete description of windows.</p>
              <br/>
              <strong>WARNING:</strong> This is a <strong>non-parallel</strong> transformation. All records will be
               gathered in one task for the `windowAll` operator.
  {% highlight java %}
  dataStream.windowAll(TumblingTimeWindows.of(5000)) // Last 5 seconds of data
  {% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Apply window function</strong><br>KeyedWindowDataStream &rarr; DataStream<br>AllWindowedDataStream &rarr; DataStream</td>
          <td>
            <p>Applies a general function to the window as a whole</p>
    {% highlight java %}
    windowedStream.apply (new WindowFunction<Tuple2<String,Integer>,Integer>, Tuple, Window>() {
        public void apply (Tuple tuple,
                Window window,
                Iterable<Tuple2<String, Integer>> values,
                Collector<Integer> out) throws Exception {
            int sum = 0;    
            for (value t: values) {
                sum += t.f1;
            }
            out.collect (new Integer(sum));
        }
    };
    {% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>ReduceWindow</strong><br>KeyedWindowDataStream &rarr; DataStream</td>
          <td>
            <p>Applies a functional reduce function to the window</p>
    {% highlight java %}
    windowedStream.reduceWindow (new ReduceFunction<Tuple2<String,Integer>() {
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
            return new Tuple2<String,Integer>(value1.f0, value1.f1 + value2.f1);
        }
    };
    {% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Union</strong><br>DataStream,DataStream &rarr; DataStream</td>
          <td>
            <p>Union of two or more data streams creating a new stream containing all the elements from all the streams. Node: If you union a data stream
            with itself you will still only get each element once.</p>
    {% highlight java %}
    dataStream.union(otherStream1, otherStream2, …)
    {% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Window Join</strong><br>DataStream,DataStream &rarr; DataStream</td>
          <td>
            <p>Join two data streams on a given key and a common window.</p>
    {% highlight java %}
    dataStream.join(otherStream)
        .where(0).equalTo(1)
        .onTimeWindow(Time.of(5, TimeUnit.SECONDS))
        .apply (new JoinFunction ...)
    {% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Connect</strong><br>DataStream,DataStream &rarr; ConnectedDataStream</td>
          <td>
            <p>"Connects" two data streams retaining their types. Connect make it possible to use shared state for 
            the two streams.</p>
    {% highlight java %}
    DataStream<Integer> someStream = ...
    DataStream<String> otherStream = ...

    someStream.connect(otherStream)
    {% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>CoMap, CoFlatMap</strong><br>ConnectedDataStream &rarr; DataStream</td>
          <td>
            <p>Similar to map and flatMap on a connected data stream</p>
    {% highlight java %}
    connectedStream.map(new CoMapFunction<Integer, String, Boolean>() {
        @Override
        public Boolean map1(Integer value) {
            return true;
        }

        @Override
        public Boolean map2(String value) {
            return false;
        }
    });
    connectedStream.flatMap(new CoFlatMapFunction<Integer, String, String>() {

       @Override
       public void flatMap1(Integer value, Collector<String> out) {
           out.collect(value.toString());
       }

       @Override
       public void flatMap2(String value, Collector<String> out) {
           for (String word: value.split(" ")) {
             out.collect(word);
           }
       }
    })
    {% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Split</strong><br>DataStream &rarr; SplitDataStream</td>
          <td>
            <p>
                Split the stream into two or more streams according to some criterion.
                {% highlight java %}
                SplitDataStream<Integer> split = someDataStream.split(new OutputSelector<Integer>() {
                    @Override
                    public Iterable<String> select(Integer value) {
                        if (value % 2 == 0) {
                            output.add("even");
                        }
                        else {
                            output.add("odd");
                        }
                    }
                });
                DataStream<Integer> even = split.select("even");
                DataStream<Integer> odd = split.select("odd");
                DataStream<Integer> all = split.select("even","odd");
                {% endhighlight %}
            </p>
          </td>
        </tr>
        <tr>
          <td><strong>Iterate</strong><br>DataStream &rarr; IterativeDataStream</td>
          <td>
            <p>
                Creates a "feedback" loop in the flow, by redirecting the output of one operator
                to some previous operator. This is especially useful for defining algorithms that
                continuously update a model.
                {% highlight java %}
                DataStream<Integer> initial = //...
                IterativeDataStream<Integer> iteration = initial.iterate();
                SplitDataStream<Integer> split = iteration.split(new OutputSelector<Integer>() {
                     @Override
                     public Iterable<String> select(Integer value) {
                         if (value % 2 == 0) {
                             output.add("feedback");
                         }
                         else {
                             output.add("forward");
                         }
                     }
                 });
                iteration.closeWith(split.select("feedback"));
                {% endhighlight %}
            </p>
          </td>
        </tr>
  </tbody>
</table>

</div>
</div>

The following transformations are available on data streams of Tuples:


<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

<br />

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
   <tr>
      <td><strong>Project</strong><br>DataStream &rarr; DataStream</td>
      <td>
        <p>Selects a subset of fields from the tuples
{% highlight java %}
DataStream<Tuple3<Integer, Double, String>> in = // [...]
DataStream<Tuple2<String, Integer>> out = in.project(2,0);
{% endhighlight %}
        </p>
      </td>
    </tr>
  </tbody>
</table>

</div>
</div>




Flink also gives low-level control (if desired) on the exact stream partitioning after a transformation
via the following functions.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

<br />

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
   <tr>
      <td><strong>Hash partitioning</strong><br>DataStream &rarr; DataStream</td>
      <td>
        <p>
            Identical to `keyBy` but returns a `DataStream` instead of a `KeyedDataStream`
            {% highlight java %}
            dataStream.partitionByHash("someKey");
            dataStream.partitionByHash(0);
            {% endhighlight %}
        </p>
      </td>
    </tr>
   <tr>
      <td><strong>Custom partitioning</strong><br>DataStream &rarr; DataStream</td>
      <td>
        <p>
            Uses a user-defined `Partitioner` to select the target task for each element.
            {% highlight java %}
            dataStream.partitionCustom(new Partitioner(){...}, "someKey");
            dataStream.partitionCustom(new Partitioner(){...}, 0);
            {% endhighlight %}
        </p>
      </td>
    </tr>
   <tr>
     <td><strong>Random partitioning</strong><br>DataStream &rarr; DataStream</td>
     <td>
       <p>
            Partitions elements randomly according to a uniform distribution.
            {% highlight java %}
            dataStream.partitionRandom();
            {% endhighlight %}
       </p>
     </td>
   </tr>
   <tr>
      <td><strong>Rebalancing (Round-robin partitioning)</strong><br>DataStream &rarr; DataStream</td>
      <td>
        <p>
            Partitions elements round-robin, creating equal load per partition. Useful for performance
            optimization in the presence of data skew.
            {% highlight java %}
            dataStream.rebalance();
            {% endhighlight %}
        </p>
      </td>
    </tr>
   <tr>
      <td><strong>Broadcasting</strong><br>DataStream &rarr; DataStream</td>
      <td>
        <p>
            Broadcasts elements to every partition.
            {% highlight java %}
            dataStream.broadcast();
            {% endhighlight %}
        </p>
      </td>
    </tr>
  </tbody>
</table>

</div>
</div>

[Back to top](#top)

Specifying Keys
--------------------

The keyBy transformation requires that a key is defined on
its argument DataStream.

A DataStream is keyed as
{% highlight java %}
DataStream<...> input = // [...]
DataStream<...> windowed = input
	.keyBy(/*define key here*/)
	.window(/*define window here*/);
{% endhighlight %}

The data model of Flink is not based on key-value pairs. Therefore,
you do not need to physically pack the data stream types into keys and
values. Keys are "virtual": they are defined as functions over the
actual data to guide the grouping operator.

See [the relevant section of the DataSet API documentation](programming_guide.html#specifying-keys) on how to specify keys.
Just replace `DataSet` with `DataStream`, and `groupBy` with `keyBy`.



Passing Functions to Flink
--------------------------

Some transformations take user-defined functions as arguments. 

See [the relevant section of the DataSet API documentation](programming_guide.html#passing-functions-to-flink).


[Back to top](#top)


Data Types
----------

Flink places some restrictions on the type of elements that are used in DataStreams and as results
of transformations. The reason for this is that the system analyzes the types to determine
efficient execution strategies.

See [the relevant section of the DataSet API documentation](programming_guide.html#data-types).

[Back to top](#top)


Data Sources
------------

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

Sources can by created by using `StreamExecutionEnvironment.addSource(sourceFunction)`.
You can either use one of the source functions that come with Flink or write a custom source
by implementing the `SourceFunction` for non-parallel sources, or by implementing the
`ParallelSourceFunction` interface or extending `RichParallelSourceFunction` for parallel sources.

There are several predefined stream sources accessible from the `StreamExecutionEnvironment`:

File-based:

- `readTextFile(path)` / `TextInputFormat` - Reads files line wise and returns them as Strings.

- `readTextFileWithValue(path)` / `TextValueInputFormat` - Reads files line wise and returns them as
  StringValues. StringValues are mutable strings.

- `readFile(path)` / Any input format - Reads files as dictated by the input format.

- `readFileOfPrimitives(path, Class)` / `PrimitiveInputFormat` - Parses files of new-line (or another char sequence) delimited primitive data types such as `String` or `Integer`.

- `readFileStream` - create a stream by appending elements when there are changes to a file

Socket-based:

- `socketTextStream` - Reads from a socket. Elements can be separated by a delimiter.

Collection-based:

- `fromCollection(Collection)` - Creates a data set from the Java Java.util.Collection. All elements
  in the collection must be of the same type.

- `fromCollection(Iterator, Class)` - Creates a data set from an iterator. The class specifies the
  data type of the elements returned by the iterator.

- `fromElements(T ...)` - Creates a data set from the given sequence of objects. All objects must be
  of the same type.

- `fromParallelCollection(SplittableIterator, Class)` - Creates a data set from an iterator, in
  parallel. The class specifies the data type of the elements returned by the iterator.

- `generateSequence(from, to)` - Generates the sequence of numbers in the given interval, in
  parallel.

Custom:

- `addSource` - Attache a new source function. For example, to read from Apache Kafka you can use
    `addSource(new FlinkKafkaConsumer082<>(...))`. See [connectors](#stream-connectors) for more details.

</div>
</div>

[Back to top](#top)


Execution Configuration
----------

The `StreamExecutionEnvironment` also contains the `ExecutionConfig` which allows to set job specific configuration values for the runtime.

See [the relevant section of the DataSet API documentation](programming_guide.html#execution-configuration).

[Back to top](#top)

Data Sinks
----------

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

Data sinks consume DataStreams and forward them to files, sockets, external systems, or print them.
Flink comes with a variety of built-in output formats that are encapsulated behind operations on the
DataSet:

- `writeAsText()` / `TextOuputFormat` - Writes elements line-wise as Strings. The Strings are
  obtained by calling the *toString()* method of each element.
- `writeAsCsv(...)` / `CsvOutputFormat` - Writes tuples as comma-separated value files. Row and field
  delimiters are configurable. The value for each field comes from the *toString()* method of the objects.
- `print()` / `printToErr()`  - Prints the *toString()* value
of each element on the standard out / strandard error stream. Optionally, a prefix (msg) can be provided which is
prepended to the output. This can help to distinguish between different calls to *print*. If the parallelism is
greater than 1, the output will also be prepended with the identifier of the task which produced the output.
- `write()` / `FileOutputFormat` - Method and base class for custom file outputs. Supports
  custom object-to-bytes conversion.
- `writeToSocket` - Writes elements to a socket according to a `SerializationSchema`
- `addSink` - Invokes a custom sink function. Flink comes bundled with connectors to other systems (such as 
    Apache Kafka) that are implemented as sink functions.

</div>
</div>


[Back to top](#top)

Windows
-------

### Notions of time and watermarks

Windows are typically groups of events within a certain time period. Reasoning about time and windows assumes
a definition of time. Flink has support for three kinds of time:

- *Processing time:* Processing time is simply the wall clock time of the machine that happens to be
    executing the transformation. Processing time is the simplest notion of time and provides the best
    performance. However, in distributed and asynchronous environments processing time does not provide
    determinism.

- *Event time:* Event time is the time that each individual event occurred. This time is either recorded
    in a timestamp embedded within the records before they enter Flink, or is assigned at the source.
    When using event time, out-of-order events can be properly handled. For example, an event with a lower
    timestamp may arrive after an event with a higher timestamp, but transformations will handle these events
    correctly. Event time processing provides predictable results, but incurs more latency, as out-of-order
    events need to be buffered

- *Ingestion time:*: Ingestion time is the time that events enter Flink. Ingestion time is more predictable
    than processing time, and gives lower latencies than event time as the latency does not depend on 
    external systems. Ingestion time provides thus a middle ground between processing time and event time.

When dealing with event time, transformations need to know how long to buffer data to avoid indefinite
wait times. *Watermarks* provide the mechanism to control the event time-processing time skew. Watermarks
are emitted by the sources. A watermark with a certain timestamp denotes the knowledge that no event
with timestamp lower than the timestamp of the watermark will ever arrive.
 
You can specify the semantics of time in a Flink DataStream program using `StreamExecutionEnviroment`, as

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
{% endhighlight %}
</div>
</div>

The default value is `TimeCharacteristic.ProcessingTime`.

This will make sure that all transformations (unless explicitly specified in the transformation name) will
use the desired notion of time. Users do not need to worry about watermark generation if they use one 
of the bundled Flink sources that supports watermarks.


### Windows on keyed data streams 

Flink offers a variety of methods for defining windows on a `KeyedDataStream`. All of these group elements *per key*,
i.e., each window will contain elements with the same key value.

#### Basic window constructs

Flink offers a general window mechanism that provides flexibility, as well as a number of pre-defined windows
for common use cases. See first if your use case can be served by the pre-defined windows below before moving
to defining your own windows.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

<br />

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
      <tr>
        <td><strong>Tumbling time window</strong><br>KeyedStream &rarr; WindowedStream</td>
        <td>
          <p>
          Defines a window of 5 seconds, that "tumbles". This means that elements are
          grouped according to their timestamp in groups of 5 second duration, and every element belongs to exactly one window.
          The notion of time used is controlled by the `StreamExecutionEnvironment`.
    {% highlight java %}
        keyedStream.timeWindow(Time.of(5, TimeUnit.SECONDS));
    {% endhighlight %}
          </p>
        </td>
      </tr>
      <tr>
          <td><strong>Sliding time window</strong><br>KeyedStream &rarr; WindowedStream</td>
          <td>
            <p>
             Defines a window of 5 seconds, that "slides" by 1 seconds. This means that elements are
             grouped according to their timestamp in groups of 5 second duration, and elements can belong to more than
             one window (since windows overlap by at least 4 seconds)
             The notion of time used is controlled by the `StreamExecutionEnvironment`.
      {% highlight java %}
        keyedStream.timeWindow(Time.of(5, TimeUnit.SECONDS), Time.of(1, TimeUnit.SECONDS));
      {% endhighlight %}
            </p>
          </td>
        </tr>
      <tr>
        <td><strong>Tumbling count window</strong><br>KeyedStream &rarr; WindowedStream</td>
        <td>
          <p>
          Defines a window of 1000 elements, that "tumbles". This means that elements are
          grouped according to their arrival time (equivalent to processing time) in groups of 1000 elements, 
          and every element belongs to exactly one window.
    {% highlight java %}
        keyedStream.countWindow(1000)
    {% endhighlight %}
        </p>
        </td>
      </tr>
      <tr>
      <td><strong>Sliding count window</strong><br>KeyedStream &rarr; WindowedStream</td>
      <td>
        <p>
          Defines a window of 1000 elements, that "slides" every 100 elements. This means that elements are
          grouped according to their arrival time (equivalent to processing time) in groups of 1000 elements, 
          and every element can belong to more than one window (as windows overlap by at least 900 elements).
  {% highlight java %}
    keyedStream.countWindow(1000, 100)
  {% endhighlight %}
        </p>
      </td>
    </tr>
  </tbody>
</table>

</div>
</div>


#### Advanced window constructs
  
The general mechanism can define more powerful windows at the cost of more verbose syntax. For example,
below is a window definition where windows hold elements of the last second and slide 100 milliseconds,
but the execution of the window function is triggered when 100 elements have been added to the
window, and every time execution is triggered, 10 elements are removed from the window:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
keyedStream
    .window(SlidingTimeWindows.of(1000,100)
    .trigger(Count.of(100))
    .evictor(Count.of(10));
{% endhighlight %}
</div>
</div>

The general recipe for building a custom window is to specify (1) a `WindowAssigner`, (2) a `Trigger` (optionally),
and (3) an `Evictor` (optionally).

The `WindowAssigner` defines how incoming elements are assigned to windows. A window is a logical group of elements
that has a begin-value, and an end-value corresponding to a begin-time and end-time. Elements with timestamp (according
to some notion of time described above within these values are part of the window).

For example, the `SlidingTimeWindows`
assigner in the example defines a window of size 1000 milliseconds, and a slide of 100 milliseconds. Assume that
time starts from 0. Then, we have 10 windows that overlap: [0,1000], [100,1100], [100,1100], ..., [1000, 2000]. Each incoming
element is assigned to the windows according to its timestamp. For example, an element with timestamp 200 will be 
assigned to the first three windows. Flink comes bundled with window assigners that cover the most common use cases. You can write your
own window types by extending the `WindowAssigner` class.

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
      <tr>
        <td><strong>Global window</strong><br>KeyedStream &rarr; WindowedStream</td>
        <td>
          <p>
            All incoming elements of a given key are assigned to the same window. The window is never triggered.
          </p>
    {% highlight java %}
        stream.window(GlobalWindows.create())
    {% endhighlight %}
        </td>
      </tr>
      <tr>
          <td><strong>Tumbling event time windows</strong><br>KeyedStream &rarr; WindowedStream</td>
          <td>
            <p>
              Incoming elements are assigned to a window of a certain size (1000 milliseconds below) based on 
              their timestamp. Windows do not overlap, i.e., each element is assigned to exactly one window.
              A window is triggered when a watermark with value higher than its end-value is received.
            </p>
      {% highlight java %}
        stream.window(TumblingTimeWindows.of(1000))
      {% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Tumbling processing time windows</strong><br>KeyedStream &rarr; WindowedStream</td>
          <td>
            <p>
              Incoming elements are assigned to a window of a certain size (1000 milliseconds below) based on
              the current processing time. A window is triggered when the current processing time exceeds
              its end value.
            </p>
      {% highlight java %}
        stream.window(TumblingProcessingTimeWindows.of(1000))
      {% endhighlight %}
          </td>
        </tr>
      <tr>
        <td><strong>Sliding event time windows</strong><br>KeyedStream &rarr; WindowedStream</td>
        <td>
          <p>
            Incoming elements are assigned to a window of a certain size (1000 milliseconds below) based on 
            their timestamp. Windows "slide" by the provided value (100 milliseconds in the example), and hence 
            overlap. A window is triggered when a watermark with value higher than its end-value is received.
          </p>
    {% highlight java %}
        stream.window(SlidingTimeWindows.of(1000,100))
    {% endhighlight %}
        </td>
      </tr>
      <tr>
          <td><strong>Sliding processing time windows</strong><br>KeyedStream &rarr; WindowedStream</td>
          <td>
            <p>
              Incoming elements are assigned to a window of a certain size (1000 milliseconds below) based on
              the current processing time. Windows "slide" by the provided value (100 milliseconds in the example), and hence
              overlap. A window is triggered when the current processing time exceeds its end value.
            </p>
      {% highlight java %}
          stream.window(SlidingTimeWindows.of(1000,100))
      {% endhighlight %}
          </td>
        </tr>
  </tbody>
</table>

<!-- TODO -->

***NOTE:*** Flink has internally a "fast-path" implementation that does not naively duplicate elements to several windows. This
code path is used for some windows.

The `Trigger` specifies when the function that comes after the window clause (e.g., `sum`, `count`) is evaluated ("fires")
for each window. If a trigger is not specified, a default trigger for each window type is used (that is part of the
definition of the `WindowAssigner`). Flink comes bundled with a set of triggers if the ones that windows use by
default do not fit the application. You can write your own trigger by implementing the `Trigger` interface.

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
  <tr>
    <td><strong>Processing time trigger</strong></td>
    <td>
      <p>
        A window is fired when the current processing time exceeds its end-value.
        The elements on the triggered window are henceforth discarded.
      </p>
{% highlight java %}
    windowedStream.trigger(ProcessingTimeTrigger.create());
{% endhighlight %}
    </td>
  </tr>
  <tr>
    <td><strong>Watermark trigger</strong></td>
    <td>
      <p>
        A window is fired when a watermark with value that exceeds the window's end-value has been received.
        The elements on the triggered window are henceforth discarded.
      </p>
{% highlight java %}
    windowedStream.trigger(WatermarkTrigger.create());
{% endhighlight %}
    </td>
  </tr>
  <tr>
    <td><strong>Continuous processing time trigger</strong></td>
    <td>
      <p>
        A window is periodically considered for being fired (every 5000 milliseconds in the example). 
        The window is actually fired only when the current processing time exceeds its end-value.
        The elements on the triggered window are retained.
      </p>
{% highlight java %}
    windowedStream.trigger(ContinuousProcessingTimeTrigger.of(5000));
{% endhighlight %}
    </td>
  </tr>
  <tr>
    <td><strong>Continuous watermark time trigger</strong></td>
    <td>
      <p>
        A window is periodically considered for being fired (every 5000 milliseconds in the example).
        A window is actually fired when a watermark with value that exceeds the window's end-value has been received.
        The elements on the triggered window are retained.
      </p>
{% highlight java %}
    windowedStream.trigger(ContinuousWatermarkTrigger.of(5000));
{% endhighlight %}
    </td>
  </tr>
  <tr>
    <td><strong>Count trigger</strong></td>
    <td>
      <p>
        A window is fired when it has more than a certain number of elements (1000 below).
        The elements of the triggered window are retained.
      </p>
{% highlight java %}
    windowedStream.trigger(CountTrigger.of(1000));
{% endhighlight %}
    </td>
  </tr>
  <tr>
    <td><strong>Purging trigger</strong></td>
    <td>
      <p>
        Takes any trigger as an argument and forces the triggered window elements to be
        "purged" (discarded) after triggering.
      </p>
{% highlight java %}
    windowedStream.trigger(PurgingTrigger.of(CountTrigger.of(1000)));
{% endhighlight %}
    </td>
  </tr>
  <tr>
    <td><strong>Delta trigger</strong></td>
    <td>
      <p>
        A window is periodically considered for being fired (every 5000 milliseconds in the example).
        A window is actually fired when the value of the current element exceeds the value of
        the first element inserted in the window according to a `DeltaFunction`.
      </p>
{% highlight java %}
    windowedStream.trigger(DeltaTrigger.of(5000, new DeltaFunction<Double>() {
        public double (Double old, Double new) {
            return (new - old > 0.01);
        }
    }));
{% endhighlight %}
    </td>
  </tr>
 </tbody>
</table>

After the trigger fires, and before the function (e.g., `sum`, `count`) is applied to the window contents, an
optional `Evictor` removes some elements from the beginning of the window before the remaining elements
are passed on to the function. Flink comes bundled with a set of evictors You can write your own evictor by 
implementing the `Evictor` interface.

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
  <tr>
      <td><strong>Time evictor</strong></td>
      <td>
        <p>
         Evict all elements from the beginning of the window, so that elements from end-value - 1000 milliseconds
         until end-value are retained (the resulting window size is 1000 milliseconds).
        </p>
  {% highlight java %}
      triggeredStream.evict(TimeEvictor.of(1000));
  {% endhighlight %}
      </td>
    </tr>
   <tr>
       <td><strong>Count evictor</strong></td>
       <td>
         <p>
          Retain 1000 elements from the end of the window backwards, evicting all others.
         </p>
   {% highlight java %}
       triggeredStream.evict(CountEvictor.of(1000));
   {% endhighlight %}
       </td>
     </tr>
    <tr>
        <td><strong>Delta evictor</strong></td>
        <td>
          <p>
            Starting from the beginning of the window, evict elements until an element with
            value lower than the value of the last element is found (by a threshold and a 
            `DeltaFunction`).
          </p>
    {% highlight java %}
        triggeredStream.evict(DeltaEvictor.of(5000, new DeltaFunction<Double>() {
          public double (Double old, Double new) {
              return (new - old > 0.01);
          }
      }));
    {% endhighlight %}
        </td>
      </tr>
 </tbody>
</table>

#### Guidelines for defining advanced windows


### Windows on unkeyed data streams (non-parallel windows)

You can also define windows on regular (non-keyed) data streams using the `windowAll` transformation. These 
windowed data streams have all the capabilities of keyed windowed data streams, but are evaluated at a single
task (and hence at a single computing node). The syntax for defining triggers and evictors is exactly the
same: 

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
nonKeyedStream
    .windowAll(SlidingTimeWindows.of(1000,100)
    .trigger(Count.of(100))
    .evictor(Count.of(10));
{% endhighlight %}
</div>
</div>

The syntax for the basic window definitions is similar:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

<br />

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
      <tr>
        <td><strong>Tumbling time window</strong><br>KeyedStream &rarr; WindowedStream</td>
        <td>
          <p>
          Defines a window of 5 seconds, that "tumbles". This means that elements are
          grouped according to their timestamp in groups of 5 second duration, and every element belongs to exactly one window.
          The notion of time used is controlled by the `StreamExecutionEnvironment`.
    {% highlight java %}
        nonKeyedStream.timeWindowAll(Time.of(5, TimeUnit.SECONDS));
    {% endhighlight %}
          </p>
        </td>
      </tr>
      <tr>
          <td><strong>Sliding time window</strong><br>KeyedStream &rarr; WindowedStream</td>
          <td>
            <p>
             Defines a window of 5 seconds, that "slides" by 1 seconds. This means that elements are
             grouped according to their timestamp in groups of 5 second duration, and elements can belong to more than
             one window (since windows overlap by at least 4 seconds)
             The notion of time used is controlled by the `StreamExecutionEnvironment`.
      {% highlight java %}
        nonKeyedStream.timeWindowAll(Time.of(5, TimeUnit.SECONDS), Time.of(1, TimeUnit.SECONDS));
      {% endhighlight %}
            </p>
          </td>
        </tr>
      <tr>
        <td><strong>Tumbling count window</strong><br>KeyedStream &rarr; WindowedStream</td>
        <td>
          <p>
          Defines a window of 1000 elements, that "tumbles". This means that elements are
          grouped according to their arrival time (equivalent to processing time) in groups of 1000 elements,
          and every element belongs to exactly one window.
    {% highlight java %}
        nonKeyedStream.countWindowAll(1000)
    {% endhighlight %}
        </p>
        </td>
      </tr>
      <tr>
      <td><strong>Sliding count window</strong><br>KeyedStream &rarr; WindowedStream</td>
      <td>
        <p>
          Defines a window of 1000 elements, that "slides" every 100 elements. This means that elements are
          grouped according to their arrival time (equivalent to processing time) in groups of 1000 elements,
          and every element can belong to more than one window (as windows overlap by at least 900 elements).
  {% highlight java %}
    nonKeyedStream.countWindowAll(1000, 100)
  {% endhighlight %}
        </p>
      </td>
    </tr>
  </tbody>
</table>

</div>
</div>

[Back to top](#top)

Execution parameters
--------------------

### Fault tolerance

Flink has a checkpointing mechanism that recovers streaming jobs after failues. The checkpointing mechanism requires a *persistent* or *durable* source that
can be asked for prior records again (Apache Kafka is a good example of a durable source).

The checkpointing mechanism stores the progress in the source as well as the user-defined state (see [Working with State](#Stateful_computation))
consistently to provide *exactly once* processing guarantees.

To enable checkpointing, call `enableCheckpointing(n)` on the `StreamExecutionEnvironment`, where *n* is how often a checkpoint is taken in milliseconds.

Other parameters for checkpointing include:

- *Number of retries*: The `setNumberOfExecutionRerties()` method defines how many times the job is restarted after a failure.
  When checkpointing is activated, but this value is not explicitly set, the job is restarted infinitely often.
- *exactly-once vs. at-least-once*: You can optionally pass a mode to the `enableCheckpointing(n)` method to choose between the two guarantee levels.
  Exactly-once is preferrable for most applications. At-least-once may be relevant for certain super-low-latency (consistently few milliseconds) applications.

The [docs on streaming fault tolerance](../internals/stream_checkpointing.html) describe in detail the technique behind Flink's streaming fault tolerance mechanism.

While Flink can always guarantee exactly-once state updates to user-defined state, end-to-end (source to sink) guarantees depend
on the kind of source and sink. The following tables list the strongest consistency level that is provided by the
sources and sinks bundled with Flink.

| Source                | Strongest guarantees  | Notes |
|-----------------------|-----------------------|-------|
| Apache Kafka          | exactly once          | Use the appropriate [Kafka connector](#apache-kafka) |
| RabbitMQ              | at most once          | |
| Twitter Streaming API | at most once          | |
| Collection sources    | at most once          | |
| File sources          | at least once         | Restarts from beginning of the file |
| Socket sources        | at most once          | |

| Sink                  | Strongest guarantees  | Notes |
|-----------------------|-----------------------|-------|
| HDFS rolling sink     | exactly once          | Implementation depends on Hadoop version |
| Elasticsearch         | at least once         | Duplicates need to be handled in Elasticsearch
| File sinks            | at least once         | |
| Socket sinks          | at least once         | |
| Standard output       | at least once         | |

### Parallelism

You can control the number of parallel instances created for each operator by 
calling the `operator.setParallelism(int)` method.

### Buffer timeout

By default, elements are not transferred on the network one-by-one (which would cause unnecessary network traffic)
but are buffered. The size of the buffers (which are actually transferred between machines) can be set in the Flink config files.
While this method is good for optimizing throughput, it can cause latency issues when the incoming stream is not fast enough.
To control throughput and latency, you can use `env.setBufferTimeout(timeoutMillis)` on the execution environment
(or on individual operators) to set a maximum wait time for the buffers to fill up. After this time, the 
buffers are sent automatically even if they are not full. The default value for this timeout is 100 ms.

Usage:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
env.setBufferTimeout(timeoutMillis);

env.genereateSequence(1,10).map(new MyMapper()).setBufferTimeout(timeoutMillis);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment
env.setBufferTimeout(timeoutMillis)

env.genereateSequence(1,10).map(myMap).setBufferTimeout(timeoutMillis)
{% endhighlight %}
</div>
</div>

To maximize throughput, set `setBufferTimeout(-1)` which will remove the timeout and buffers will only be
flushed when they are full. To minimize latency, set the timeout to a value close to 0 (for example 5 or 10 ms). 
A buffer timeout of 0 should be avoided, because it can cause severe performance degradation.

[Back to top](#top)

Working with State
------------------

All transformations in Flink may look like functions (in the functional processing terminology), but
are in fact stateful operators. You can make *every* transformation (`map`, `filter`, etc) stateful
by declaring local variables or using Flink's state interface. You can register any local variable
as ***managed*** state by implementing an interface. In this case, and also in the case of using
Flink's native state interface, Flink will automatically take consistent snapshots of your state
periodically, and restore its value in the case of a failure.

The end effect is that updates to any form of state are the same under failure-free execution and
execution under failures. 

First, we look at how to make local variables consistent under failures, and then we look at
Flink's state interface.


### Making local variables consistent

Local variables can be made consistent by using the `Checkpointed` interface.

When the user defined function implements the `Checkpointed` interface, the `snapshotState(…)` and `restoreState(…)` 
methods will be executed to draw and restore function state.

In addition to that, user functions can also implement the `CheckpointNotifier` interface to receive notifications on 
completed checkpoints via the `notifyCheckpointComplete(long checkpointId)` method.
Note that there is no guarantee for the user function to receive a notification if a failure happens between c
heckpoint completion and notification. The notifications should hence be treated in a way that notifications from 
later checkpoints can subsume missing notifications.

For example the same counting, reduce function shown for `OperatorState`s by using the `Checkpointed` interface instead:

{% highlight java %}
public class CounterSum implements ReduceFunction<Long>, Checkpointed<Long> {

    //persistent counter
    private long counter = 0;

    @Override
    public Long reduce(Long value1, Long value2) throws Exception {
        counter++;
        return value1 + value2;
    }

    // regularly persists state during normal operation
    @Override
    public Serializable snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
        return new Long(counter);
    }

    // restores state on recovery from failure
    @Override
    public void restoreState(Serializable state) {
        counter = (Long) state;
    }
}
{% endhighlight %}

### Using the state interface

Flink supports two types of operator states: partitioned and non-partitioned states.

In case of non-partitioned operator state, an operator state is maintained for each parallel instance of a given operator. 
When `OperatorState.value()` is called, a separate state is returned in each parallel instance. 
In practice this means if we keep a counter for the received inputs in a mapper, `value()` will 
return the number of inputs processed by each parallel mapper.

In case of of partitioned operator state a separate state is maintained for each received key. 
This can be used for instance to count received inputs by different keys, or store and update summary 
statistics of different sub-streams.

Checkpointing of the states needs to be enabled from the `StreamExecutionEnvironment` using the `enableCheckpointing(…)` 
where additional parameters can be passed to modify the default 5 second checkpoint interval.

Operator states can be accessed from the `RuntimeContext` using the `getOperatorState(“name”, defaultValue, partitioned)` 
method so it is only accessible in `RichFunction`s. A recommended usage pattern is to retrieve the operator state in the `open(…)` 
method of the operator and set it as a field in the operator instance for runtime usage. Multiple `OperatorState`s 
can be used simultaneously by the same operator by using different names to identify them.

Partitioned operator state is only supported on `KeyedDataStreams`. 

By default operator states are checkpointed using default java serialization thus they need to be `Serializable`. 
The user can gain more control over the state checkpoint mechanism by passing a `StateCheckpointer` instance when retrieving
 the `OperatorState` from the `RuntimeContext`. The `StateCheckpointer` allows custom implementations for the checkpointing 
 logic for increased efficiency and to store arbitrary non-serializable states.

By default state checkpoints will be stored in-memory at the JobManager. Flink also supports storing the checkpoints on 
 Flink-supported file system which can be set in the flink-conf.yaml. 
 Note that the state backend must be accessible from the JobManager, use `file://` only for local setups.

For example let us write a reduce function that besides summing the data it also counts have many elements it has seen.

{% highlight java %}
public class CounterSum implements RichReduceFunction<Long> {
    
    //persistent counter
    private OperatorState<Long> counter;

    @Override
    public Long reduce(Long value1, Long value2) throws Exception {
        counter.update(counter.value() + 1);
        return value1 + value2;
    }

    @Override
    public void open(Configuration config) {
        counter = getRuntimeContext().getOperatorState(“counter”, 0L, false);
    }
}
{% endhighlight %} 

Stateful sources require a bit more care as opposed to other operators they are not data driven, but their `run(SourceContext)` methods potentially run infinitely. In order to make the updates to the state and output collection atomic the user is required to get a lock from the source's context.

{% highlight java %}
public static class CounterSource implements RichParallelSourceFunction<Long> {

    // utility for job cancellation
    private volatile boolean isRunning = false;
    
    // maintain the current offset for exactly once semantics
    private OperatorState<Long> offset;
    
    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        isRunning = true;
        Object lock = ctx.getCheckpointLock();
        
        while (isRunning) {
            // output and state update are atomic
            synchronized (lock){
                ctx.collect(offset);
                offset.update(offset.value() + 1);
            }
        }
    }

    @Override
    public void open(Configuration config) {
        offset = getRuntimeContext().getOperatorState(“offset”, 0L);
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
{% endhighlight %}

Some operators might need the information when a checkpoint is fully acknowledged by Flink to communicate that with the outside world. In this case see the `flink.streaming.api.checkpoint.CheckpointNotifier` interface.

### State checkpoints in iterative jobs

Fink currently only provides processing guarantees for jobs without iterations. Enabling checkpointing on an iterative job causes an exception. In order to force checkpointing on an iterative program the user needs to set a special flag when enabling checkpointing: `env.enableCheckpointing(interval, force = true)`.

Please note that records in flight in the loop edges (and the state changes associated with them) will be lost during failure.

[Back to top](#top)

Iterations
----------

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

Iterative streaming programs implement a step function and embed it into an `IterativeDataStream`. As a DataStream
program may never finish, there is no maximum number of iterations. Instead, you need to specify which part
of the stream is fed back to the iteration and which part is forwarded downstream using a `split` transformation
or a `filter`.

{% highlight java %}
IterativeDataStream<Integer> iteration = input.iterate();
{% endhighlight %}

The operator applied on the iteration starting point is the "head of the iteration", where data is received from the 
feedback loop that originates at the iteration "tail".

{% highlight java %}
DataStream<Integer> head = iteration.map(new IterationHead());
{% endhighlight %}

To close an iteration and define the iteration tail, call the `closeWith(feedbackStream)` method of the `IterativeDataStream`. 
The DataStream given to the `closeWith` function will be fed back to the iteration head. 
A common pattern is to use a filter to separate the output of the iteration from the feedback-stream.

{% highlight java %}
DataStream<Integer> tail = head.map(new IterationTail());

iteration.closeWith(tail.filter(isFeedback));

DataStream<Integer> output = tail.filter(isOutput);
{% endhighlight %}

In this case, all values passing the `isFeedback` filter will be fed back to the iteration head, and the values 
passing the `isOutput` filter will produce the output of the iteration that can be 
transformed further (here with a `map` and a `projection`) outside the iteration.

By default the partitioning of the feedback stream will be automatically set to be the same as the input of the 
iteration head. To override this the user can set an optional boolean flag in the `closeWith` method. 

#### Iteration head as a co-operator

The user can also treat the input and feedback stream of a streaming iteration as a `ConnectedDataStream`. 
This can be used to distinguish the feedback tuples and also to change the type of the iteration feedback. 

To use this feature the user needs to call the `withFeedbackType(type)` method of 
the iterative data stream and pass the type of the feedback stream:

{% highlight java %}
ConnectedIterativeDataStream<Integer, String> coiteration = source.iterate(maxWaitTimeMillis).withFeedbackType(“String”);

DataStream<String> head = coiteration.flatMap(new CoFlatMapFunction<Integer, String, String>(){})

iteration.closeWith(head);
{% endhighlight %}

In this case the original input of the head operator will be used as the first input to the co-operator and the feedback stream will be used as the second input.
</div>
</div>

[Back to top](#top)

Connectors
----------

<!-- TODO: reintroduce flume -->
Connectors provide code for interfacing with various third-party systems.
Typically the connector packages consist of a source and sink class
(with the exception of Twitter where only a source is provided and Elasticsearch
where only a sink is provided).

Currently these systems are supported:

 * [Apache Kafka](https://kafka.apache.org/) (sink/source)
 * [Elasticsearch](https://elastic.co/) (sink)
 * [Hadoop FileSystem](http://hadoop.apache.org) (sink)
 * [RabbitMQ](http://www.rabbitmq.com/) (sink/source)
 * [Twitter Streaming API](https://dev.twitter.com/docs/streaming-apis) (source)

To run an application using one of these connectors, additional third party
components are usually required to be installed and launched, e.g. the servers
for the message queues. Further instructions for these can be found in the
corresponding subsections. [Docker containers](#docker-containers-for-connectors)
are also provided encapsulating these services to aid users getting started
with connectors.

### Apache Kafka

This connector provides access to event streams served by [Apache Kafka](https://kafka.apache.org/).

Flink provides special Kafka Connectors for reading and writing data to Kafka topics.
The Flink Kafka Consumer integrates with Flink's checkpointing mechanisms to provide different
processing guarantees (most importantly exactly-once guarantees).

For exactly-once processing Flink can not rely on the auto-commit capabilities of the Kafka consumers.
The Kafka consumer might commit offsets to Kafka which have not been processed successfully.

Flink provides different connector implementations for different use-cases and environments.

Please pick a package (maven artifact id) and class name for your use-case and environment. For most users, the `flink-connector-kafka-083` package and the `FlinkKafkaConsumer082` class are appropriate.

| Package                     | Supported Since | Class | Kafka Version | Allows exactly once processing | Notes |
| -------------               |-------------| -----| ------ | ------ |
| flink-connector-kafka       | 0.9, 0.10 | `KafkaSource` | 0.8.1, 0.8.2 | **No**, does not participate in checkpointing at all. | Uses the old, high level KafkaConsumer API, autocommits to ZK by Kafka |
| flink-connector-kafka       | 0.9, 0.10 | `PersistentKafkaSource` | 0.8.1, 0.8.2 | **No**, does not guarantee exactly-once processing, element order or strict partition assignment | Uses the old, high level KafkaConsumer API, offsets are committed into ZK manually |
| flink-connector-kafka-083   | 0.9.1 0.10 | `FlinkKafkaConsumer081` | 0.8.1  | **yes** | Uses the [SimpleConsumer](https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+SimpleConsumer+Example) API of Kafka internally. Offsets are committed to ZK manually |
| flink-connector-kafka-083   | 0.9.1 0.10 | `FlinkKafkaConsumer082` | 0.8.2  | **yes** | Uses the [SimpleConsumer](https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+SimpleConsumer+Example) API of Kafka internally. Offsets are committed to ZK manually |

Then, import the connector in your maven project:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-kafka</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

Note that the streaming connectors are currently not part of the binary distribution. See linking with them for cluster execution [here](cluster_execution.html#linking-with-modules-not-contained-in-the-binary-distribution).

#### Installing Apache Kafka
* Follow the instructions from [Kafka's quickstart](https://kafka.apache.org/documentation.html#quickstart) to download the code and launch a server (launching a Zookeeper and a Kafka server is required every time before starting the application).
* On 32 bit computers [this](http://stackoverflow.com/questions/22325364/unrecognized-vm-option-usecompressedoops-when-running-kafka-from-my-ubuntu-in) problem may occur.
* If the Kafka and Zookeeper servers are running on a remote machine, then the `advertised.host.name` setting in the `config/server.properties` file the  must be set to the machine's IP address.

#### Kafka Source
The standard `FlinkKafkaConsumer082` is a Kafka consumer providing access to one topic.

The following parameters have to be provided for the `FlinkKafkaConsumer082(...)` constructor:

1. The topic name
2. A DeserializationSchema
3. Properties for the Kafka consumer.
  The following properties are required:
  - "bootstrap.servers" (comma separated list of Kafka brokers)
  - "zookeeper.connect" (comma separated list of Zookeeper servers)
  - "group.id" the id of the consumer group

Example:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Properties properties = new Properties();
properties.setProperty("bootstrap.servers", "localhost:9092");
properties.setProperty("zookeeper.connect", "localhost:2181");
properties.setProperty("group.id", "test");
DataStream<String> stream = env
	.addSource(new FlinkKafkaConsumer082<>("topic", new SimpleStringSchema(), properties))
	.print();
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val properties = new Properties();
properties.setProperty("bootstrap.servers", "localhost:9092");
properties.setProperty("zookeeper.connect", "localhost:2181");
properties.setProperty("group.id", "test");
stream = env
    .addSource(new KafkaSource[String]("topic", new SimpleStringSchema(), properties))
    .print
{% endhighlight %}
</div>
</div>

#### Kafka Consumers and Fault Tolerance
As Kafka persists all the data, a fault tolerant Kafka consumer can be provided.

The FlinkKafkaConsumer082 can read a topic, and if the job fails for some reason, the source will
continue on reading from where it left off after a restart.
For example if there are 3 partitions in the topic with offsets 31, 122, 110 read at the time of job
failure, then at the time of restart it will continue on reading from those offsets, no matter whether these partitions have new messages.

To use fault tolerant Kafka Consumers, checkpointing of the topology needs to be enabled at the execution environment:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(5000);
{% endhighlight %}
</div>
</div>

Also note that Flink can only restart the topology if enough processing slots are available to restart the topology.
So if the topology fails due to loss of a TaskManager, there must still be enough slots available afterwards.
Flink on YARN supports automatic restart of lost YARN containers.


#### Kafka Sink
A class providing an interface for sending data to Kafka.

The following arguments have to be provided for the `KafkaSink(…)` constructor in order:

1. Broker address (in hostname:port format, can be a comma separated list)
2. The topic name
3. Serialization schema

Example:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
stream.addSink(new KafkaSink<String>("localhost:9092", "test", new SimpleStringSchema()));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
stream.addSink(new KafkaSink[String]("localhost:9092", "test", new SimpleStringSchema))
{% endhighlight %}
</div>
</div>

The user can also define custom Kafka producer configuration for the KafkaSink with the constructor:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public KafkaSink(String zookeeperAddress, String topicId, Properties producerConfig,
      SerializationSchema<IN, byte[]> serializationSchema)
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
public KafkaSink(String zookeeperAddress, String topicId, Properties producerConfig,
      SerializationSchema serializationSchema)
{% endhighlight %}
</div>
</div>

If this constructor is used, the user needs to make sure to set the broker(s) with the "metadata.broker.list" property. Also the serializer configuration should be left default, the serialization should be set via SerializationSchema.

More about Kafka can be found [here](https://kafka.apache.org/documentation.html).

[Back to top](#top)

### Elasticsearch

This connector provides a Sink that can write to an
[Elasticsearch](https://elastic.co/) Index. To use this connector, add the
following dependency to your project:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-elasticsearch</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

Note that the streaming connectors are currently not part of the binary
distribution. See
[here](cluster_execution.html#linking-with-modules-not-contained-in-the-binary-distribution)
for information about how to package the program with the libraries for
cluster execution.

#### Installing Elasticsearch

Instructions for setting up an Elasticsearch cluster can be found
[here](https://www.elastic.co/guide/en/elasticsearch/reference/current/setup.html).
Make sure to set and remember a cluster name. This must be set when
creating a Sink for writing to your cluster

#### Elasticsearch Sink
The connector provides a Sink that can send data to an Elasticsearch Index.

The sink can use two different methods for communicating with Elasticsearch:

1. An embedded Node
2. The TransportClient

See [here](https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/client.html)
for information about the differences between the two modes.

This code shows how to create a sink that uses an embedded Node for
communication:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<String> input = ...;

Map<String, String> config = Maps.newHashMap();
// This instructs the sink to emit after every element, otherwise they would be buffered
config.put("bulk.flush.max.actions", "1");
config.put("cluster.name", "my-cluster-name");

input.addSink(new ElasticsearchSink<>(config, new IndexRequestBuilder<String>() {
    @Override
    public IndexRequest createIndexRequest(String element, RuntimeContext ctx) {
        Map<String, Object> json = new HashMap<>();
        json.put("data", element);

        return Requests.indexRequest()
                .index("my-index")
                .type("my-type")
                .source(json);
    }
}));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val input: DataStream[String] = ...

val config = new util.HashMap[String, String]
config.put("bulk.flush.max.actions", "1")
config.put("cluster.name", "my-cluster-name")

text.addSink(new ElasticsearchSink(config, new IndexRequestBuilder[String] {
  override def createIndexRequest(element: String, ctx: RuntimeContext): IndexRequest = {
    val json = new util.HashMap[String, AnyRef]
    json.put("data", element)
    println("SENDING: " + element)
    Requests.indexRequest.index("my-index").`type`("my-type").source(json)
  }
}))
{% endhighlight %}
</div>
</div>

Not how a Map of Strings is used to configure the Sink. The configuration keys
are documented in the Elasticsearch documentation
[here](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html).
Especially important is the `cluster.name` parameter that must correspond to
the name of your cluster.

Internally, the sink uses a `BulkProcessor` to send index requests to the cluster.
This will buffer elements before sending a request to the cluster. The behaviour of the
`BulkProcessor` can be configured using these config keys:
 * **bulk.flush.max.actions**: Maximum amount of elements to buffer
 * **bulk.flush.max.size.mb**: Maximum amount of data (in megabytes) to buffer
 * **bulk.flush.interval.ms**: Interval at which to flush data regardless of the other two
  settings in milliseconds

This example code does the same, but with a `TransportClient`:
<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<String> input = ...;

Map<String, String> config = Maps.newHashMap();
// This instructs the sink to emit after every element, otherwise they would be buffered
config.put("bulk.flush.max.actions", "1");
config.put("cluster.name", "my-cluster-name");

List<TransportAddress> transports = new ArrayList<String>();
transports.add(new InetSocketTransportAddress("node-1", 9300));
transports.add(new InetSocketTransportAddress("node-2", 9300));

input.addSink(new ElasticsearchSink<>(config, transports, new IndexRequestBuilder<String>() {
    @Override
    public IndexRequest createIndexRequest(String element, RuntimeContext ctx) {
        Map<String, Object> json = new HashMap<>();
        json.put("data", element);

        return Requests.indexRequest()
                .index("my-index")
                .type("my-type")
                .source(json);
    }
}));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val input: DataStream[String] = ...

val config = new util.HashMap[String, String]
config.put("bulk.flush.max.actions", "1")
config.put("cluster.name", "my-cluster-name")

val transports = new ArrayList[String]
transports.add(new InetSocketTransportAddress("node-1", 9300))
transports.add(new InetSocketTransportAddress("node-2", 9300))

text.addSink(new ElasticsearchSink(config, transports, new IndexRequestBuilder[String] {
  override def createIndexRequest(element: String, ctx: RuntimeContext): IndexRequest = {
    val json = new util.HashMap[String, AnyRef]
    json.put("data", element)
    println("SENDING: " + element)
    Requests.indexRequest.index("my-index").`type`("my-type").source(json)
  }
}))
{% endhighlight %}
</div>
</div>

The difference is that we now need to provide a list of Elasticsearch Nodes
to which the sink should connect using a `TransportClient`.

More about information about Elasticsearch can be found [here](https://elastic.co).

[Back to top](#top)

### Hadoop FileSystem

This connector provides a Sink that writes rolling files to any filesystem supported by
Hadoop FileSystem. To use this connector, add the
following dependency to your project:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-filesystem</artifactId>
  <version>{{site.version}}</version>
</dependency>
{% endhighlight %}

Note that the streaming connectors are currently not part of the binary
distribution. See
[here](cluster_execution.html#linking-with-modules-not-contained-in-the-binary-distribution)
for information about how to package the program with the libraries for
cluster execution.

#### Rolling File Sink

The rolling behaviour as well as the writing can be configured but we will get to that later.
This is how you can create a default rolling sink:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<String> input = ...;

input.addSink(new RollingSink<String>("/base/path"));

{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val input: DataStream[String] = ...

input.addSink(new RollingSink("/base/path"))

{% endhighlight %}
</div>
</div>

The only required parameter is the base path where the rolling files (buckets) will be
stored. The sink can be configured by specifying a custom bucketer, writer and batch size.

By default the rolling sink will use the pattern `"yyyy-MM-dd--HH"` to name the rolling buckets.
This pattern is passed to `SimpleDateFormat` with the current system time to form a bucket path. A
new bucket will be created whenever the bucket path changes. For example, if you have a pattern
that contains minutes as the finest granularity you will get a new bucket every minute.
Each bucket is itself a directory that contains several part files: Each parallel instance
of the sink will create its own part file and when part files get too big the sink will also
create a new part file next to the others. To specify a custom bucketer use `setBucketer()`
on a `RollingSink`.

The default writer is `StringWriter`. This will call `toString()` on the incoming elements
and write them to part files, separated by newline. To specify a custom writer use `setWriter()`
on a `RollingSink`. If you want to write Hadoop SequenceFiles you can use the provided
`SequenceFileWriter` which can also be configured to use compression.

The last configuration option is the batch size. This specifies when a part file should be closed
and a new one started. (The default part file size is 384 MB).

Example:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<Tuple2<IntWritable,Text>> input = ...;

RollingSink sink = new RollingSink<String>("/base/path");
sink.setBucketer(new DateTimeBucketer("yyyy-MM-dd--HHmm"));
sink.setWriter(new SequenceFileWriter<IntWritable, Text>());
sink.setBatchSize(1024 * 1024 * 400); // this is 400 MB,

input.addSink(sink);

{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val input: DataStream[Tuple2[IntWritable, Text]] = ...

val sink = new RollingSink[String]("/base/path")
sink.setBucketer(new DateTimeBucketer("yyyy-MM-dd--HHmm"))
sink.setWriter(new SequenceFileWriter[IntWritable, Text]())
sink.setBatchSize(1024 * 1024 * 400) // this is 400 MB,

input.addSink(sink)

{% endhighlight %}
</div>
</div>

This will create a sink that writes to bucket files that follow this schema:

```
/base/path/{date-time}/part-{parallel-task}-{count}
```

Where `date-time` is the string that we get from the date/time format, `parallel-task` is the index
of the parallel sink instance and `count` is the running number of part files that where created
because of the batch size.

For in-depth information, please refer to the JavaDoc for
[RollingSink](http://flink.apache.org/docs/latest/api/java/org/apache/flink/streaming/connectors/fs/RollingSink.html).

[Back to top](#top)

### RabbitMQ

This connector provides access to data streams from [RabbitMQ](http://www.rabbitmq.com/). To use this connector, add the following dependency to your project:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-rabbitmq</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

Note that the streaming connectors are currently not part of the binary distribution. See linking with them for cluster execution [here](cluster_execution.html#linking-with-modules-not-contained-in-the-binary-distribution).

#### Installing RabbitMQ
Follow the instructions from the [RabbitMQ download page](http://www.rabbitmq.com/download.html). After the installation the server automatically starts, and the application connecting to RabbitMQ can be launched.

#### RabbitMQ Source

A class providing an interface for receiving data from RabbitMQ.

The followings have to be provided for the `RMQSource(…)` constructor in order:

1. The hostname
2. The queue name
3. Deserialization schema

Example:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<String> stream = env
	.addSource(new RMQSource<String>("localhost", "hello", new SimpleStringSchema()))
	.print
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
stream = env
    .addSource(new RMQSource[String]("localhost", "hello", new SimpleStringSchema))
    .print
{% endhighlight %}
</div>
</div>

#### RabbitMQ Sink
A class providing an interface for sending data to RabbitMQ.

The followings have to be provided for the `RMQSink(…)` constructor in order:

1. The hostname
2. The queue name
3. Serialization schema

Example:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
stream.addSink(new RMQSink<String>("localhost", "hello", new StringToByteSerializer()));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
stream.addSink(new RMQSink[String]("localhost", "hello", new StringToByteSerializer))
{% endhighlight %}
</div>
</div>

More about RabbitMQ can be found [here](http://www.rabbitmq.com/).

[Back to top](#top)

### Twitter Streaming API

Twitter Streaming API provides opportunity to connect to the stream of tweets made available by Twitter. Flink Streaming comes with a built-in `TwitterSource` class for establishing a connection to this stream. To use this connector, add the following dependency to your project:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-twitter</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

Note that the streaming connectors are currently not part of the binary distribution. See linking with them for cluster execution [here](cluster_execution.html#linking-with-modules-not-contained-in-the-binary-distribution).

#### Authentication
In order to connect to Twitter stream the user has to register their program and acquire the necessary information for the authentication. The process is described below.

#### Acquiring the authentication information
First of all, a Twitter account is needed. Sign up for free at [twitter.com/signup](https://twitter.com/signup) or sign in at Twitter's [Application Management](https://apps.twitter.com/) and register the application by clicking on the "Create New App" button. Fill out a form about your program and accept the Terms and Conditions.
After selecting the application, the API key and API secret (called `consumerKey` and `sonsumerSecret` in `TwitterSource` respectively) is located on the "API Keys" tab. The necessary access token data (`token` and `secret`) can be acquired here.
Remember to keep these pieces of information a secret and do not push them to public repositories.

#### Accessing the authentication information
Create a properties file, and pass its path in the constructor of `TwitterSource`. The content of the file should be similar to this:

~~~bash
#properties file for my app
secret=***
consumerSecret=***
token=***-***
consumerKey=***
~~~

#### Constructors
The `TwitterSource` class has two constructors.

1. `public TwitterSource(String authPath, int numberOfTweets);`
to emit finite number of tweets
2. `public TwitterSource(String authPath);`
for streaming

Both constructors expect a `String authPath` argument determining the location of the properties file containing the authentication information. In the first case, `numberOfTweets` determine how many tweet the source emits.

#### Usage
In contrast to other connectors, the `TwitterSource` depends on no additional services. For example the following code should run gracefully:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<String> streamSource = env.addSource(new TwitterSource("/PATH/TO/myFile.properties"));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
streamSource = env.addSource(new TwitterSource("/PATH/TO/myFile.properties"))
{% endhighlight %}
</div>
</div>

The `TwitterSource` emits strings containing a JSON code.
To retrieve information from the JSON code you can add a FlatMap or a Map function handling JSON code. For example, there is an implementation `JSONParseFlatMap` abstract class among the examples. `JSONParseFlatMap` is an extension of the `FlatMapFunction` and has a

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
String getField(String jsonText, String field);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
getField(jsonText : String, field : String) : String
{% endhighlight %}
</div>
</div>

function which can be use to acquire the value of a given field.

There are two basic types of tweets. The usual tweets contain information such as date and time of creation, id, user, language and many more details. The other type is the delete information.

#### Example
`TwitterLocal` is an example how to use `TwitterSource`. It implements a language frequency counter program.

[Back to top](#top)

### Docker containers for connectors

A Docker container is provided with all the required configurations for test running the connectors of Apache Flink. The servers for the message queues will be running on the docker container while the example topology can be run on the user's computer.

#### Installing Docker
The official Docker installation guide can be found [here](https://docs.docker.com/installation/).
After installing Docker an image can be pulled for each connector. Containers can be started from these images where all the required configurations are set.

#### Creating a jar with all the dependencies
For the easiest setup, create a jar with all the dependencies of the *flink-streaming-connectors* project.

~~~bash
cd /PATH/TO/GIT/flink/flink-staging/flink-streaming-connectors
mvn assembly:assembly
~~~bash

This creates an assembly jar under *flink-streaming-connectors/target*.

#### RabbitMQ
Pull the docker image:

~~~bash
sudo docker pull flinkstreaming/flink-connectors-rabbitmq
~~~

To run the container, type:

~~~bash
sudo docker run -p 127.0.0.1:5672:5672 -t -i flinkstreaming/flink-connectors-rabbitmq
~~~

Now a terminal has started running from the image with all the necessary configurations to test run the RabbitMQ connector. The -p flag binds the localhost's and the Docker container's ports so RabbitMQ can communicate with the application through these.

To start the RabbitMQ server:

~~~bash
sudo /etc/init.d/rabbitmq-server start
~~~

To launch the example on the host computer, execute:

~~~bash
java -cp /PATH/TO/JAR-WITH-DEPENDENCIES org.apache.flink.streaming.connectors.rabbitmq.RMQTopology \
> log.txt 2> errorlog.txt
~~~

There are two connectors in the example. One that sends messages to RabbitMQ, and one that receives messages from the same queue. In the logger messages, the arriving messages can be observed in the following format:

~~~
<DATE> INFO rabbitmq.RMQTopology: String: <one> arrived from RMQ
<DATE> INFO rabbitmq.RMQTopology: String: <two> arrived from RMQ
<DATE> INFO rabbitmq.RMQTopology: String: <three> arrived from RMQ
<DATE> INFO rabbitmq.RMQTopology: String: <four> arrived from RMQ
<DATE> INFO rabbitmq.RMQTopology: String: <five> arrived from RMQ
~~~

#### Apache Kafka

Pull the image:

~~~bash
sudo docker pull flinkstreaming/flink-connectors-kafka
~~~

To run the container type:

~~~bash
sudo docker run -p 127.0.0.1:2181:2181 -p 127.0.0.1:9092:9092 -t -i \
flinkstreaming/flink-connectors-kafka
~~~

Now a terminal has started running from the image with all the necessary configurations to test run the Kafka connector. The -p flag binds the localhost's and the Docker container's ports so Kafka can communicate with the application through these.
First start a zookeeper in the background:

~~~bash
/kafka_2.9.2-0.8.1.1/bin/zookeeper-server-start.sh /kafka_2.9.2-0.8.1.1/config/zookeeper.properties \
> zookeeperlog.txt &
~~~

Then start the kafka server in the background:

~~~bash
/kafka_2.9.2-0.8.1.1/bin/kafka-server-start.sh /kafka_2.9.2-0.8.1.1/config/server.properties \
 > serverlog.txt 2> servererr.txt &
~~~

To launch the example on the host computer execute:

~~~bash
java -cp /PATH/TO/JAR-WITH-DEPENDENCIES org.apache.flink.streaming.connectors.kafka.KafkaTopology \
> log.txt 2> errorlog.txt
~~~


In the example there are two connectors. One that sends messages to Kafka, and one that receives messages from the same queue. In the logger messages, the arriving messages can be observed in the following format:

~~~
<DATE> INFO kafka.KafkaTopology: String: (0) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (1) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (2) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (3) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (4) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (5) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (6) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (7) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (8) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (9) arrived from Kafka
~~~


[Back to top](#top)