#Motivation
Apache Flink currently uses `NormalizedKeySorter` to sort data. However, the
sorter doesn't not aware of length of the sorting key, especially use of low-level methods during comparison and swapping keys.

##Current Flink's sorter performance
![img](http://i.imgur.com/tI9Awpi.png)
Testing on `IBM Machine` with `4GB` Heap size and big endian architecture.

As can be seen from the figure, `Unsafe.copyMemory` calls completely dominate sorting performance.

```
$ JAVA_DEBUG=true \
WAIT_FOR_START=true \
java -Dcom.sun.management.jmxremote.port=3333 \
  -Dcom.sun.management.jmxremote.ssl=false \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Xmx4g \
  -jar bdapro-code-generation-1.0-jar-with-dependencies.jar
```

# Improvement
## Assumption
We assume that the underlying records are `Tuple2<Long,Int>`. As a result, `indexEntrySize` is `16` bytes.

## What we can do better (FLINK-4867)
- Eliminate use of `copyMemory`. Under the assumption, we can use use `getLong` and `putLong` instead.
More info : https://github.com/heytitle/bdapro-code-generation/blob/master/src/main/java/org/apache/flink/core/memory/MyMemorySegment.java#L130
- `while` in `compare` can be unrolled because we know the exact number of iterations.
More info : https://github.com/heytitle/bdapro-code-generation/blob/master/src/main/java/org/apache/flink/core/memory/MyMemorySegment.java#L144
- [DOING] Virtual method calls
- [DOING] Eliminate reversing bytes for little endianness machine

##Result
### EC2 m4.large
```
# openjdk-8
# Run complete. Total time: 00:26:25

Benchmark                   (noRecords)                                                (sorterClass)  Mode  Samples    Score  Score error  Units
o.e.Benchmarker.testSort          10000  org.apache.flink.runtime.operators.sort.NormalizedKeySorter    ss     1000    5.713        0.214     ms
o.e.Benchmarker.testSort          10000                                         org.example.MySorter    ss     1000    3.514        0.204     ms
o.e.Benchmarker.testSort         100000  org.apache.flink.runtime.operators.sort.NormalizedKeySorter    ss     1000   59.265        0.712     ms
o.e.Benchmarker.testSort         100000                                         org.example.MySorter    ss     1000   33.672        0.729     ms
o.e.Benchmarker.testSort        1000000  org.apache.flink.runtime.operators.sort.NormalizedKeySorter    ss     1000  615.143        0.621     ms
o.e.Benchmarker.testSort        1000000                                         org.example.MySorter    ss     1000  335.605        1.996     ms
```

### IBM Machine
```
# openjdk version "1.8.0_101"
# Run complete. Total time: 01:05:32

Benchmark                   (noRecords)                                                (sorterClass)  Mode  Samples     Score  Score error  Units
o.e.Benchmarker.testSort          10000  org.apache.flink.runtime.operators.sort.NormalizedKeySorter    ss     1000    21.575        0.423     ms
o.e.Benchmarker.testSort          10000                                         org.example.MySorter    ss     1000     4.669        0.083     ms
o.e.Benchmarker.testSort         100000  org.apache.flink.runtime.operators.sort.NormalizedKeySorter    ss     1000   170.871        0.849     ms
o.e.Benchmarker.testSort         100000                                         org.example.MySorter    ss     1000    30.557        0.284     ms
o.e.Benchmarker.testSort        1000000  org.apache.flink.runtime.operators.sort.NormalizedKeySorter    ss     1000  1989.859        1.621     ms
o.e.Benchmarker.testSort        1000000                                         org.example.MySorter    ss     1000   344.326        0.884     ms
```
- IBM machine is much slower than ec2 m4.large. maybe because endianness?

##Conclusion
