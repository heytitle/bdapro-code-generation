## Dec 4th
- Change data type from Tuple2(Int,Int) to Tuple2(Long,Int) to get `indexEntriesPerSegment` as a exponential of 2.


```
# Using bit-wise operations instead of dividing and modulo in compare and swap function.

Benchmark                   (noRecords)                                                (sorterClass)  Mode  Samples   Score  Score error  Units
o.e.Benchmarker.testSort          10000  org.apache.flink.runtime.operators.sort.NormalizedKeySorter    ss      100   5.334        0.248     ms
o.e.Benchmarker.testSort          10000                                         org.example.MySorter    ss      100   5.124        0.219     ms
o.e.Benchmarker.testSort          20000  org.apache.flink.runtime.operators.sort.NormalizedKeySorter    ss      100  12.582        0.940     ms
o.e.Benchmarker.testSort          20000                                         org.example.MySorter    ss      100  12.146        0.685     ms
o.e.Benchmarker.testSort          50000  org.apache.flink.runtime.operators.sort.NormalizedKeySorter    ss      100  22.348        1.327     ms
o.e.Benchmarker.testSort          50000                                         org.example.MySorter    ss      100  21.993        1.217     ms
```
