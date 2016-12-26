## Dec 4th
- Change data type from Tuple2(Int,Int) to Tuple2(Long,Int) to get `indexEntriesPerSegment` as a exponential of 2.
- Introduce `MySorter` and  `MyMemorySegment`.
- `MyMemorySegment` is inherit from Flink's `MemorySegment` and provide
  optimized low-level methods which HeapMemoerySegment doesn't provide. It will be used in both `NormalizedKeySorter` and `MySorter`,
  and only `MySorter` uses the optimized methods.

```
# Using bit-wise operations instead of dividing and modulo in compare and swap function.
# commit: 1ba7f78846b95334be8c32d8d1338651836e46ec

Benchmark                   (noRecords)                                                (sorterClass)  Mode  Samples   Score  Score error  Units
o.e.Benchmarker.testSort          10000  org.apache.flink.runtime.operators.sort.NormalizedKeySorter    ss      100   5.427        0.282     ms
o.e.Benchmarker.testSort          10000                                         org.example.MySorter    ss      100   4.580        0.302     ms
o.e.Benchmarker.testSort          20000  org.apache.flink.runtime.operators.sort.NormalizedKeySorter    ss      100  11.594        0.392     ms
o.e.Benchmarker.testSort          20000                                         org.example.MySorter    ss      100   9.515        0.368     ms
o.e.Benchmarker.testSort          50000  org.apache.flink.runtime.operators.sort.NormalizedKeySorter    ss      100  22.092        1.202     ms
o.e.Benchmarker.testSort          50000                                         org.example.MySorter    ss      100  16.745        1.324     ms
```


## TODO
- [x] create a main class that take `sort-class`, `input-file` as input and output sorting stat and sorted output
- [x] set up automatic test for checking correctness when building ( varying input size also )
- [x] Hot coding on local and IBM cluster
- [ ] Report ...
- [ ] http://janino-compiler.github.io/janino/

### Questions
- ~Reduce `pageSize`, score is much lower. is that strange?
- ~What exactly are `buffer`, `segment` in MemorySegment?
- ~Is there a way to copy `sorter`, so we don't have to instantiate it every iteration?
- What is the 8 bytes prefix in IndexEntry? a pointer to original record?
