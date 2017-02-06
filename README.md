[![Build Status](https://travis-ci.org/heytitle/flink-sorter-performance-evaluation.svg?branch=master)](https://travis-ci.org/heytitle/flink-sorter-performance-evaluation)

## Build test app
```
$ mvn clean compile assembly:single
```

## Run app
```
JAVA_DEBUG=true \
WAIT_FOR_START=true \
java -XX:+UnlockCommercialFeatures -XX:+FlightRecorder -jar target/bdapro-code-generation-1.0-jar-with-dependencies.jar <SORTER_CLASS>
```

## Build benchmark
```
$ mvn clean install
```

## Run benchmark
```
$ java -jar target/benchmarks.jar
```

example output
```
# Run progress: 90.00% complete, ETA 00:00:08
# Fork: 10 of 10
# Warmup Iteration   1: 184.304 ops/s
# Warmup Iteration   2: 215.557 ops/s
Iteration   1: 217.208 ops/s
Iteration   2: 213.576 ops/s
Iteration   3: 216.138 ops/s
Iteration   4: 216.152 ops/s
Iteration   5: 204.705 ops/s


Result: 212.969 ±(99.9%) 1.922 ops/s [Average]
  Statistics: (min, avg, max) = (203.144, 212.969, 218.493), stdev = 3.882
  Confidence interval (99.9%): [211.047, 214.891]


# Run complete. Total time: 00:01:27

Benchmark                             Mode  Samples    Score  Score error  Units
o.e.Benchmarker.normalizedKeySort    thrpt       50  212.969        1.922  ops/s
```
