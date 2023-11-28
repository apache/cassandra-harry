
# Harry, a fuzz testing tool for Apache Cassandra

The project aims to generate _reproducible_ workloads that are as close to real-life as possible, while being able to
_efficiently_ verify the cluster state against the model without pausing the workload itself.

## Getting Started in under 5 minutes

Harry can operate as a straightforward read/write "correctness stress tool" that will check to ensure reads on a cluster
are consistent with what it knows it wrote to the cluster. You have a couple options for this.

### Option 1: Test a local cassandra branch using harry-stress.sh

1. Start your local cassandra node
2. Use harry-stress.sh:

```bash
./harry-stress.sh --help

usage: harry-stress.sh [args]
  -f --flag           Flag last run as a failure (dir name + touch file indicating)
  -t --title          Optional title to decorate the results directory with internally in 'title.txt'
  -d --duration       Time in ms to run test (default 60000)
  -m --minutes        Alternate duration in minutes
  -r --read           Read threads. default 2
  -w --write          Write threads. default 2
  -s --seed           seed. Defaults to random in 0-1e13
  -c --conf           .yaml config file. Defaults to conf/external.yaml
  -v --validate       validation type. Defaults to validate-all
```

An example run:
`./harry-stress.sh -s 1234 -r 3 -w 3 -t my_custom_branch_test_1 -m 5`

The above will run a test using seed 1234 with 3 read threads, 3 write threads, for 5 minutes, and record the output (
seed, command, and stress output) in `results/my_custom_branch_test_1`, validating that everything it wrote it finds in
the database upon read. If there's a failure, you can rapidly flag the last test run as a failure
with `./harry-stress.sh -f`

Further config params can be found in `conf/external.yaml` or whatever custom .yaml file you've pointed to via
the `-c | --conf` flag including things like schema, partition sizes, timestamp generation rules, authentication params,
ratios for different mutation query types, and more.

### Option 2: Running things manually lower in the stack:

The make file has a stress target where you can more directly access all available ARGS rather than restricting yourself
to the convenience script above. If you're using an external cluster (i.e. `./bin/cassandra -f`, CCM, docker,
kubernetes, or just a deployed cluster), the mini stress tool can be used directly as follows:

```bash
# In one terminal; this is needed for the local cassandra dependencies
$ make run-cassandra

# In another terminal
$ make stress ARGS="conf/external.yaml --read=2 --write=2 --duration=60000 --validate-all-local"
```

This command will start a client that performs a concurrent read/write workload, 2 read and 2 write threads for 60
seconds. Much like the hary-stress.sh script, if the runner sees any inconsistencies it will inform you accordingly.

An equivalent for an in-jvm cluster is:

```
try (Cluster cluster = builder().withNodes(3)
                                .start())
{
    SchemaSpec schema = new SchemaSpec("harry", "test_table",
                                       asList(pk("pk1", asciiType), pk("pk1", int64Type)),
                                       asList(ck("ck1", asciiType), ck("ck1", int64Type)),
                                       asList(regularColumn("regular1", asciiType), regularColumn("regular1", int64Type)),
                                       asList(staticColumn("static1", asciiType), staticColumn("static1", int64Type)));

    Configuration config = Configuration.fromFile("conf/example.yaml")
                                        .unbuild()
                                        .setKeyspaceDdl(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': %d};", schema.keyspace, 3))
                                        .setSUT(() -> new InJvmSut(cluster))
                                        .build();

    Run run = config.createRun();

    concurrent(run, config,
               asList(pool("Writer", 2, MutatingVisitor::new),
                      pool("Reader", 2, RandomPartitionValidator::new)),
               2, TimeUnit.MINUTES)
    .run();
}
```

Running sequential read/write workload (i.e. reads strictly following writes, without overlaps), for more determinism is
possible with:

```
$ make stress ARGS="conf/external.yaml --write-before-read --duration=60000 --validate-all"
```

# I've found a falsification. What now?

There is no one-size-fits-all solution for debugging a falsification. We did try to create a shrinker, but unfortunately
without Simulator, shrinker only works for issues that are non-concurrent in nature, since there's no way to create a
stable repro otherwise. That said, there are several things that might get you started and inspire further ideas about
how to debug the issue.

First of all, understand whether or not the issue is likely to be concurrent in nature. If you re-run your test with the
same seed, but see no falsification, and it fails only sporadically, and often on different logical timestamp, it is
likely that the issue is, in fact concurrent. Here, it is important to note that when you are running concurrent
read/write workload, you will get different interleaving of reads and writes every time you do this. If you have reasons
to think that you're seeing the falsification because a read has queried a specific partition state, try re-running your
test with sequential runner (`--write-before-read`) if you are using ministress.

If you can get a stable repro with a sequential runner, you're in luck. Now all you need to do is to add logs everywhere
and understand what might be causing it. But even if you do not have a stable repro, you are still likely to follow the
same steps:

* Inspect the error itself. Do Cassandra-returned results make sense? Is anything out of order? Are there any duplicates
  or gaps?
* Switch to logging mutating visitor and closely inspect its output. Closely inspect the output of the model. Do the
  values make sense?
* Check the output of data tracker. Does the model or Cassandra have missing columns or rows? Do these outputs contain
  latest logical timestamps for each of the operations from the log? How about in-flight operations?
* Filter out relevant operation log entries and inspect them closely. Given these operations, does the output of the
  model, or output of the database make most sense?

Next, you might want to try to narrow down the scope of the problem. Depending on what the falsification looks like, use
your Cassandra knowledge to see what might apply in your situation:

* Try checking if changing schema to use different column types does anything.
* Try disabling range deletes, regular deletes, or column deletes.
* Try changing the size of partition and see if the issue still reproduces.
* Try disabling static columns.

To avoid listing every feature in Harry, it suffices to say you should try to enable/disable features that make sense
in the given context, and try to find the combination that avoids the failure, or a minimal combination that still
reproduces the issue. Your first goal should be to find a _stable repro_, even if it involves modifying Cassandra or
Harry, or taking the operations, and composing the repro manually. Having a stable repro will make finding a cause much
simpler. Sometimes you will find the cause before you have a stable repro, in which case, you _still_ have to produce a
stable repro to make things simpler for the reviewer, and to include it into the test suite of your patch.

Lastly, *be patient*. Debugging falsifications is often a multi-hour endeavour, and things do not always jump out at you,
so you might have to spend a significant amount of time tracking the problem down. Once you have found it, it is very
rewarding.

## Further Reading
* [Harry: An open-source fuzz testing and verification tool for Apache Cassandra](https://cassandra.apache.org/_/blog/Harry-an-Open-Source-Fuzz-Testing-and-Verification-Tool-for-Apache-Cassandra.html)

---
# Technical and Implementation Details

## System Under Test implementations

* `in_jvm/InJvmSut` - simple in-JVM-dtest system under test.
* `println/PrintlnSut` - system under test that prints to sdtout instead of executing queries on the cluster; useful for
  debugging.
* `mixed_in_jvm/MixedVersionInJvmSut` - in-JVM-dtest system under test that works with mixed version clusters.
* `external/ExternalClusterSut` - system under test that works with CCM, Docker, Kubernetes, or cluster you may. have
  deployed elsewhere

Both in-JVM SUTs have fault-injecting functionality available.

## Visitors

* `single/SingleValidator` - visitor that runs several different read queries against a single partition that is
  associated with current logical timestamp, and validates their results using given model.
* `all_partitions/AllPartitionsValidator` - concurrently validates all partitions that were visited during this run.
* `repair_and_validate_local_states/RepairingLocalStateValidator` - similar to `AllPartitionsValidator`, but performs
  repair before checking node states.
* `mutating/MutatingVisitor` - visitor that performs all sorts of mutations.
* `logging/LoggingVisitor` - similar to `MutatingVisitor`, but also logs all operations to a file; useful for debug
  purposes.
* `corrupting/CorruptingVisitor` - visitor that will deliberately change data in the partition it visits. Useful for
  negative tests (i.e. to ensure that your model actually detects data inconsistencies).

And more.

## Models

* `querying_no_op/QueryingNoOpValidator` - a model that can be used to "simply" run random queries.
* `quiescent_checker/QuiescentChecker` - a model that can be used to verify results of any read that has no writes to
  the same partition_ concurrent to it. Should be used in conjunction with locking data tracker.
* `quiescent_local_state_checker/QuiescentLocalStateChecker` - a model that can check local states of each replica that
  has to own

## Runners

* `sequential/SequentialRunner` - runs all visitors sequentially, in the loop, for a specified amount of time; useful
  for simple tests that do not have to exercise concurrent read/write path.
* `concurrent/ConcurrentRunner` - runs all visitors concurrently, each visitor in its own thread, looped, for a
  specified amount of time; useful for things like concurrent read/write workloads.
* `chain/ChainRunner` - receives other runners as input, and runs them one after another once. Useful for both simple
  and complex scenarios that involve both read/write workloads, validating all partitions, exercising other node-local
  or cluster-wide operations.
* `staged/StagedRunner` - receives other runners (stages) as input, and runs them one after another in a loop; useful
  for implementing complex scenarios, such as read/write workloads followed by some cluster changing operations.

## Clock

* `approximate_monotonic/ApproxomateMonotonicClock` - a timestamp supplier implementation that tries to keep as close to
  real time as possible, while preserving mapping from real-time to logical timestamps.
* `offset/OffsetClock` - a (monotonic) clock that supplies timestamps that do not have any relation to real time.

# Introduction

Harry has two primary modes of functionality:

* Unit test mode: in which you define specific sequences of
  operations and let Harry test these operations using different
  schemas and conditions.
* Exploratory/fuzz mode: in which you define distributions of events
  rather rather than sequences themselves, and let Harry try out
  different things.

Usually, in unit-test mode, we’re applying several write operations to the cluster state and then run different read
queries and validate their results. To learn more about writing unit tests, refer to the "Writing Unit Tests" section.

In exploratory mode, we continuously apply write operations to the cluster and validate their state, allowing data size
to grow and simulating real-life behaviour. To learn more about implementing test cases using fuzz mode, refer to the "
Implementing Tests" section of this guide, but it's likely you'll have to read the rest of this document to implement
more complex scenarios.

# Writing Unit Tests

To write unit tests with Harry, there's no special knowledge required. Usually, unit tests are written by simply
hardcoding the schema and then writing several modification statements one after the other, and then manually validating
results of a `SELECT` query. This might work for simple scenarios, but there’s still a chance that for some other schema
or some combination of values the tested feature may not work.

To improve the situation, we can express the test in more abstract terms and, instead of writing it using specific
statements, we can describe which statement _types_ are to be used:

```
test(new SchemaGenerators.Builder("harry")
                         .partitionKeySpec(1, 5)
                         .clusteringKeySpec(1, 5)
                         .regularColumnSpec(1, 10)
                         .generator(),
     historyBuilder -> {
         historyBuilder.nextPartition()
                       .simultaneously()
                       .randomOrder()
                         .partitionDeletion()
                         .rangeDeletion()
                       .finish();
     });
```

This spec can be used to generate clusters of different sizes, configured with different schemas, executing the given a
sequence of actions both in isolation, and combined with other randomly generated ones, with failure-injection.

Best of all is that this test will _not only_ ensure that such a sequence of actions does not produce an exception, but
also would ensure that cluster will respond with correct results to _any_ allowed read query.

`HistoryBuilder` is using the configuration provided by Harry `Run`, which can be written either
using `Configuration#ConfigurationBuilder` like we
did [here](https://github.com/apache/cassandra-harry/blob/ddd643ecc904258abe5e2f73d9b612793b0ac0e6/harry-integration/test/harry/model/IntegrationTestBase.java#L91),
or provided in
a [yaml file](https://github.com/apache/cassandra-harry/blob/ddd643ecc904258abe5e2f73d9b612793b0ac0e6/conf/example.yaml).

To begin specifying operations for a new partition, `HistoryBuilder#nextPartition` has to be called, which returns
a `PartitionBuilder`. For the commands within this partition, you have a choice between:

* `PartitionBuilder#simultaneously`, which will execute listed operations with the same timestamp
* `PartitionBuilder#sequentially`, which will execute listed operations with monotonically increasing timestamps, giving
  each operation its own timestamp

Similarly, you can choose between:

* `PartitionBuilder#randomOrder`, which will execute listed operations in random order
* `PartitionBuilder#strictOrder`, which will execute listed operations in the order specified by the user

The rest of operations are
self-explanatory: `#insert`, `#update`, `#delete`, `#columnDelete`, `#rangeDelete`, `#sliceDelete`, `#partitionDelete`,
and their plural counterparts.

After history generated by `HistoryBuilder` is replayed using `ReplayingVisitor`, you can use any
model (`QuiescentChecker` by default) to validate queries. Queries can be provided manually or generated
using `QueryGenerator` or `TypedQueryGenerator`.

# Basic Terminology

* Inflate / inflatable: a process of producing a value (for example, string, or a blob) from a `long` descriptor that
  uniquely identifies the value. See [data generation](https://github.com/apache/cassandra-harry#data-generation)
  section of this guide for more details.
* Deflate / deflatable: a process of producing the descriptor the value was inflated from during verification.
  See [model](https://github.com/apache/cassandra-harry#model) section of this guide for more details.

For definitions of logical timestamp, descriptor, and other entities used during inflation and deflation, refer
to [formal relationships](https://github.com/apache/cassandra-harry#formal-relations-between-entities) section.

# Features

Currently, Harry can exercise the following Cassandra functionality:

* Supported data types: `int8`, `int16`, `int32`, `int64`, `boolean`, `float`, `double`, `ascii`, `uuid`, `timestamp`.
  Collections are only _inflatable_.
* Random schema generation, with an arbitrary number of partition and clustering keys.
* Schemas with arbitrary `CLUSTERING ORDER BY`
* Randomly generated `INSERT` and `UPDATE` queries with all columns or arbitrary column subset
* Randomly generated `DELETE` queries: for a single column, single row, or a range of rows
* Inflating and validating entire partitions (with allowed in-flight queries)
* Inflating and validating random `SELECT` queries: single row, slices (with single open end), and ranges (with both
  ends of clusterings specified)

Inflating partitions is done
using [Reconciler](https://github.com/apache/cassandra-harry/blob/master/harry-core/src/harry/reconciler/Reconciler.java).
Validating partitions and random queries can be done
using [Quiescent Checker](https://github.com/apache/cassandra-harry/blob/master/harry-core/src/harry/model/QuiescentChecker.java)
and [Exhaustive Checker](https://github.com/apache/cassandra-harry/blob/master/harry-core/src/harry/model/ExhaustiveChecker.java).

## Outstanding Work

#### The following have not yet been implemented:

* Some types (such as collections) are not deflatable
* 2i queries are not implemented
* Fault injection is not implemented (available via Cassandra Simulator)
* TTL is not supported
* Some SELECT queries are not supported: `LIMIT`, `IN`, `GROUP BY`, token range queries

#### Some features can be improved upon or further optimized:

* Pagination is currently implemented but hard-coded to a page size of 1
* RNG should be able to yield less than 64 bits of entropy per step
* State tracking should be done in a compact off-heap data stucture
* Inflated partition state and per-row operation log should be done in a compact
  off-heap data structure
* Decision-making about _when_ we visit partitions and/or rows should be improved

While thislist of improvements is incomplete, t should give the reader a rough idea about the state of the project.
The original goal of the project was to drive to stability after the significant storage engine rewrite in
CASSANDRA-8099 and help remove data loss bugs from the codebase before they got out into the wild. Next steps are to
integrate it into both CI and into regular daily dev workflows.

# Goals: Reproducibility and Efficiency

_Reproducibility_ is achieved by using the PCG family of random number generators and generating schema, configuration,
and every step of the workload from the repeatable sequence of random numbers. Schema and configuration are generated
from the _seed_. Each operation is assigned its own monotonically increasing _logical timestamp_, which preserves
logical operation order between different runs.

_Efficiency_ is achieved by employing the features of the PCG random number generator (walking the sequence of random
numbers back and forth), and writing value generators in a way that preserves properties of the descriptor it was
generated from.

Given a `long` _descriptor_ can be _inflated_ into some value:

* value can be _deflated_ back to the descriptor it was generated from (in other words, generation is *invertible*)
* two inflated values will sort the same way as two descriptors they were generated from (in other words, generation is
  *order-preserving*)

These properties are also preserved for the composite values, such as clustering and partition keys.

# Components

Every Harry run starts from Configuration. You can find an example configuration under `conf/example.yml`.

*Clock* is a component responsible for mapping _logical_ timestamps to _real-time_ ones. When reproducing test failures,
and for validation purposes, a snapshot of such be taken to map a real-time timestamp from the value retrieved from the
database to map it back to the logical timestamp of the operation that wrote this value. Given a real-time timestamp,
the clock can return a logical timestamp, and vice versa.

*Runner* is a component that schedules operations that change the cluster (system under test) and model state.

*System under test*: a Cassandra node or cluster. Default implementation is in_jvm (in-JVM DTest cluster). Harry also
supports external clusters.

*Model* is responsible for tracking logical timestamps that system under test was notified about.

*Partition descriptor selector* controls how partitions are selected based on the current logical timestamp. The default
implementation is a sliding window of partition descriptors that will visit one partition after the other in the
window `slide_after_repeats` times. After that, it will retire one partition descriptor, and pick a new one.

*Clustering descriptor selector* controls how clustering keys are picked within the partition: how many rows there can
be in a partition, how many rows are visited for a logical timestamp, how many operations there will be in batch, what
kind of operations there will and how often each kind of operation is going to occur.

# Implementing Tests

All Harry components are pluggable and can be redefined. However, many of the default implementations will cover most of
the use-cases, so in this guide we’ll focus on ones that are most often used to implement different use cases:

* System Under Test: defines how Harry can communicate with Cassandra instances and issue common queries. Examples of a
  system under test can be a CCM cluster, a “real” Cassandra cluster, or an in-JVM dtest cluster.
* Visitor: defines behaviour that gets triggered at a specific logical timestamp. One of the default implementations is
  MutatingVisitor, which executes write workload against SystemUnderTest. Examples of a visitor, besides a mutating
  visitor, could be a validator that uses the model to validate results of different queries, a repair runner, or a
  fault injector.
* Model: validates results of read queries by comparing its own internal representation against the results returned by
  system under test. You can find three simplified implementations of model in this document: Visible Rows Checker,
  Quiescent Checker, and an Exhaustive Checker.
* Runner: defines how operations defined by visitors are executed. Harry includes two default implementations: a
  sequential and a concurrent runner. Sequential runner allows no overlap between different visitors or logical
  timestamps. Concurrent runner allows visitors for different timestamps to overlap.

System under test is the simplest one to implement: you only need a way to execute Cassandra queries. At the moment of
writing, all custom things, such as nodetool commands, failure injection, etc, are implemented using a SUT / visitor
combo: visitor knows about internals of the cluster it is dealing with.

Generally, visitor has to follow the rules specified by DescriptorSelector and PdSelector: (it can only visit issue
mutations against the partition that PdSelector has picked for this LTS), and DescriptorSelector (it can visit exactly
DescriptorSelector#numberOfModifications rows within this partition, operations have to have a type specified by
#operationKind, clustering and value descriptors have to be in accordance with DescriptorSelector#cd and
DescriptorSelector#vds). The reason for these limitations is because model has to be able to reproduce the exact
sequence of events that was applied to system under test.

Default implementations of partition and clustering descriptors, used in fuzz mode allow to optimise verification. For
example, it is possible to go find logical timestamps that are visiting the same partition as any given logical
timestamp. When running Harry in unit-test mode, we use a special generating visitor that keeps an entire given sequence
of events in memory rather than producing it on the fly. For reasons of efficiency, we do not use generating visitors
for generating and verifying large datasets.

# Formal Relations Between Entities

To be able to implement efficient models, we had to reduce the amount of state required for validation to a minimum and
try to operate on primitive data values most of the time. Any Harry run starts with a `seed`. Given the same
configuration, and the same seed, we're able to make runs deterministic (in other words, records in two clusters created
from the same seed are going to have different real-time timestamps, but will be otherwise identical; logical time
stamps will also be identical).

Since it's clear how to generate things like random schemas, cluster configurations, etc., let's discuss how we're
generating data, and why this type of generation makes validation efficient.

First, we're using PCG family of random number generators, which, besides having nice characteristics that any RNG
should have, have two important features:

* Streams: for single seed, we can have several independent _different_ streams of random numbers.
* Walkability: PCG generators generate a stream of numbers you can walk _back_ and _forth_. That is, for any number _n_
  that represents a _position_ of the random number in the stream of random numbers, we can get the random number at
  this position. Conversely, given a random number, we can determine what is its position in the stream. Moreover,
  knowing a random number, we can determine which number precedes it in the stream of random numbers, and, finally, we
  can determine how many numbers there are in a stream between the two random numbers.

Out of these operations, determining the _next_ random number in the sequence can be done in constant time, `O(1)`.
Advancing generator by _n_ steps can be done in `O(log(n))` steps. Since generation is cyclical, advancing the iterator
backward is equivalent to advancing it by `cardinality - 1` steps. If we're generating 64 bits of entropy, advancing
by `-1` can be done in 64 steps.

Let's introduce some definitions:

* `lts` is a *logical timestamp*, an entity (number in our case), given by the clock, on which some action occurs
* `m` is a *modification id*, a sequential number of the modification that occurs on `lts`
* `rts` is an approximate real-time as of clock for this run
* `pid` is a partition position, a number between `0` and N, for `N` unique generated partitions
* `pd` is a partition descriptor, a unique descriptor identifying the partition
* `cd` is a clustering descriptor, a unique descriptor identifying row within some partition

A small introduction that can help to understand the relation between these
entities. Hierarchically, the generation process looks as follows:

* `lts` is an entry point, from which the decision process starts
* `pd` is picked from `lts`, and determines which partition is going to be visited
* for `(pd, lts)` combination, `#mods` (the number of modification batches) and `#rows` (the number of rows per
  modification batch) is determined. `m` is an index of the modification batch, and `i` is an index of the operation in
  the modification batch.
* `cd` is picked based on `(pd, lts)`, and `n`, a sequential number of the operation among all modification batches
* operation type (whether we're going to perform a write, delete, range delete, etc), columns involved in this
  operation, and values for the modification are picked depending on the `pd`, `lts`, `m`, and `i`

Most of this formalization is implemented in `OpSelectors`, and is relied upon in`PartitionVisitor` and any
implementation of a `Model`.

Random number generation (see `OpSelectors#Rng`):

* `rng(i, stream[, e])`: returns i'th number drawn from random sequence `stream` producing values with `e` bits of
  entropy (64 bits for now).
* `rng'(s, stream[, e])`: returns `i` of the random number `s` drawn from the random sequence `stream`. This function is
  an inverse of `rng`.
* `next(rnd, stream[, e])` and `prev(rnd, stream[, e])`: the next/previous number relative to `rnd` drawn from random
  sequence `stream`.

A simple example of a partition descriptor selector is one that is based on a sliding window of a size `s`, that slides
every `n` iterations. First, we determine _where_ the window should start for a given `lts` (in other words, how many
times it has already slid). After that, we determine which `pd` we pick out of `s` available ones. After picking each
one of the `s` descriptors `n` times, we retire the oldest descriptor and pick a new one to the window. Window start and
offset are then used as input for the `rng(start + offset, stream)` to make sure descriptors are uniformly distributed.

We can build a clustering descriptor selector in a similar manner. Each partition will use its `pd` as a stream id, and
pick `cd` from a universe of possible `cds` of size `#cds`. On each `lts`, we pick a random `offset`, and start
picking `#ops` clusterings from this `offset < #cds`, and wrap around to index 0 after that. This way, each operation
maps to a unique `cd`, and `#op` can be determined from `cd` deterministically.

# Data Generation

So far, we have established how to generate partition, clustering, and value _descriptors_. Now, we need to understand
how we can generate data modification statements out of these descriptors in a way that helps us to validate data later.

Since every run has a predefined schema, and by the time we visit a partition we have a logical timestamp, we can make
the rest of the decisions: pick a number of batches we're about to perform, determine what kind of operations each one
of the batches is going to contain, which rows we're going to visit (clustering for each modification operation).

To generate a write, we need to know _which partition_ we're going to visit (in other words, partition descriptor),
_which row_ we'd like to modify (in other words, clustering descriptor), _which columns_ we're modifying (in other
words, a column mask), and, for each modified column - its value. By the time we're ready to make an actual query to the
database, we already know `pd`, `cd`, `rts`, and `vds[]`, which is all we need to "inflate" a write.

To inflate each value descriptor, we take a generator for its datatype, and turn its descriptor into the object. This
generation process has the following important properties:

* it is invertible: for every `inflate(vd) -> value`, there's `deflate(value) -> vd`
* it is order-preserving: `compare(vd1, vd2) == compare(inflate(vd1), inflate(vd2))`

Inflating `pd` and `cd` is slightly more involved than inflating `vds`, since partition and clustering keys are often
composite. This means that `inflate(pd)` returns an array of objects, rather just a single
object: `inflate(pd) -> value[]`, and `deflate(value[]) -> pd`. Just like inflating value descriptors, inflating keys
preserves order.

It is easy to see that, given two modifications: `Update(pd1, cd1, [vd1_1, vd2_1, vd3_1], lts1)`
and `Update(pd1, cd1, [vd1_2, vd3_2], lts2)`, we will end up with a resultset that contains effects of both
operations: `ResultSetRow(pd1, cd1, [vd1_2@rts2, vd2_1@rts1, vd3_2@rts2])`.

# Model

`Model` in Harry ties the rest of the components together and allows us to check whether or not data returned by the
cluster actually makes sense. The model relies on the clock, since we have to convert real-time timestamps of the
returned values back to logical timestamps, and on descriptor selectors to pick the right partition and rows.

## Visible Rows Checker

Let's try to put it all together and build a simple model. The simplest one is a visible row checker. It can check if
any row in the response returned from the database could have been produced by one of the operations. However, it won't
be able to find errors related to missing rows, and will only notice some cases of erroneously overwritten rows.

In the model, we can see a response from the database in its deflated state. In other words, instead of the actual
values returned, we see their descriptors. Every resultset row consists of `pd`, `cd`, `vds[]` (value descriptors),
and `lts[]` (logical timestamps at which these values were written).

To validate, we need to iterate through all operations for this partition, starting with the latest one the model is
aware of. This model has no internal state, and validates entire partitions:

```
void validatePartitionState(long validationLts, List<ResultSetRow> rows) {
  long pd = pdSelector.pd(validationLts, schema);

  for (ResultSetRow row : rows) {
      // iterator that gives us unique lts from the row in descending order
      LongIterator rowLtsIter = descendingIterator(row.lts);
      // iterator that gives us unique lts from the model in descending order
      LongIterator modelLtsIter = descendingIterator(pdSelector, validationLts);

      outer:
      while (rowLtsIter.hasNext()) {
        long rowLts = rowLtsIter.nextLong();

 	// this model can not check columns whose values were never written or were deleted
        if (rowLts == NO_TIMESTAMP)
          continue outer;

        if (!modelLtsIter.hasNext())
          throw new ValidationException(String.format("Model iterator is exhausted, could not verify %d lts for the row: \n%s %s",
                                                       rowLts, row));

        while (modelLtsIter.hasNext()) {
          long modelLts = modelLtsIter.nextLong();
	  // column was written by the operation that has a lower lts than the current one from the model
          if (modelLts > rowLts)
              continue;
	  // column was written by the operation that has a higher lts, which contradicts to the model, since otherwise we'd validate it by now
          if (modelLts < rowLts)
              throw new RuntimeException("Can't find a corresponding event id in the model for: " + rowLts + " " + modelLts);

          // Compare values for columns that were supposed to be written with this lts
	  for (int col = 0; col < row.lts.length; col++) {
             if (row.lts[col] != rowLts)
               continue;

             long m = descriptorSelector.modificationId(pd, row.cd, rowLts, row.vds[col], col);
             long vd = descriptorSelector.vd(pd, row.cd, rowLts, m, col);

	     // If the value model predicts doesn't match the one received from the database, throw an exception
             if (vd != row.vds[col])
                throw new RuntimeException("Returned value doesn't match the model");
	  }
	  continue outer;
        }
    }
  }
}
```

As you can see, all validation is done using deflated `ResultSetRows`, which contain enough data to say which logical
timestamp each value was written with, and which value descriptor each value has. This model can also validate data
concurrently to the ongoing data modification operations.

## Quiescent Checker

Let's consider one more checker. It'll be more powerful than the visible rows checker in one way since it can find any
inconsistency in data (incorrect timestamp, missing or additional row, rows coming in the wrong order, etc), but it'll
also have one limitation: it won't be able to run concurrently with data modification statements. This means that for
this model to be used, we should have no _in-flight_ queries, and all queries have to be in a deterministic state by the
time we're validating their results.

For this checker, we assume that we have a component that is called `Reconciler`, which can inflate partition state _up
to some_ `lts`. `Reconciler` works by simply applying each modification in the same order they were applied to the
cluster, and using standard Cassandra data reconciliation rules (last write wins / DELETE wins over INSERT in case of a
timestamp collision).

With this component, and knowing that there can be no in-fight queries, we can validate data in the following way:

```
public void validatePartitionState(Iterator<ResultSetRow> actual, Query query) {
  // find out up the highest completed logical timestamp
  long maxCompleteLts = tracker.maxComplete();

  // get the expected state from reconciler
  Iterator<Reconciler.RowState> expected = reconciler.inflatePartitionState(query.pd, maxCompleteLts, query).iterator(query.reverse);

  // compare actual and expected rows one-by-one in-order
  while (actual.hasNext() && expected.hasNext()) {
    ResultSetRow actualRowState = actual.next();
    Reconciler.RowState expectedRowState = expected.next();

    if (actualRowState.cd != expectedRowState.cd)
      throw new ValidationException("Found a row in the model that is not present in the resultset:\nExpected: %s\nActual: %s",
                                    expectedRowState, actualRowState);

    if (!Arrays.equals(actualRowState.vds, expectedRowState.vds))
      throw new ValidationException("Returned row state doesn't match the one predicted by the model:\nExpected: %s (%s)\nActual:   %s (%s).",
                                    Arrays.toString(expectedRowState.vds), expectedRowState,
                                    Arrays.toString(actualRowState.vds), actualRowState);

    if (!Arrays.equals(actualRowState.lts, expectedRowState.lts))
      throw new ValidationException("Timestamps in the row state don't match ones predicted by the model:\nExpected: %s (%s)\nActual:   %s (%s).",
                                    Arrays.toString(expectedRowState.lts), expectedRowState,
                                    Arrays.toString(actualRowState.lts), actualRowState);
  }

  if (actual.hasNext() || expected.hasNext()) {
    throw new ValidationException("Expected results to have the same number of results, but %s result iterator has more results",
                                   actual.hasNext() ? "actual" : "expected");
  }
}
```

If there's any mismatch, it'll be caught right away: if there's an extra row
(for example, there were issues in Cassandra that caused it to have duplicate
rows), or if some row or even value in the row is missing.

## Exhaustive Checker

To be able to both run validation concurrently to modifications and be able to
catch all kinds of inconsistencies, we need a more involved checker.

In this checker, we rely on inflating partition state. However, we're most
interested in `lts`, `opId`, and visibility (whether or not it is still
in-flight) of each modification operation. To be able to give a reliable result,
we need to make sure we follow these rules:

* every operation model _thinks_ should be visible, has to be visible
* every operation model _thinks_ should be invisible, has to be invisible
* every operation model doesn't know the state of (i.e., it is still
  in-flight) can be _either_ visible _invisible_
* there can be no state in the database that model is not aware of (in other words,
  we either can _explain_ how a row came to be, or we conclude that the row is
  erroneous)

A naive way to do this would be to inflate every possible partition state, where
every in-flight operation would be either visible or invisible, but this gets
costly very quickly since the number of possible combinations grows
exponentially. A better (and simpler) way to do this is to iterate all
operations and keep the state of "explained" operations:

```
public class RowValidationState {
  // every column starts in UNOBSERVED, and has to move to either REMOVED, or OBSERVED state
  private final ColumnState[] columnStates;
  // keep track of operations related to each column state
  private final Operation[] causingOperations;
}
```

Now, we move through all operations for a given row, starting from the _newest_ ones, towards
the oldest ones:

```
public void validatePartitionState(long verificationLts, PeekingIterator<ResultSetRow> actual_, Query query) {
  // get a list of operations for each cd
  NavigableMap<Long, List<Operation>> operations = inflatePartitionState(query);

  for (Map.Entry<Long, List<Operation>> entry : operations.entrySet()) {
    long cd = entry.getKey();
    List<Operation> ops = entry.getValue();

    // Found a row that is present both in the model and in the resultset
    if (actual.hasNext() && actual.peek().cd == cd) {
      validateRow(new RowValidationState(actual.next), operations);
    } else {
      validateNoRow(cd, operations);

      // Row is not present in the resultset, and we currently look at modifications with a clustering past it
      if (actual.hasNext() && cmp.compare(actual.peek().cd, cd) < 0)
         throw new ValidationException("Couldn't find a corresponding explanation for the row in the model");
    }
  }

  // if there are more rows in the resultset, and we don't have model explanation for them, we've found an issue
  if (actual.hasNext())
    throw new ValidationException("Observed unvalidated rows");
}
```

Now, we have to implement `validateRow` and `validateNoRow`. `validateNoRow` is easy: we only need to make sure that a
set of operations results in an invisible row. Since we're iterating operations in reverse order, if we encounter a
delete not followed by any writes, we can conclude that the row is invisible and exit early. If there's a write that is
not followed by a delete, and the row isn't covered by a range tombstone, we know it's an error.

`validateRow` only has to iterate operations in reverse order until it can explain the value in every column. For
example, if a value is `UNOBSERVED`, and the first thing we encounter is a `DELETE` that removes this column, we only
need to make sure that the value is actually `null`, in which case we can conclude that the value can be explained
as `REMOVED`.

Similarly, if we encounter an operation that has written the expected value, we conclude that the value is `OBSERVED`.
If there are any seeming inconsistencies between the model and resultset, we have to check whether or not the operation
in question is still in flight. If it is, its results may still not be visible, so we can't reliably say it's an error.

To summarize, in order for us to implement an exhaustive checker, we have to iterate operations for each of the rows
present in the model in reverse order until we either detect inconsistency that can't be explained by an in-flight
operation or until we explain every value in the row.

## Conclusion

As you can see, all checkers up till now are almost entirely stateless. Exhaustive and quiescent models rely
on `DataTracker` component that is aware of the in-flight and completed `lts`, but don't need any other state apart from
that, since we can always inflate a complete partition from scratch every time we validate.

While not relying on the state is a useful feature, at least _some_ state is useful to have. For example, if we're
validating just a few rows in the partition, right now we have to iterate through each and every `lts` that has visited
this partition and filter out only modifications that have visited it. However, since the model is notified of each
_started_, and, later, finished modification via `recordEvent`, we can keep track of `pd -> (cd -> lts)` map. You can
check out `VisibleRowsChecker` as an example of that.

# Usage

To use Harry, you first need to build a Cassandra in-JVM dtest jar. At the moment of writing, there's no official
repository where these jars are released, so you'll have to build it manually (_*TODO: Make this simpler!*_):

```
make package
mvn dependency:copy-dependencies
java -cp harry-core/target/harry-core-0.0.2-SNAPSHOT.jar:$(find harrry-core/target/dependency/*.jar | tr -s '\n' ':'). harry.runner.HarryRunner
```

`4.2` is a version of Cassandra which you can find in `build.xml`, and
`4.2-SNAPSHOT` is a version of dtest jar that'll be installed under
`org.apache.cassandra:cassandra-dtest-local` in your `~/.m2/repository`.

Alternatively, you can use a Docker container. For that just run:

```
git clone git@github.com:apache/cassandra.git
cd cassandra
./build-shaded-dtest-jar.sh 4.2 4.2-SNAPSHOT
cd ~/../harry/
make run
```

To see how Harry detects corruption, uncomment `scheduleCorruption(run, executor);` in `HarryRunner`, which will corrupt
data in some way.

Each Harry failure contains a complete cluster state, operation log, failure description, and a run configuration. Most
of the time, you'll be able to just load up the existing cluster state with `harry.runner.Reproduce` class, which pick
up a run configuration from `shared/run.yml`, and see the same error you've seen in your failure log.

When reproducing, make sure to point `system_under_test/root` in the yaml file to the dump, which is something
like `~/harry/shared/cluster-state/1599155256261`, and make sure to point validation to the same LTS as the failed one
with `run.validator.validatePartition(...L)`.

Because of how our corruptor works, some errors are only reproducible on a specific lts, since they're kind of writing
the data "from the future", so you should also make sure you set the following values from the corresponding values
in `failure.dump`.

```
model:
  exhaustive_checker:
    max_seen_lts: your_value
    max_complete_lts: your_value
```


# How to cut a release

## Publishing snapshot

Make sure `~/.m2/settings.xml` contains records for the following:

```
    <server>
      <id>apache.snapshots.https</id>
      <username>username</username>
      <password>password</password>
    </server>
    <server>
      <id>apache.releases.https</id>
      <username>username</username>
      <password>password</password>
    </server>
```

```
mvn versions:set -DnewVersion=0.0.2-`git rev-parse --short HEAD`-SNAPSHOT
mvn deploy
```

# Releasing

1. Prepare the release:

```
mvn release:clean
CURRENT=0.0.CURRENT
NEXT_DEV=0.0.NEXT
mvn -DreleaseVersion=$CURRENT -Dtag=$CURRENT -DdevelopmentVersion=$NEXT_DEV-SNAPSHOT release:prepare
mvn release:perform
```

2. Close staging repository: https://repository.apache.org/#stagingRepositories
3. Issue a vote on developers mailing list. Add your GPG key signature, release SHA, and staged artifacts to release
   information.

# Contributors

* [Alex Petrov](https://github.com/ifesdjeen)
* [Benedict Elliot Smith](https://github.com/belliottsmith)

Special thanks
to [Aleksey Yeschenko](https://github.com/iamaleksey), [Sam Tunnicliffe](https://github.com/beobal), [Marcus Eriksson](https://github.com/krummas),
and [Scott Andreas](https://github.com/cscotta).

# License

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "
AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
language governing permissions and limitations under the License.
