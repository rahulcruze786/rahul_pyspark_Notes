Let's map each failure mode to actual PySpark functions/operators. The key distinction is whether Spark can spill temporary working data successfully, whether a required memory structure becomes too large, or whether the spill itself exhausts local disk.

Quick comparison
Case	Example functions	Main resource pressure	Typical outcome✅ Normal spill	orderBy(), sort(), repartition(), shuffle joins	Execution memory	Spill → continue
🔴 Executor OOM	collect_list(), collect_set(), oversized broadcast(), memory-heavy UDF	Required live memory/state	Executor crashes
💾 Disk-space failure	Very large orderBy(), repartition(), shuffle join with repeated spill	Local disk	Spill/write failure
1. ✅ Normal spill: orderBy()

This is probably the easiest example.

df.orderBy("transaction_amount").write.parquet(...)


Imagine one task needs to sort:

Partition = 8 GB
Available execution memory = 2 GB


Spark does not necessarily need all 8 GB in RAM simultaneously.

Conceptually:

Read records
     ↓
Sort in memory
     ↓
Memory pressure
     ↓
Spill sorted chunk to disk
     ↓
Read more records
     ↓
Sort
     ↓
Spill another chunk
     ↓
...
     ↓
Merge sorted chunks
     ↓
Output


So:

8 GB partition
+
2 GB execution memory

does NOT automatically mean OOM


The algorithm can process the partition using memory plus local disk.

Other common spill-producing operations
df.orderBy("amount")

df.sort("amount")

df.repartition(200, "customer_id")


and shuffled joins such as:

df1.join(df2, "customer_id")


can all generate substantial shuffle/sort execution state.

2. ✅ groupBy().count() can spill, but often won't for your skew example

Using your example:

df.groupBy("expense_type").count()


with:

10 GB dataset

Travel = 9 GB
Food   = 0.5 GB
Hotel  = 0.5 GB


Spark can partially aggregate:

P1 → Travel=2,000,000
P2 → Travel=1,800,000
P3 → Travel=2,300,000
...


Then shuffle:

Travel=2M ────┐
Travel=1.8M ──┤
Travel=2.3M ──┼──→ final count
...           │
              ↓
        Travel = 90M


So even though Travel represents 9 GB of input, its aggregation state is basically a counter.

There's usually no 9 GB object that needs to be held in memory.

That's why:

groupBy("expense_type").count() is usually not a good example of an OOM caused simply by one heavily skewed key.

3. 🔴 Better OOM example: collect_list()

Now change your code to:

from pyspark.sql import functions as F

result = (
    df.groupBy("expense_type")
      .agg(F.collect_list("description").alias("descriptions"))
)


Suppose 90% of rows are:

expense_type = Travel


Now Spark needs to produce something logically like:

Travel →

[
 "Airport taxi",
 "Hotel",
 "Flight",
 "Train",
 "Uber",
 "Meal",
 ...
 millions of elements
]


The aggregation state can become extremely large.

Conceptually:

Travel records
      ↓
collect_list()
      ↓
aggregation state

Travel → [....millions of values....]

      ↓
State keeps growing
      ↓
Memory pressure
      ↓
Spill whatever can be spilled
      ↓
Required live/result state still too large
      ↓
Cannot allocate required JVM object
      ↓
💥 Executor OOM


This is a much better aggregation-based OOM example than:

groupBy().count()

4. 🔴 Another potential OOM: collect_set()

For example:

df.groupBy("customer_id").agg(
    F.collect_set("transaction_id")
)


Suppose one customer has millions of unique transaction IDs.

Spark has to maintain the unique values:

C001 →

{
 TX000001,
 TX000002,
 TX000003,
 ...
 TX8000000
}


Unlike count():

C001 → 8,000,000


the state grows with distinct values.

So collect_set() can also create substantial aggregation memory pressure.

5. 🔴 Another OOM example: oversized broadcast()

This is a particularly good interview example.

Suppose:

transactions_df = 500 GB
lookup_df       = 4 GB


Someone forces:

from pyspark.sql.functions import broadcast

result = transactions_df.join(
    broadcast(lookup_df),
    "expense_type"
)


The executors require the broadcast relation to be materialized into an appropriate in-memory structure.

Conceptually:

                 4 GB lookup
                     ↓
                  Broadcast
                     ↓

        ┌────────────┼────────────┐
        ↓            ↓            ↓
    Executor 1   Executor 2   Executor 3
        │            │            │
        ↓            ↓            ↓
    Hash table   Hash table   Hash table


The actual in-memory representation can also differ materially from the compressed/source-file size.

If the broadcast structure is too large relative to available executor resources:

Huge broadcast structure
        ↓
Required memory allocation
        ↓
Memory unavailable
        ↓
💥 OOM


This is why manually doing:

broadcast(huge_df)


without understanding its actual size is dangerous.

6. 🔴 PySpark UDF can also create OOM

For example, poorly designed Python code:

@F.udf(...)
def process(records):
    huge_list = []
    ...


Or mapPartitions() code that creates huge Python data structures:

df.rdd.mapPartitions(my_function)


If my_function does something equivalent to:

def my_function(rows):
    everything = list(rows)
    ...


you're saying:

"Give me the entire partition as a Python list."

Now:

Large partition
       ↓
Python worker
       ↓
list(rows)
       ↓
Massive Python objects
       ↓
Python process memory grows
       ↓
💥 OOM / worker killed


This is different from Spark's normal spillable execution structures.

7. 💾 Disk-space failure example: orderBy()

Now imagine a very large sort:

df.orderBy("transaction_date")


Perhaps you're sorting terabytes with relatively little execution memory.

Spark repeatedly performs:

Memory fills
    ↓
spill 20 GB

Memory fills
    ↓
spill another 20 GB

Memory fills
    ↓
spill another 20 GB

...


Local spill directory:

/tmp / local SSD

████████████████████ 80%
████████████████████ 90%
████████████████████ 99%
████████████████████ 100%


Eventually Spark needs:

Another 10 GB spill


but local disk has:

500 MB available


Now the problem is disk, not JVM heap.

The job can fail writing temporary/shuffle/spill data.

Don't describe this as:

❌ "Executor OOM because disk became full."

Describe it as:

✅ "Spark successfully attempted to spill because of execution-memory pressure, but local storage was exhausted, causing a disk/spill failure."

8. 💾 repartition() can also contribute heavily to disk spill

Example:

df.repartition(10, "customer_id")


Imagine:

Dataset = 2 TB
Partitions requested = only 10


That's potentially an enormous amount of shuffled data per output partition.

Conceptually:

                  2 TB
                    ↓
            repartition(10)
                    ↓
                  SHUFFLE
                    ↓

       P1     P2     P3 ...    P10
     ~200GB ~200GB ~200GB     ~200GB


There may be massive shuffle/spill activity.

If workers don't have sufficient local storage:

Huge shuffle
    ↓
execution memory pressure
    ↓
spill
    ↓
more spill
    ↓
local disk exhausted
    ↓
💥 disk/write failure


The better fix may be increasing the number of partitions rather than increasing executor heap:

df.repartition(1000, "customer_id")


The correct value is workload-dependent, but the principle is:

Too few partitions
       ↓
Huge task workloads
       ↓
More memory/disk pressure

9. JOIN can produce all three situations

This is important because a join isn't automatically a "spill", "OOM", or "disk failure" operation.

Suppose:

transactions.join(
    tax_rules,
    "expense_type"
)


A shuffle join might be perfectly healthy:

Shuffle
   ↓
execution-memory pressure
   ↓
spill
   ↓
continue

✅ Normal


But if a task's required working state becomes too large:

Huge/skewed workload
      ↓
memory pressure
      ↓
Cannot obtain enough required memory
      ↓
💥 OOM


And if the shuffle/spill volume exhausts worker local disks:

Huge shuffle
    ↓
repeated spills
    ↓
local disk full
    ↓
💥 Disk failure


So the same logical function can end differently depending on data volume, distribution, partition count, join strategy and resources.

10. Best way to memorize this
                 MEMORY PRESSURE
                       │
                       ▼
           Can operator spill data?
                       │
               ┌───────┴───────┐
               │               │
              Yes              Not enough /
               │               required live
               ↓               state too large
          Spill to disk             │
               │                    ↓
               │                  OOM 💥
               ↓
       Is enough disk available?
               │
          ┌────┴────┐
          │         │
         Yes        No
          │         │
          ↓         ↓
      Continue   Disk failure 💾

Functions/examples worth remembering for interviews
✅ Normal spill
df.orderBy("amount")


or large shuffle operations:

df.repartition(500, "customer_id")


Reason: Spark can spill intermediate sort/shuffle data and continue.

🔴 Executor OOM

Good examples:

df.groupBy("expense_type").agg(
    F.collect_list("description")
)


or an oversized forced broadcast:

large_df.join(
    F.broadcast(large_lookup_df),
    "id"
)


Reason: the required aggregation/broadcast/working structure can become too large for available memory.

💾 Disk-space failure

Example:

huge_df.orderBy("transaction_date")


or a badly undersized shuffle:

huge_df.repartition(10, "customer_id")


Reason: enormous shuffle/sort → repeated spill → local disk fills → spill/write failure.

And the example NOT to use for OOM
df.groupBy("expense_type").count()


with:

Travel = 90% of rows


Don't say:

"90% Travel means one 9 GB partition must fit in executor memory."

For count(), Spark can perform partial aggregation, dramatically reducing the required aggregation state before the final aggregation.

That's probably the single most important correction from the tutorials you've been reviewing.
