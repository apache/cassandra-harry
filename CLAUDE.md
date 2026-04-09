# Harry

## Value Model: idx, descriptor, and value

Harry represents data at three abstraction layers. An idx is a positional index into a sorted population of generated values (e.g. "the 5th partition key"). A descriptor is a long seed that deterministically produces a typed value. A value is the concrete data (String, UUID, int, etc.) used in actual database operations. The conversion chain is: idx -> descriptor -> value, using descriptorAt(idx) and inflate(descriptor). The reverse path is deflate(value) back to a descriptor, and idxFor(descriptor) back to an index. Operations are stored using descriptors, not values, which keeps the history compact and reproducible.

## Specs

When modifying code related to the SQL IR / rendering layer (`src/main/java/org/apache/cassandra/harry/dml/sql/`),
always keep the design spec up to date:

  `docs/superpowers/specs/005-direct-sql-testing-interface-design.md`

This includes changes to: `TableSpec`, `Expression`, `Predicate`, `Assignment`, `UpdateStatement`,
`SqlRenderer`, `ValuePool`, `ValueIndex`, `ComparisonOp`, `DslPrelude`, and any new IR node types.
If you add, remove, or rename a class, method, or concept, update the spec to match.

## SQL Statement Integrity

Never patch, rewrite, or post-process rendered SQL statements (e.g. appending
ON CONFLICT clauses, rewriting WHERE predicates). If the rendered output does
not work as-is, that is a signal that the IR or the renderer needs to change.
Notify the user instead of working around it.

## Bijection and IndexedBijection

Bijection<T> (in gen/Bijections.java) defines a reversible mapping between long descriptors and typed values via inflate(descriptor) and deflate(value). Implementations exist for all supported column types (int, long, UUID, String, timestamp, etc.). The key contract is that descriptors compare in the same order as the values they produce. IndexedBijection<T> (defined in dsl/HistoryBuilder.java, implemented by InvertibleGenerator and DiskBackedInvertibleGenerator) extends Bijection<T> with descriptorAt(idx) and idxFor(descriptor), decoupling descriptor order from value order. This allows descriptors to serve purely as generation seeds while a separate sorted index tracks the population. ValuePool and ValueIndex in dml/sql/ use these abstractions to bridge the SQL rendering layer with Harry's descriptor-based value system.
