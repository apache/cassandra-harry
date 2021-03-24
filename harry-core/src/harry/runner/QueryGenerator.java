/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package harry.runner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.LongSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import harry.core.Run;
import harry.ddl.ColumnSpec;
import harry.ddl.SchemaSpec;
import harry.generators.DataGenerators;
import harry.generators.RngUtils;
import harry.generators.Surjections;
import harry.model.OpSelectors;
import harry.operations.Relation;

// TODO: there's a lot of potential to reduce an amount of garbage here.
// TODO: refactor. Currently, this class is a base for both SELECT and DELETE statements. In retrospect,
//       a better way to do the same thing would've been to just inflate bounds, be able to inflate
//       any type of query from the bounds, and leave things like "reverse" up to the last mile / implementation.
public class QueryGenerator
{
    private static final Logger logger = LoggerFactory.getLogger(QueryGenerator.class);
    private static final long GT_STREAM = 0b1;
    private static final long E_STREAM = 0b10;

    private final OpSelectors.Rng rng;
    private final OpSelectors.PdSelector pdSelector;
    private final OpSelectors.DescriptorSelector descriptorSelector;
    private final SchemaSpec schema;

    public QueryGenerator(Run run)
    {
        this(run.schemaSpec, run.pdSelector, run.descriptorSelector, run.rng);
    }

    // TODO: remove constructor
    public QueryGenerator(SchemaSpec schema,
                          OpSelectors.PdSelector pdSelector,
                          OpSelectors.DescriptorSelector descriptorSelector,
                          OpSelectors.Rng rng)
    {
        this.pdSelector = pdSelector;
        this.descriptorSelector = descriptorSelector;
        this.schema = schema;
        this.rng = rng;
    }

    public static class TypedQueryGenerator
    {
        private final OpSelectors.Rng rng;
        private final QueryGenerator queryGenerator;
        private final Surjections.Surjection<Query.QueryKind> queryKindGen;

        public TypedQueryGenerator(Run run)
        {
            this(run.rng, new QueryGenerator(run));
        }

        public TypedQueryGenerator(OpSelectors.Rng rng,
                                   QueryGenerator queryGenerator)
        {
            this(rng, Surjections.enumValues(Query.QueryKind.class), queryGenerator);
        }

        public TypedQueryGenerator(OpSelectors.Rng rng,
                                   Surjections.Surjection<Query.QueryKind> queryKindGen,
                                   QueryGenerator queryGenerator)
        {
            this.rng = rng;
            this.queryGenerator = queryGenerator;
            this.queryKindGen = queryKindGen;
        }

        // Queries are inflated from LTS, which identifies the partition, and i, a modifier for the query to
        // be able to generate different queries for the same lts.
        public Query inflate(long lts, long modifier)
        {
            long descriptor = rng.next(modifier, lts);
            Query.QueryKind queryKind = queryKindGen.inflate(descriptor);
            return queryGenerator.inflate(lts, modifier, queryKind);
        }
    }

    public Query inflate(long lts, long modifier, Query.QueryKind queryKind)
    {
        long pd = pdSelector.pd(lts, schema);
        long descriptor = rng.next(modifier, lts);
        boolean reverse = descriptor % 2 == 0;
        switch (queryKind)
        {
            case SINGLE_PARTITION:
                return new Query.SinglePartitionQuery(queryKind,
                                                      pd,
                                                      reverse,
                                                      Collections.emptyList(),
                                                      schema);
            case SINGLE_CLUSTERING:
            {
                long cd = descriptorSelector.randomCd(pd, descriptor, schema);
                return new Query.SingleClusteringQuery(queryKind,
                                                       pd,
                                                       cd,
                                                       reverse,
                                                       Relation.eqRelations(schema.ckGenerator.slice(cd), schema.clusteringKeys),
                                                       schema);
            }
            case CLUSTERING_SLICE:
            {
                List<Relation> relations = new ArrayList<>();
                long cd = descriptorSelector.randomCd(pd, descriptor, schema);
                boolean isGt = RngUtils.asBoolean(rng.next(descriptor, GT_STREAM));
                // TODO: make generation of EQ configurable; turn it off and on
                boolean isEquals = RngUtils.asBoolean(rng.next(descriptor, E_STREAM));

                long[] sliced = schema.ckGenerator.slice(cd);
                long min;
                long max;
                int nonEqFrom = RngUtils.asInt(descriptor, 0, sliced.length - 1);

                long[] minBound = new long[sliced.length];
                long[] maxBound = new long[sliced.length];

                // Algorithm that determines boundaries for a clustering slice.
                //
                // Basic principles are not hard but there are a few edge cases. I haven't figured out how to simplify
                // those, so there might be some room for improvement. In short, what we want to achieve is:
                //
                // 1. Every part that is restricted with an EQ relation goes into the bound verbatim.
                // 2. Every part that is restricted with a non-EQ relation (LT, GT, LTE, GTE) is taken into the bound
                //    if it is required to satisfy the relationship. For example, in `ck1 = 0 AND ck2 < 5`, ck2 will go
                //    to the _max_ boundary, and minimum value will go to the _min_ boundary, since we can select every
                //    descriptor that is prefixed with ck1.
                // 3. Every other part (e.g., ones that are not explicitly mentioned in the query) has to be restricted
                //    according to equality. For example, in `ck1 = 0 AND ck2 < 5`, ck3 that is present in schema but not
                //    mentioned in query, makes sure that any value between [0, min_value, min_value] and [0, 5, min_value]
                //    is matched.
                //
                // One edge case is a query on the first clustering key: `ck1 < 5`. In this case, we have to fixup the lower
                // value to the minimum possible value. We could really just do Long.MIN_VALUE, but in case we forget to
                // adjust entropy elsewhere, it'll be caught correctly here.
                for (int i = 0; i < sliced.length; i++)
                {
                    long v = sliced[i];
                    DataGenerators.KeyGenerator gen = schema.ckGenerator;
                    ColumnSpec column = schema.clusteringKeys.get(i);
                    int idx = i;
                    LongSupplier maxSupplier = () -> gen.maxValue(idx);
                    LongSupplier minSupplier = () -> gen.minValue(idx);

                    if (i < nonEqFrom)
                    {
                        relations.add(Relation.eqRelation(schema.clusteringKeys.get(i), v));
                        minBound[i] = v;
                        maxBound[i] = v;
                    }
                    else if (i == nonEqFrom)
                    {
                        relations.add(Relation.relation(relationKind(isGt, isEquals), schema.clusteringKeys.get(i), v));

                        if (column.isReversed())
                        {
                            minBound[i] = isGt ? minSupplier.getAsLong() : v;
                            maxBound[i] = isGt ? v : maxSupplier.getAsLong();
                        }
                        else
                        {
                            minBound[i] = isGt ? v : minSupplier.getAsLong();
                            maxBound[i] = isGt ? maxSupplier.getAsLong() : v;
                        }
                    }
                    else
                    {
                        if (isEquals)
                        {
                            minBound[i] = minSupplier.getAsLong();
                            maxBound[i] = maxSupplier.getAsLong();
                        }
                        else if (i > 0 && schema.clusteringKeys.get(i - 1).isReversed())
                            maxBound[i] = minBound[i] = isGt ? minSupplier.getAsLong() : maxSupplier.getAsLong();
                        else
                            maxBound[i] = minBound[i] = isGt ? maxSupplier.getAsLong() : minSupplier.getAsLong();
                    }
                }

                if (schema.clusteringKeys.get(nonEqFrom).isReversed())
                    isGt = !isGt;

                min = schema.ckGenerator.stitch(minBound);
                max = schema.ckGenerator.stitch(maxBound);

                if (nonEqFrom == 0)
                {
                    min = isGt ? min : schema.ckGenerator.minValue();
                    max = !isGt ? max : schema.ckGenerator.maxValue();
                }

                // if we're about to create an "impossible" query, just bump the modifier and re-generate
                if (min == max && !isEquals)
                    return inflate(lts, modifier + 1, queryKind);

                return new Query.ClusteringSliceQuery(Query.QueryKind.CLUSTERING_SLICE,
                                                      pd,
                                                      min,
                                                      max,
                                                      relationKind(true, isGt ? isEquals : true),
                                                      relationKind(false, !isGt ? isEquals : true),
                                                      reverse,
                                                      relations,
                                                      schema);
            }
            case CLUSTERING_RANGE:
            {
                List<Relation> relations = new ArrayList<>();
                long cd1 = descriptorSelector.randomCd(pd, descriptor, schema);
                boolean isMinEq = RngUtils.asBoolean(descriptor);
                long cd2 = descriptorSelector.randomCd(pd, rng.next(descriptor, lts), schema);

                boolean isMaxEq = RngUtils.asBoolean(rng.next(descriptor, lts));

                long[] minBound = schema.ckGenerator.slice(cd1);
                long[] maxBound = schema.ckGenerator.slice(cd2);

                int lock = RngUtils.asInt(descriptor, 0, schema.clusteringKeys.size() - 1);

                // Logic here is similar to how clustering slices are implemented, except for both lower and upper bound
                // get their values from sliced value in (1) and (2) cases:
                //
                // 1. Every part that is restricted with an EQ relation, takes its value from the min bound.
                //    TODO: this can actually be improved, since in case of hierarchical clustering generation we can
                //          pick out of the keys that are already locked. That said, we'll exercise more cases the way
                //          it is implemented right now.
                // 2. Every part that is restricted with a non-EQ relation is taken into the bound, if it is used in
                //    the query. For example in, `ck1 = 0 AND ck2 > 2 AND ck2 < 5`, ck2 values 2 and 5 will be placed,
                //    correspondingly, to the min and max bound.
                // 3. Every other part has to be restricted according to equality. Similar to clustering slice, we have
                //    to decide whether we use a min or the max value for the bound. Foe example `ck1 = 0 AND ck2 > 2 AND ck2 <= 5`,
                //    assuming we have ck3 that is present in schema but not mentioned in the query, we'll have bounds
                //    created as follows: [0, 2, max_value] and [0, 5, max_value]. Idea here is that since ck2 = 2 is excluded,
                //    we also disallow all ck3 values for [0, 2] prefix. Similarly, since ck2 = 5 is included, we allow every
                //    ck3 value with a prefix of [0, 5].
                for (int i = 0; i < schema.clusteringKeys.size(); i++)
                {
                    ColumnSpec<?> col = schema.clusteringKeys.get(i);
                    if (i < lock)
                    {
                        relations.add(Relation.eqRelation(col, minBound[i]));
                        maxBound[i] = minBound[i];
                    }
                    else if (i == lock)
                    {
                        long minLocked = Math.min(minBound[lock], maxBound[lock]);
                        long maxLocked = Math.max(minBound[lock], maxBound[lock]);

                        relations.add(Relation.relation(relationKind(true, isMinEq), col, minLocked));
                        minBound[i] = col.isReversed() ? maxLocked : minLocked;
                        relations.add(Relation.relation(relationKind(false, isMaxEq), col, maxLocked));
                        maxBound[i] = col.isReversed() ? minLocked : maxLocked;
                    }
                    else
                    {
//                        if (i > 0 && schema.clusteringKeys.get(i - 1).isReversed())
//                        {
//                            minBound[i] = isMinEq ? schema.ckGenerator.maxValue(i) : schema.ckGenerator.minValue(i);
//                            maxBound[i] = isMaxEq ? schema.ckGenerator.minValue(i) : schema.ckGenerator.maxValue(i);
//                        }
//                        else
                        {
                            minBound[i] = isMinEq ? schema.ckGenerator.minValue(i) : schema.ckGenerator.maxValue(i);
                            maxBound[i] = isMaxEq ? schema.ckGenerator.maxValue(i) : schema.ckGenerator.minValue(i);
                        }
                    }
                }

                long stitchedMin = schema.ckGenerator.stitch(minBound);
                long stitchedMax = schema.ckGenerator.stitch(maxBound);

//                if (stitchedMin > stitchedMax)
//                {
//                    long[] tmp = minBound;
//                    minBound = maxBound;
//                    maxBound = tmp;
//                    stitchedMin = schema.ckGenerator.stitch(minBound);
//                    stitchedMax = schema.ckGenerator.stitch(maxBound);
//                }
//
//                for (int i = 0; i <= lock; i++)
//                {
//                    ColumnSpec<?> col = schema.clusteringKeys.get(i);
//                    if (i < lock)
//                    {
//                        relations.add(Relation.eqRelation(col, minBound[i]));
//                    }
//                    else
//                    {
//                        relations.add(Relation.relation(relationKind(true, isMinEq), col, minBound[lock]));
//                        relations.add(Relation.relation(relationKind(false, isMaxEq), col, maxBound[lock]));
//                    }
//                }

                // if we're about to create an "impossible" query, just bump the modifier and re-generate
                // TODO: so this isn't considered "normal" that we do it this way, but I'd rather fix it with
                //       a refactoring that's mentioned below
                if (stitchedMin == stitchedMax)
                {
//                    if (modifier > 10)
//                    {
//                        logger.error(String.format("Unsuccessfully tried to generate query for %s%s;%s%s %s %d times. Schema: %s",
//                                                   isMinEq ? "[" : "(", stitchedMin,
//                                                   stitchedMax, isMaxEq ? "]" : ")",
//                                                   queryKind, modifier, schema.compile().cql()),
//                                     new RuntimeException());
//                    }
                    return inflate(lts, modifier + 1, queryKind);
                }

                // TODO: one of the ways to get rid of garbage here, and potentially even simplify the code is to
                //       simply return bounds here. After bounds are created, we slice them and generate query right
                //       from the bounds. In this case, we can even say that things like -inf/+inf are special values,
                //       and use them as placeholdrs. Also, it'll be easier to manipulate relations.
                return new Query.ClusteringRangeQuery(Query.QueryKind.CLUSTERING_RANGE,
                                                      pd,
                                                      stitchedMin,
                                                      stitchedMax,
                                                      relationKind(true, isMinEq),
                                                      relationKind(false, isMaxEq),
                                                      reverse,
                                                      relations,
                                                      schema);
            }
            default:
                throw new IllegalArgumentException("Shouldn't happen");
        }
    }

    public static Relation.RelationKind relationKind(boolean isGt, boolean isEquals)
    {
        if (isGt)
            return isEquals ? Relation.RelationKind.GTE : Relation.RelationKind.GT;
        else
            return isEquals ? Relation.RelationKind.LTE : Relation.RelationKind.LT;
    }
}
