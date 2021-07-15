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

package harry.operations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import harry.ddl.SchemaSpec;
import harry.model.SelectHelper;
import harry.util.Ranges;

import static harry.operations.Relation.FORWARD_COMPARATOR;

public abstract class Query
{
    private static final Logger logger = LoggerFactory.getLogger(Query.class);
    // TODO: There are queries without PD
    public final long pd;
    public final boolean reverse;
    public final List<Relation> relations;
    public final Map<String, List<Relation>> relationsMap;
    public final SchemaSpec schemaSpec;
    public final QueryKind queryKind;

    public Query(QueryKind kind, long pd, boolean reverse, List<Relation> relations, SchemaSpec schemaSpec)
    {
        this.queryKind = kind;
        this.pd = pd;
        this.reverse = reverse;
        this.relations = relations;
        this.relationsMap = new HashMap<>();
        for (Relation relation : relations)
            this.relationsMap.computeIfAbsent(relation.column(), column -> new ArrayList<>()).add(relation);
        this.schemaSpec = schemaSpec;
    }

    // TODO: pd, values, filtering?
    public boolean match(long cd)
    {
        return simpleMatch(this, cd);
    }

    public static boolean simpleMatch(Query query,
                                      long cd)
    {
        long[] sliced = query.schemaSpec.ckGenerator.slice(cd);
        for (int i = 0; i < query.schemaSpec.clusteringKeys.size(); i++)
        {
            List<Relation> relations = query.relationsMap.get(query.schemaSpec.clusteringKeys.get(i).name);
            if (relations == null)
                continue;

            for (Relation r : relations)
            {
                if (!r.match(sliced[i]))
                    return false;
            }
        }

        return true;
    }

    public static class SinglePartitionQuery extends Query
    {
        public SinglePartitionQuery(QueryKind kind, long pd, boolean reverse, List<Relation> allRelations, SchemaSpec schemaSpec)
        {
            super(kind, pd, reverse, allRelations, schemaSpec);
        }

        public boolean match(long cd)
        {
            return true;
        }

        public Ranges.Range toRange(long ts)
        {
            return new Ranges.Range(Long.MIN_VALUE, Long.MAX_VALUE, true, true, ts);
        }

        public String toString()
        {
            return "SinglePartitionQuery{" +
                   "pd=" + pd +
                   ", reverse=" + reverse +
                   ", relations=" + relations +
                   ", relationsMap=" + relationsMap +
                   ", schemaSpec=" + schemaSpec +
                   '}';
        }
    }

    public static class SingleClusteringQuery extends Query
    {
        private final long cd;

        public SingleClusteringQuery(QueryKind kind, long pd, long cd, boolean reverse, List<Relation> allRelations, SchemaSpec schemaSpec)
        {
            super(kind, pd, reverse, allRelations, schemaSpec);
            this.cd = cd;
        }

        public Ranges.Range toRange(long ts)
        {
            return new Ranges.Range(cd, cd, true, true, ts);
        }

        @Override
        public boolean match(long cd)
        {
            return cd == this.cd;
        }
    }

    public static class ClusteringSliceQuery extends ClusteringRangeQuery
    {
        public ClusteringSliceQuery(QueryKind kind,
                                    long pd,
                                    long cdMin,
                                    long cdMax,
                                    Relation.RelationKind minRelation,
                                    Relation.RelationKind maxRelation,
                                    boolean reverse,
                                    List<Relation> allRelations,
                                    SchemaSpec schemaSpec)
        {
            super(kind, pd, cdMin, cdMax, minRelation, maxRelation, reverse, allRelations, schemaSpec);
        }

        public String toString()
        {
            return "ClusteringSliceQuery{" +
                   "\n" + toSelectStatement() +
                   "\npd=" + pd +
                   "\nreverse=" + reverse +
                   "\nrelations=" + relations +
                   "\nrelationsMap=" + relationsMap +
                   "\nschemaSpec=" + schemaSpec +
                   "\nqueryKind=" + queryKind +
                   "\ncdMin=" + cdMin +
                   "(" + Arrays.toString(schemaSpec.ckGenerator.slice(cdMin)) + ")" +
                   "\ncdMax=" + cdMax +
                   "(" + Arrays.toString(schemaSpec.ckGenerator.slice(cdMax)) + ")" +
                   "\nminRelation=" + minRelation +
                   "\nmaxRelation=" + maxRelation +
                   '}' + "\n" + toSelectStatement().cql();
        }
    }

    public static class ClusteringRangeQuery extends Query
    {
        protected final long cdMin;
        protected final long cdMax;
        protected final Relation.RelationKind minRelation;
        protected final Relation.RelationKind maxRelation;

        public ClusteringRangeQuery(QueryKind kind,
                                    long pd,
                                    long cdMin,
                                    long cdMax,
                                    Relation.RelationKind minRelation,
                                    Relation.RelationKind maxRelation,
                                    boolean reverse,
                                    List<Relation> allRelations,
                                    SchemaSpec schemaSpec)
        {
            super(kind, pd, reverse, allRelations, schemaSpec);
            assert cdMin != cdMax || (minRelation.isInclusive() && maxRelation.isInclusive());
            this.cdMin = cdMin;
            this.cdMax = cdMax;
            this.minRelation = minRelation;
            this.maxRelation = maxRelation;
        }

        public Ranges.Range toRange(long ts)
        {
            return new Ranges.Range(cdMin, cdMax, minRelation.isInclusive(), maxRelation.isInclusive(), ts);
        }

        public boolean match(long cd)
        {
            // TODO: looks like we don't really need comparator here.
            Relation.LongComparator cmp = FORWARD_COMPARATOR;
            boolean res = minRelation.match(cmp, cd, cdMin) && maxRelation.match(cmp, cd, cdMax);
            if (!logger.isDebugEnabled())
                return res;
            boolean superRes = super.match(cd);
            if (res != superRes)
            {
                logger.debug("Query did not pass a sanity check.\n{}\n Super call returned: {}, while current call: {}\n" +
                             "cd         = {} {} ({})\n" +
                             "this.cdMin = {} {} ({})\n" +
                             "this.cdMax = {} {} ({})\n" +
                             "minRelation.match(cmp, cd, this.cdMin) = {}\n" +
                             "maxRelation.match(cmp, cd, this.cdMax) = {}\n",
                             this,
                             superRes, res,
                             cd, Long.toHexString(cd), Arrays.toString(schemaSpec.ckGenerator.slice(cd)),
                             cdMin, Long.toHexString(cdMin), Arrays.toString(schemaSpec.ckGenerator.slice(cdMin)),
                             cdMax, Long.toHexString(cdMax), Arrays.toString(schemaSpec.ckGenerator.slice(cdMax)),
                             minRelation.match(cmp, cd, cdMin),
                             maxRelation.match(cmp, cd, cdMax));
            }
            return res;
        }

        public String toString()
        {
            return "ClusteringRangeQuery{" +
                   "pd=" + pd +
                   ", reverse=" + reverse +
                   ", relations=" + relations +
                   ", relationsMap=" + relationsMap +
                   ", schemaSpec=" + schemaSpec +
                   ", cdMin=" + cdMin +
                   ", cdMax=" + cdMax +
                   ", minRelation=" + minRelation +
                   ", maxRelation=" + maxRelation +
                   '}';
        }
    }


    public CompiledStatement toSelectStatement()
    {
        return toSelectStatement(true);
    }

    public CompiledStatement toSelectStatement(boolean includeWriteTime)
    {
        return SelectHelper.select(schemaSpec, pd, relations, reverse, includeWriteTime);
    }

    public CompiledStatement toDeleteStatement(long rts)
    {
        return DeleteHelper.delete(schemaSpec, pd, relations, null, null, rts);
    }

    public abstract Ranges.Range toRange(long ts);

    public static Query selectPartition(SchemaSpec schemaSpec, long pd, boolean reverse)
    {
        return new SinglePartitionQuery(QueryKind.SINGLE_PARTITION, pd, reverse, Collections.emptyList(), schemaSpec);
    }

    public enum QueryKind
    {
        SINGLE_PARTITION,
        SINGLE_CLUSTERING,
        // Not sure if that's the best way to name these, but the difference is as follows:
        //
        // For a single clustering, clustering slice is essentially [x; ∞) or (-∞; x].
        //
        // However, in case of multiple clusterings, such as (ck1, ck2, ck3), can result into a range query with locked prefix:
        //    ck1 = x and ck2 = y and ck3 > z
        //
        // This would translate into bounds such as:
        //    ( (x, y, z); (x, y, max_ck3) )
        //
        // Logic here is that when we're "locking" x and y, and allow third slice to be in the range [z; ∞).
        // Implementation is a bit more involved than that, since we have to make sure it works for all clustering sizes
        // and for reversed type.
        CLUSTERING_SLICE,
        // For a single clustering, clustering slice is essentially [x; y].
        //
        // For multiple-clusterings case, for example (ck1, ck2, ck3), we select how many clusterings we "lock" with EQ
        // relation, and do a range slice for the rest. For example:
        //    ck1 = x and ck2 = y and ck3 > z1 and ck3 < z2
        //
        // Such queries only make sense if written partition actually has clusterings that have intersecting parts.
        CLUSTERING_RANGE
    }
}
