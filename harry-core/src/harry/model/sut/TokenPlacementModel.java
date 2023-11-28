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

package harry.model.sut;

import java.net.InetAddress;
import java.util.*;
import java.util.function.Function;

import static harry.model.sut.TokenPlacementModel.NtsReplicationFactor.assertStrictlySorted;

public class TokenPlacementModel
{
    public abstract static class ReplicationFactor
    {
        private final int nodesTotal;

        public ReplicationFactor(int total)
        {
            this.nodesTotal = total;
        }

        public int total()
        {
            return nodesTotal;
        }

        public abstract Map<String, Integer> asMap();
        public abstract int dcs();

        public final ReplicatedRanges replicate(List<Node> nodes)
        {
            assertStrictlySorted(nodes);
            return replicate(toRanges(nodes), nodes);
        }

        public abstract ReplicatedRanges replicate(Range[] ranges, List<Node> nodes);
    }

    public static class ReplicatedRanges
    {
        private final Range[] ranges;
        private final NavigableMap<Range, List<Node>> placementsForRange;

        public ReplicatedRanges(Range[] ranges, NavigableMap<Range, List<Node>> placementsForRange)
        {
            this.ranges = ranges;
            this.placementsForRange = placementsForRange;
        }

        public List<Node> replicasFor(long token)
        {
            int idx = indexedBinarySearch(ranges, range -> {
                // exclusive start, so token at the start belongs to a lower range
                if (token <= range.start)
                    return 1;
                // ie token > start && token <= end
                if (token <= range.end ||range.end == Long.MIN_VALUE)
                    return 0;

                return -1;
            });
            assert idx >= 0 : String.format("Somehow ranges %s do not contain token %d", Arrays.toString(ranges), token);
            return placementsForRange.get(ranges[idx]);
        }

        public NavigableMap<Range, List<Node>> asMap()
        {
            return placementsForRange;
        }

        private static <T> int indexedBinarySearch(T[] arr, CompareTo<T> comparator)
        {
            int low = 0;
            int high = arr.length - 1;

            while (low <= high)
            {
                int mid = (low + high) >>> 1;
                T midEl = arr[mid];
                int cmp = comparator.compareTo(midEl);

                if (cmp < 0)
                    low = mid + 1;
                else if (cmp > 0)
                    high = mid - 1;
                else
                    return mid;
            }
            return -(low + 1); // key not found
        }
    }

    public interface CompareTo<V>
    {
        int compareTo(V v);
    }

    public static void addIfUnique(List<Node> nodes, Set<String> names, Node node)
    {
        if (names.contains(node.id))
            return;
        nodes.add(node);
        names.add(node.id);
    }

    /**
     * Finds a primary replica
     */
    public static int primaryReplica(List<Node> nodes, Range range)
    {
        for (int i = 0; i < nodes.size(); i++)
        {
            if (range.end != Long.MIN_VALUE && nodes.get(i).token >= range.end)
                return i;
        }
        return -1;
    }

    /**
     * Generates token ranges from the list of nodes
     */
    public static Range[] toRanges(List<Node> nodes)
    {
        List<Long> tokens = new ArrayList<>();
        for (Node node : nodes)
            tokens.add(node.token);
        tokens.add(Long.MIN_VALUE);
        tokens.sort(Long::compareTo);

        Range[] ranges = new Range[nodes.size() + 1];
        long prev = tokens.get(0);
        int cnt = 0;
        for (int i = 1; i < tokens.size(); i++)
        {
            long current = tokens.get(i);
            ranges[cnt++] = new Range(prev, current);
            prev = current;
        }
        ranges[ranges.length - 1] = new Range(prev, Long.MIN_VALUE);
        return ranges;
    }

    public static List<Node> peerStateToNodes(Object[][] resultset)
    {
        List<Node> nodes = new ArrayList<>();
        for (Object[] row : resultset)
        {
            InetAddress address = (InetAddress) row[0];
            Set<String> tokens = (Set<String>) row[1];
            String dc = (String) row[2];
            String rack = (String) row[3];
            for (String token : tokens)
                nodes.add(new Node(Long.parseLong(token), address.toString(), new Location(dc, rack)));
        }
        return nodes;
    }

    public static class NtsReplicationFactor extends ReplicationFactor
    {
        private final Map<String, Integer> map;

        public NtsReplicationFactor(int... nodesPerDc)
        {
            this("datacenter", nodesPerDc);
        }
        public NtsReplicationFactor(String prefix, int... nodesPerDc)
        {
            super(total(nodesPerDc));
            this.map = new HashMap<>();
            for (int i = 0; i < nodesPerDc.length; i++)
                map.put(prefix + (i + 1), nodesPerDc[i]);
        }

        public NtsReplicationFactor(Map<String, Integer> m)
        {
            super(m.values().stream().reduce(0, Integer::sum));
            this.map = m;
        }

        private static int total(int... num)
        {
            int tmp = 0;
            for (int i : num)
                tmp += i;
            return tmp;
        }

        public Map<String, Integer> asMap()
        {
            return new TreeMap<>(map);
        }

        public int dcs()
        {
            return map.size();
        }

        public ReplicatedRanges replicate(Range[] ranges, List<Node> nodes)
        {
            return replicate(ranges, nodes, map);
        }

        public static <T extends Comparable<T>> void assertStrictlySorted(Collection<T> coll)
        {
            if (coll.size() <= 1) return;

            Iterator<T> iter = coll.iterator();
            T prev = iter.next();
            while (iter.hasNext())
            {
                T next = iter.next();
                assert next.compareTo(prev) > 0 : String.format("Collection does not seem to be sorted. %s and %s are in wrong order", prev, next);
                prev = next;
            }
        }
        public static ReplicatedRanges replicate(Range[] ranges, List<Node> nodes, Map<String, Integer> rfs)
        {
            Map<String, DatacenterNodes> template = new HashMap<>();

            Map<String, List<Node>> nodesByDC = nodesByDC(nodes);
            Map<String, Set<String>> racksByDC = racksByDC(nodes);

            for (Map.Entry<String, Integer> entry : rfs.entrySet())
            {
                String dc = entry.getKey();
                int rf = entry.getValue();
                List<Node> nodesInThisDC = nodesByDC.get(dc);
                Set<String> racksInThisDC = racksByDC.get(dc);
                int nodeCount = nodesInThisDC == null ? 0 : nodesInThisDC.size();
                int rackCount = racksInThisDC == null ? 0 : racksInThisDC.size();
                if (rf <= 0 || nodeCount == 0)
                    continue;

                template.put(dc, new DatacenterNodes(rf, rackCount, nodeCount));
            }

            NavigableMap<Range, Map<String, List<Node>>> replication = new TreeMap<>();

            for (Range range : ranges)
            {
                final int idx = primaryReplica(nodes, range);
                int cnt = 0;
                if (idx >= 0)
                {
                    int dcsToFill = template.size();

                    Map<String, DatacenterNodes> nodesInDCs = new HashMap<>();
                    for (Map.Entry<String, DatacenterNodes> e : template.entrySet())
                        nodesInDCs.put(e.getKey(), e.getValue().copy());

                    while (dcsToFill > 0 && cnt < nodes.size())
                    {
                        Node node = nodes.get((idx + cnt) % nodes.size());
                        DatacenterNodes dcNodes = nodesInDCs.get(node.location.dc);
                        if (dcNodes != null && dcNodes.addAndCheckIfDone(node, new Location(node.location.dc, node.location.rack)))
                            dcsToFill--;

                        cnt++;
                    }

                    replication.put(range, mapValues(nodesInDCs, v -> v.nodes));
                }
                else
                {
                    // if the range end is larger than the highest assigned token, then treat it
                    // as part of the wraparound and replicate it to the same nodes as the first
                    // range. This is most likely caused by decommission removing the node with
                    // the largest token.
                    replication.put(range, replication.get(ranges[0]));
                }
            }

            return combine(replication);
        }

        private static ReplicatedRanges combine(NavigableMap<Range, Map<String, List<Node>>> orig)
        {

            Range[] ranges = new Range[orig.size()];
            int idx = 0;
            NavigableMap<Range, List<Node>> flattened = new TreeMap<>();
            for (Map.Entry<Range, Map<String, List<Node>>> e : orig.entrySet())
            {
                List<Node> placementsForRange = new ArrayList<>();
                for (List<Node> v : e.getValue().values())
                    placementsForRange.addAll(v);
                ranges[idx++] = e.getKey();
                flattened.put(e.getKey(), placementsForRange);
            }
            return new ReplicatedRanges(ranges, flattened);
        }

        public String toString()
        {
            return "NtsReplicationFactor{" +
                   ", map=" + map +
                   '}';
        }
    }

    private static final class DatacenterNodes
    {
        private final List<Node> nodes = new ArrayList<>();
        private final Set<Location> racks = new HashSet<>();

        /** Number of replicas left to fill from this DC. */
        int rfLeft;
        int acceptableRackRepeats;

        public DatacenterNodes copy()
        {
            return new DatacenterNodes(rfLeft, acceptableRackRepeats);
        }

        DatacenterNodes(int rf,
                        int rackCount,
                        int nodeCount)
        {
            this.rfLeft = Math.min(rf, nodeCount);
            acceptableRackRepeats = rf - rackCount;
        }

        // for copying
        DatacenterNodes(int rfLeft, int acceptableRackRepeats)
        {
            this.rfLeft = rfLeft;
            this.acceptableRackRepeats = acceptableRackRepeats;
        }

        boolean addAndCheckIfDone(Node node, Location location)
        {
            if (done())
                return false;

            if (nodes.contains(node))
                // Cannot repeat a node.
                return false;

            if (racks.add(location))
            {
                // New rack.
                --rfLeft;
                nodes.add(node);
                return done();
            }
            if (acceptableRackRepeats <= 0)
                // There must be rfLeft distinct racks left, do not add any more rack repeats.
                return false;

            nodes.add(node);

            // Added a node that is from an already met rack to match RF when there aren't enough racks.
            --acceptableRackRepeats;
            --rfLeft;
            return done();
        }

        boolean done()
        {
            assert rfLeft >= 0;
            return rfLeft == 0;
        }
    }

    public static class Location
    {
        public final String dc;
        public final String rack;

        public Location(String dc, String rack)
        {
            this.dc = dc;
            this.rack = rack;
        }
    }

    private static <K extends Comparable<K>, T1, T2> Map<K, T2> mapValues(Map<K, T1> allDCs, Function<T1, T2> map)
    {
        NavigableMap<K, T2> res = new TreeMap<>();
        for (Map.Entry<K, T1> e : allDCs.entrySet())
        {
            res.put(e.getKey(), map.apply(e.getValue()));
        }
        return res;
    }

    public static Map<String, List<Node>> nodesByDC(List<Node> nodes)
    {
        Map<String, List<Node>> nodesByDC = new HashMap<>();
        for (Node node : nodes)
            nodesByDC.computeIfAbsent(node.location.dc, (k) -> new ArrayList<>()).add(node);

        return nodesByDC;
    }

    public static Map<String, Integer> dcLayout(List<Node> nodes)
    {
        Map<String, List<Node>> nodesByDC = nodesByDC(nodes);
        Map<String, Integer> layout = new HashMap<>();
        for (Map.Entry<String, List<Node>> e : nodesByDC.entrySet())
            layout.put(e.getKey(), e.getValue().size());

        return layout;
    }

    public static Map<String, Set<String>> racksByDC(List<Node> nodes)
    {
        Map<String, Set<String>> racksByDC = new HashMap<>();
        for (Node node : nodes)
            racksByDC.computeIfAbsent(node.location.dc, (k) -> new HashSet<>()).add(node.location.rack);

        return racksByDC;
    }


    public static class SimpleReplicationFactor extends ReplicationFactor
    {
        public SimpleReplicationFactor(int total)
        {
            super(total);
        }

        public Map<String, Integer> asMap()
        {
            return Collections.singletonMap("datacenter1", total());
        }

        public int dcs()
        {
            return 1;
        }

        public ReplicatedRanges replicate(Range[] ranges, List<Node> nodes)
        {
            return replicate(ranges, nodes, total());
        }

        public static ReplicatedRanges replicate(Range[] ranges, List<Node> nodes, int rf)
        {
            NavigableMap<Range, List<Node>> replication = new TreeMap<>();
            for (Range range : ranges)
            {
                Set<String> names = new HashSet<>();
                List<Node> replicas = new ArrayList<>();
                int idx = primaryReplica(nodes, range);
                if (idx >= 0)
                {
                    for (int i = idx; i < nodes.size() && replicas.size() < rf; i++)
                        addIfUnique(replicas, names, nodes.get(i));

                    for (int i = 0; replicas.size() < rf && i < idx; i++)
                        addIfUnique(replicas, names, nodes.get(i));
                    if (range.start == Long.MIN_VALUE)
                        replication.put(ranges[ranges.length - 1], replicas);
                    replication.put(range, replicas);
                }
                else
                {
                    // if the range end is larger than the highest assigned token, then treat it
                    // as part of the wraparound and replicate it to the same nodes as the first
                    // range. This is most likely caused by a decommission removing the node with
                    // the largest token.
                    replication.put(range, replication.get(ranges[0]));
                }
            }

            return new ReplicatedRanges(ranges, Collections.unmodifiableNavigableMap(replication));
        }
    }

    public static class Range implements Comparable<Range>
    {
        public final long start;
        public final long end;

        public Range(long start, long end)
        {
            this(start, end, false);
        }

        public Range(long start, long end, boolean skipAssert)
        {
            assert skipAssert || (end > start || end == Long.MIN_VALUE);
            this.start = start;
            this.end = end;
        }

        public boolean contains(long token)
        {
            return token > start && (token <= end || end == Long.MIN_VALUE);
        }

        public int compareTo(Range o)
        {
            int res = Long.compare(start, o.start);
            if (res == 0)
                return Long.compare(end, o.end);
            return res;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Range range = (Range) o;
            return start == range.start && end == range.end;
        }

        public int hashCode()
        {
            return Objects.hash(start, end);
        }

        public String toString()
        {
            return "(" +
                   "" + (start == Long.MIN_VALUE ? "MIN" : start) +
                   ", " + (end == Long.MAX_VALUE ? "MAX" : end) +
                   ']';
        }
    }

    public static class Node implements Comparable<Node>
    {
        public final long token;
        public final String id;
        public final Location location;

        public Node(long token, String id, Location location)
        {
            this.token = token;
            this.id = id;
            this.location = location;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Node node = (Node) o;
            return Objects.equals(id, node.id);
        }

        public int hashCode()
        {
            return Objects.hash(id);
        }

        public int compareTo(Node o)
        {
            return Long.compare(token, o.token);
        }

        public String toString()
        {
            return "" + id + "@" + token;
        }
    }
}
