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

public class TokenPlacementModel
{
    /**
     * Replicate ranges to rf nodes.
     */
    public static NavigableMap<Range, List<Node>> replicate(List<Node> nodes, int rf)
    {
        nodes.sort(Node::compareTo);
        List<Range> ranges = toRanges(nodes);
        return replicate(ranges, nodes, rf);
    }

    public static List<Node> getReplicas(NavigableMap<TokenPlacementModel.Range, List<TokenPlacementModel.Node>> ring, long token)
    {
        return ring.get(ring.floorKey(new Range(token, token, true)));
    }

    public static NavigableMap<Range, List<Node>> replicate(List<Range> ranges, List<Node> nodes, int rf)
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
                    replication.put(ranges.get(ranges.size() - 1), replicas);
                replication.put(range, replicas);
            }
            else
            {
                // if the range end is larger than the highest assigned token, then treat it
                // as part of the wraparound and replicate it to the same nodes as the first
                // range. This is most likely caused by a decommission removing the node with
                // the largest token.
                replication.put(range, replication.get(ranges.get(0)));
            }
        }
        return replication;
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
    public static List<Range> toRanges(List<Node> nodes)
    {
        List<Long> tokens = new ArrayList<>();
        for (Node node : nodes)
        {
            if (node.token != Long.MIN_VALUE)
                tokens.add(node.token);
        }
        tokens.add(Long.MIN_VALUE);
        tokens.sort(Long::compareTo);

        List<Range> ranges = new ArrayList<>(tokens.size() + 1);
        long prev = tokens.get(0);
        for (int i = 1; i < tokens.size(); i++)
        {
            long current = tokens.get(i);
            ranges.add(new Range(prev, current));
            prev = current;
        }
        ranges.add(new Range(prev, Long.MIN_VALUE));
        return Collections.unmodifiableList(ranges);
    }

    public static List<Node> peerStateToNodes(Object[][] resultset)
    {
        List<Node> nodes = new ArrayList<>();
        for (Object[] row : resultset)
        {
            InetAddress address = (InetAddress) row[0];
            Set<String> tokens = (Set<String>) row[1];
            for (String token : tokens)
                nodes.add(new Node(Long.parseLong(token), address.toString()));
        }
        return nodes;
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

        public Node(long token, String id)
        {
            this.token = token;
            this.id = id;
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
