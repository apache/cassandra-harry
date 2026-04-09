package org.apache.cassandra.harry.dml.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.harry.gen.EntropySource;

/**
 * Tracks which PK tuples are currently present in the table.
 * All operations are O(1) via swap-with-last removal.
 */
public class KeyTracker
{
    private final List<PkTuple> tuples = new ArrayList<>();
    private final Map<PkTuple, Integer> index = new HashMap<>();

    public void track(PkTuple tuple)
    {
        if (!index.containsKey(tuple))
        {
            index.put(tuple, tuples.size());
            tuples.add(tuple);
        }
    }

    public void untrack(PkTuple tuple)
    {
        Integer idx = index.remove(tuple);
        if (idx != null)
        {
            int last = tuples.size() - 1;
            if (idx != last)
            {
                PkTuple moved = tuples.get(last);
                tuples.set(idx, moved);
                index.put(moved, idx);
            }
            tuples.remove(last);
        }
    }

    public boolean contains(PkTuple tuple)
    {
        return index.containsKey(tuple);
    }

    public boolean isEmpty()
    {
        return tuples.isEmpty();
    }

    public int size()
    {
        return tuples.size();
    }

    public PkTuple pickRandom(EntropySource rng)
    {
        return tuples.get(rng.nextInt(tuples.size()));
    }

    public void clear()
    {
        tuples.clear();
        index.clear();
    }
}
