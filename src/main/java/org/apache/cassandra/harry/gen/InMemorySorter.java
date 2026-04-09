/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.harry.gen;

/**
 * Simple in-place quicksort for {@code long[]} arrays.
 * Sorts {@code arr[0..length-1]} using the provided {@link LongComparator}.
 * Not intended for massive arrays; use {@link ExternalSorter} for that.
 */
class InMemorySorter
{
    @FunctionalInterface
    interface LongComparator
    {
        int compare(long a, long b);
    }

    void sort(long[] arr, int length, LongComparator comparator)
    {
        quicksort(arr, 0, length - 1, comparator);
    }

    private void quicksort(long[] arr, int lo, int hi, LongComparator comparator)
    {
        if (lo >= hi)
            return;

        int pivotIdx = partition(arr, lo, hi, comparator);
        quicksort(arr, lo, pivotIdx - 1, comparator);
        quicksort(arr, pivotIdx + 1, hi, comparator);
    }

    // Lomuto partition: pivot = arr[hi]
    private int partition(long[] arr, int lo, int hi, LongComparator comparator)
    {
        long pivot = arr[hi];
        int i = lo - 1;
        for (int j = lo; j < hi; j++)
        {
            if (comparator.compare(arr[j], pivot) <= 0)
            {
                i++;
                long tmp = arr[i]; arr[i] = arr[j]; arr[j] = tmp;
            }
        }
        long tmp = arr[i + 1]; arr[i + 1] = arr[hi]; arr[hi] = tmp;
        return i + 1;
    }
}
