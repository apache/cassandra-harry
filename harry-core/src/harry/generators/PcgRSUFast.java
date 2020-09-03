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

package harry.generators;

public class PcgRSUFast implements RandomGenerator
{
    private long state;
    private long step;

    /**
     * Stream number of the rng.
     */
    private final long stream;

    public PcgRSUFast(long seed, long streamNumber)
    {
        this.stream = (streamNumber << 1) | 1; // 2* + 1
        seed(seed);
    }

    public void seed(long seed)
    {
        state = RngUtils.xorshift64star(seed) + stream;
    }

    public void advance(long steps)
    {
        this.step += steps;
        this.state = PCGFastPure.advanceState(state, steps, stream);
    }

    protected void nextStep()
    {
        state = PCGFastPure.nextState(state, stream);
        step++;
    }

    public void seek(long step)
    {
        advance(step - this.step);
    }

    public long next()
    {
        nextStep();
        return PCGFastPure.shuffle(state);
    }

    public long distance(long generated)
    {
        return PCGFastPure.distance(state, PCGFastPure.unshuffle(generated), stream);
    }

    public long distance(PcgRSUFast other)
    {
        // Check if they are the same stream...
        if (stream != other.stream)
        {
            throw new IllegalArgumentException("Can not compare generators with different " +
                                               "streams. Those generators will never converge");
        }

        return PCGFastPure.distance(state, other.state, stream);
    }
}