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

package harry.model.sut.injvm;

import java.util.ArrayList;
import java.util.List;

import harry.core.Run;
import harry.model.QuiescentLocalStateCheckerBase;
import harry.model.sut.TokenPlacementModel;
import harry.util.ByteUtils;
import harry.util.TokenUtil;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstance;

public class QuiescentLocalStateChecker extends QuiescentLocalStateCheckerBase
{
    public QuiescentLocalStateChecker(Run run)
    {
        super(run);
    }

    public static ModelFactory factory(TokenPlacementModel.ReplicationFactor rf)
    {
        return (run) -> new QuiescentLocalStateChecker(run, rf);
    }

    public QuiescentLocalStateChecker(Run run, TokenPlacementModel.ReplicationFactor rf)
    {
        super(run, rf);
    }

    @Override
    protected TokenPlacementModel.ReplicatedRanges getRing()
    {
        List<TokenPlacementModel.Node> other = TokenPlacementModel.peerStateToNodes(((InJvmSutBase<?, ?>) sut).cluster.coordinator(1).execute("select peer, tokens, data_center, rack from system.peers", ConsistencyLevel.ONE));
        List<TokenPlacementModel.Node> self = TokenPlacementModel.peerStateToNodes(((InJvmSutBase<?, ?>) sut).cluster.coordinator(1).execute("select broadcast_address, tokens, data_center, rack from system.local", ConsistencyLevel.ONE));
        List<TokenPlacementModel.Node> all = new ArrayList<>();
        all.addAll(self);
        all.addAll(other);
        all.sort(TokenPlacementModel.Node::compareTo);
        return rf.replicate(all);
    }

    @Override
    protected Object[][] executeNodeLocal(String statement, TokenPlacementModel.Node node, Object... bindings)
    {
        IInstance instance = ((InJvmSutBase<?, ?>) sut).cluster
                             .stream()
                             .filter((n) -> n.config().broadcastAddress().toString().contains(node.id))
                             .findFirst()
                             .get();
        return instance.executeInternal(statement, bindings);
    }

    protected long token(long pd)
    {
        return TokenUtil.token(ByteUtils.compose(ByteUtils.objectsToBytes(schema.inflatePartitionKey(pd))));
    }
}
