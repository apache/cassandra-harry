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

package harry.model.sut.external;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.UtilsAccessor;
import harry.core.Run;
import harry.model.QuiescentLocalStateCheckerBase;
import harry.model.sut.TokenPlacementModel;
import harry.util.ByteUtils;

import static harry.model.sut.TokenPlacementModel.Node;

public class QuiescentLocalStateChecker extends QuiescentLocalStateCheckerBase
{
    public QuiescentLocalStateChecker(Run run)
    {
        this(run, new TokenPlacementModel.SimpleReplicationFactor(3));
    }

    public QuiescentLocalStateChecker(Run run, TokenPlacementModel.ReplicationFactor rf)
    {
        super(run, rf);
    }

    @Override
    protected TokenPlacementModel.ReplicatedRanges getRing()
    {
        Session session = ((ExternalClusterSut) sut).session();
        Host control = session.getState().getConnectedHosts().stream().findFirst().get();
        SimpleStatement peers = new SimpleStatement("select peer, tokens, data_center, rack from system.peers");
        peers.setHost(control);
        List<TokenPlacementModel.Node> other = TokenPlacementModel.peerStateToNodes(ExternalClusterSut.resultSetToObjectArray(session.execute(peers)));
        SimpleStatement local = new SimpleStatement("select broadcast_address, tokens, data_center, rack from system.local");
        local.setHost(control);

        List<TokenPlacementModel.Node> self = TokenPlacementModel.peerStateToNodes(ExternalClusterSut.resultSetToObjectArray(session.execute(local)));
        List<TokenPlacementModel.Node> all = new ArrayList<>();
        all.addAll(self);
        all.addAll(other);
        all.sort(TokenPlacementModel.Node::compareTo);
        return rf.replicate(all);
    }

    @Override
    protected long token(long pd)
    {
        return UtilsAccessor.token(ByteUtils.compose(ByteUtils.objectsToBytes(schema.inflatePartitionKey(pd))));
    }


    @Override
    protected Object[][] executeNodeLocal(String statement, Node node, Object... bindings)
    {
        return ((ExternalClusterSut) sut).executeNodeLocal(statement,
                                                           (host) -> host.getAddress().toString().contains(node.id),
                                                           bindings);
    }
}
