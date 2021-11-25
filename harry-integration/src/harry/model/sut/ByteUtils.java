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
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.UUIDType;

public class ByteUtils extends harry.util.ByteUtils
{
    public static ByteBuffer objectToBytes(Object obj)
    {
        if (obj instanceof Integer)
            return bytes((int) obj);
        else if (obj instanceof Byte)
            return bytes((byte) obj);
        else if (obj instanceof Boolean)
            return bytes( (byte) (((boolean) obj) ? 1 : 0));
        else if (obj instanceof Short)
            return bytes((short) obj);
        else if (obj instanceof Long)
            return bytes((long) obj);
        else if (obj instanceof Float)
            return bytes((float) obj);
        else if (obj instanceof Double)
            return bytes((double) obj);
        else if (obj instanceof UUID)
            return bytes((UUID) obj);
        else if (obj instanceof InetAddress)
            return bytes((InetAddress) obj);
        else if (obj instanceof String)
            return bytes((String) obj);
        else if (obj instanceof List)
        {
            List l = (List) obj;
            if (l.isEmpty())
                return pack(Collections.emptyList(), 0);
            return ListType.getInstance(getType(l.get(0)), true).decompose(l);
        }
        else if (obj instanceof Set)
        {
            Set l = (Set) obj;
            if (l.isEmpty())
                return pack(Collections.emptyList(), 0);
            return SetType.getInstance(getType(l.iterator().next()), true).decompose(l);
        }
        else if (obj instanceof ByteBuffer)
            return (ByteBuffer) obj;

        else
            throw new IllegalArgumentException(String.format("Cannot convert value %s of type %s",
                                                             obj,
                                                             obj.getClass()));
    }

    public static AbstractType<?> getType(Object obj)
    {
        if (obj instanceof Integer)
            return IntegerType.instance; //TODO
        else if (obj instanceof Byte)
            return ByteType.instance;
        else if (obj instanceof Boolean)
            return BooleanType.instance;
        else if (obj instanceof Short)
            return ShortType.instance;
        else if (obj instanceof Long)
            return IntegerType.instance;
        else if (obj instanceof Float)
            return FloatType.instance;
        else if (obj instanceof Double)
            return DoubleType.instance;
        else if (obj instanceof UUID)
            return UUIDType.instance;
        else if (obj instanceof InetAddress)
            return InetAddressType.instance;
        else if (obj instanceof String)
            return AsciiType.instance;
        else
            throw new IllegalArgumentException(String.format("Cannot convert value %s of type %s",
                                                             obj,
                                                             obj.getClass()));
    }
}
