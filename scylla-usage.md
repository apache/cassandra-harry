
### Install

```
git clone -b scylla-branch https://github.com/amoskong/cassandra-harry
cd cassandra-harry
make mvn
sudo ln -s `realpath scripts/cassandra-harry` /usr/bin/cassandra-harry
```

### Execute

```
HARRY_HOME=~/cassandra-harry cassandra-harry
```
