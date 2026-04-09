package org.apache.cassandra.harry.dml.sql;

public sealed interface Statement permits UpdateStatement, SelectStatement, DeleteStatement, InsertStatement {}
