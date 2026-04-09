package org.apache.cassandra.harry.dml.sql;

import java.sql.SQLException;
import java.util.List;

public interface StatementExecutor
{
    void execute(RenderedStatement stmt) throws SQLException;

    List<IndexedRow> query(RenderedStatement stmt, TableSpec.Column[] columns) throws SQLException;
}
