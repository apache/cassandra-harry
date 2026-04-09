package org.apache.cassandra.harry.dml.sql;

import org.apache.cassandra.harry.DataType;

public class Column<T> {
    public final String name;

    public final DataType<T> type;
    /**
     * Per-column population override; 0 means use the schema-wide default
     * (populationPerColumn).
     */
    public final int population;

    public Column(String name, DataType<T> type, int population) {
        this.name = name;
        this.type = type;
        this.population = population;
    }
}
