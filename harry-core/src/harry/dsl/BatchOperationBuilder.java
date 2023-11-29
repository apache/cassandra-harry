package harry.dsl;

public interface BatchOperationBuilder
{
    SingleOperationBuilder beginBatch();

    /**
     * Begin batch for a partition descriptor at a specific index.
     *
     * Imagine all partition descriptors were longs in an array. Index of a descriptor
     * is a sequential number of the descriptor in this imaginary array.
     */
    SingleOperationBuilder beginBatch(long pdIdx);
}
