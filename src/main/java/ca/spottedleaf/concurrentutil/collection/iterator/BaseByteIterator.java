package ca.spottedleaf.concurrentutil.collection.iterator;

public abstract class BaseByteIterator extends BaseObjectIterator<Byte> {

    @Override
    public final Byte next() {
        return Byte.valueOf(this.nextByte());
    }

    public abstract byte nextByte();
}
