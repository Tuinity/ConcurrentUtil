package ca.spottedleaf.concurrentutil.collection.iterator;

public abstract class BaseShortIterator extends BaseObjectIterator<Short> {

    @Override
    public final Short next() {
        return Short.valueOf(this.nextShort());
    }

    public abstract short nextShort();
}
