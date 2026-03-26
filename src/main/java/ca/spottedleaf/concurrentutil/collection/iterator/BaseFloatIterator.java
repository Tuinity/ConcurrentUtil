package ca.spottedleaf.concurrentutil.collection.iterator;

public abstract class BaseFloatIterator extends BaseObjectIterator<Float> {

    @Override
    public final Float next() {
        return Float.valueOf(this.nextFloat());
    }

    public abstract float nextFloat();

}
