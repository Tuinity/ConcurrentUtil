package ca.spottedleaf.concurrentutil.collection.iterator;

public abstract class BaseBooleanIterator extends BaseObjectIterator<Boolean> {

    @Override
    public final Boolean next() {
        return Boolean.valueOf(this.nextBoolean());
    }

    public abstract boolean nextBoolean();
}
