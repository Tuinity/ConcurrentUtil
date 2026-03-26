package ca.spottedleaf.concurrentutil.collection.iterator;

import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.function.IntConsumer;

public abstract class BaseIntIterator extends BaseObjectIterator<Integer> implements PrimitiveIterator.OfInt {

    @Override
    public final Integer next() {
        return Integer.valueOf(this.nextInt());
    }

    @Override
    public abstract int nextInt();

    @Override
    public final void forEachRemaining(final IntConsumer action) {
        Objects.requireNonNull(action, "Action may not be null");
        while (this.hasNext()) {
            action.accept(this.nextInt());
        }
    }
}
