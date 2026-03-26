package ca.spottedleaf.concurrentutil.collection.iterator;

import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.function.LongConsumer;

public abstract class BaseLongIterator extends BaseObjectIterator<Long> implements PrimitiveIterator.OfLong {

    @Override
    public final Long next() {
        return Long.valueOf(this.nextLong());
    }

    @Override
    public abstract long nextLong();

    @Override
    public final void forEachRemaining(final LongConsumer action) {
        Objects.requireNonNull(action, "Action may not be null");
        while (this.hasNext()) {
            action.accept(this.nextLong());
        }
    }
}
