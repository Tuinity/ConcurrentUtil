package ca.spottedleaf.concurrentutil.collection.iterator;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Consumer;

public abstract class BaseObjectIterator<E> implements Iterator<E> {

    @Override
    public abstract boolean hasNext();

    @Override
    public abstract E next();

    @Override
    public abstract void remove();

    @Override
    public final void forEachRemaining(final Consumer<? super E> action) {
        Objects.requireNonNull(action, "Action may not be null");
        while (this.hasNext()) {
            action.accept(this.next());
        }
    }
}
