package ca.spottedleaf.concurrentutil.map;

import ca.spottedleaf.concurrentutil.collection.iterator.BaseObjectIterator;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Predicate;

public abstract class BaseMapCollection<E> implements Collection<E> {

    @Override
    public abstract int size();

    @Override
    public abstract boolean isEmpty();

    @Override
    public abstract boolean contains(final Object value);

    protected abstract List<E> asList();

    @Override
    public abstract BaseObjectIterator<E> iterator();

    @Override
    public void forEach(final Consumer<? super E> action) {
        this.iterator().forEachRemaining(action);
    }

    @Override
    public Object[] toArray() {
        return this.asList().toArray();
    }

    @Override
    public <T> T[] toArray(final T[] a) {
        return this.asList().toArray(a);
    }

    @Override
    public <T> T[] toArray(final IntFunction<T[]> generator) {
        return this.asList().toArray(generator);
    }

    @Override
    public boolean containsAll(final Collection<?> collection) {
        for (final Object value : collection) {
            if (!this.contains(value)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean add(final E value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(final Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(final Collection<? extends E> collection) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(final Collection<?> collection) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeIf(final Predicate<? super E> filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(final Collection<?> collection) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }
}
