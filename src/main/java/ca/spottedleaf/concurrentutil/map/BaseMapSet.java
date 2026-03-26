package ca.spottedleaf.concurrentutil.map;

import java.util.Set;

public abstract class BaseMapSet<E> extends BaseMapCollection<E> implements Set<E> {

    @Override
    public int hashCode() {
        int h = 0;

        for (final E element : this) {
            h += (element == null ? 0 : element.hashCode());
        }

        return h;
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof Set<?> set)) {
            return false;
        }

        if (this.size() != set.size()) {
            return false;
        }

        for (final E element : this) {
            if (!set.contains(element)) {
                return false;
            }
        }

        return true;
    }
}
