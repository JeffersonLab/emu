/*
 * =============================================================================
 *
 *   Copyright (c) 2010, The JAVATUPLES team (http://www.javatuples.org)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 * =============================================================================
 */
package org.jlab.coda.emu.support.data.tuple;

import java.util.Collection;
import java.util.Iterator;


/**
 * <p>
 * A tuple of four elements.
 * </p>
 *
 * @since 1.0
 *
 * @author Daniel Fern&aacute;ndez
 *
 */
public final class Quartet<A,B,C,D>
        extends Tuple
        implements IValue0<A>,
        IValue1<B>,
        IValue2<C>,
        IValue3<D> {

    private static final long serialVersionUID = 2445136048617019549L;

    private static final int SIZE = 4;

    private final A val0;
    private final B val1;
    private final C val2;
    private final D val3;



    public static <A,B,C,D> Quartet<A,B,C,D> with(final A value0, final B value1, final C value2, final D value3) {
        return new Quartet<A,B,C,D>(value0,value1,value2,value3);
    }


    /**
     * <p>
     * Create tuple from array. Array has to have exactly four elements.
     * </p>
     *
     * @param <X> the array component type
     * @param array the array to be converted to a tuple
     * @return the tuple
     */
    public static <X> Quartet<X,X,X,X> fromArray(final X[] array) {
        if (array == null) {
            throw new IllegalArgumentException("Array cannot be null");
        }
        if (array.length != 4) {
            throw new IllegalArgumentException("Array must have exactly 4 elements in order to create a Quartet. Size is " + array.length);
        }
        return new Quartet<X,X,X,X>(array[0],array[1],array[2],array[3]);
    }


    /**
     * <p>
     * Create tuple from collection. Collection has to have exactly four elements.
     * </p>
     *
     * @param <X> the collection component type
     * @param collection the collection to be converted to a tuple
     * @return the tuple
     */
    public static <X> Quartet<X,X,X,X> fromCollection(final Collection<X> collection) {
        return fromIterable(collection);
    }




    /**
     * <p>
     * Create tuple from iterable. Iterable has to have exactly four elements.
     * </p>
     *
     * @param <X> the iterable component type
     * @param iterable the iterable to be converted to a tuple
     * @return the tuple
     */
    public static <X> Quartet<X,X,X,X> fromIterable(final Iterable<X> iterable) {
        return fromIterable(iterable, 0, true);
    }



    /**
     * <p>
     * Create tuple from iterable, starting from the specified index. Iterable
     * can have more (or less) elements than the tuple to be created.
     * </p>
     *
     * @param <X> the iterable component type
     * @param iterable the iterable to be converted to a tuple
     * @return the tuple
     */
    public static <X> Quartet<X,X,X,X> fromIterable(final Iterable<X> iterable, int index) {
        return fromIterable(iterable, index, false);
    }




    private static <X> Quartet<X,X,X,X> fromIterable(final Iterable<X> iterable, int index, final boolean exactSize) {

        if (iterable == null) {
            throw new IllegalArgumentException("Iterable cannot be null");
        }

        boolean tooFewElements = false;

        X element0 = null;
        X element1 = null;
        X element2 = null;
        X element3 = null;

        final Iterator<X> iter = iterable.iterator();

        int i = 0;
        while (i < index) {
            if (iter.hasNext()) {
                iter.next();
            } else {
                tooFewElements = true;
            }
            i++;
        }

        if (iter.hasNext()) {
            element0 = iter.next();
        } else {
            tooFewElements = true;
        }

        if (iter.hasNext()) {
            element1 = iter.next();
        } else {
            tooFewElements = true;
        }

        if (iter.hasNext()) {
            element2 = iter.next();
        } else {
            tooFewElements = true;
        }

        if (iter.hasNext()) {
            element3 = iter.next();
        } else {
            tooFewElements = true;
        }

        if (tooFewElements && exactSize) {
            throw new IllegalArgumentException("Not enough elements for creating a Quartet (4 needed)");
        }

        if (iter.hasNext() && exactSize) {
            throw new IllegalArgumentException("Iterable must have exactly 4 available elements in order to create a Quartet.");
        }

        return new Quartet<X,X,X,X>(element0, element1, element2, element3);

    }




    public Quartet(
            final A value0,
            final B value1,
            final C value2,
            final D value3) {
        super(value0, value1, value2, value3);
        this.val0 = value0;
        this.val1 = value1;
        this.val2 = value2;
        this.val3 = value3;
    }


    public A getValue0() {
        return this.val0;
    }


    public B getValue1() {
        return this.val1;
    }


    public C getValue2() {
        return this.val2;
    }


    public D getValue3() {
        return this.val3;
    }


    @Override
    public int getSize() {
        return SIZE;
    }



    public <X> Quartet<X,B,C,D> setAt0(final X value) {
        return new Quartet<X,B,C,D>(
                value, this.val1, this.val2, this.val3);
    }

    public <X> Quartet<A,X,C,D> setAt1(final X value) {
        return new Quartet<A,X,C,D>(
                this.val0, value, this.val2, this.val3);
    }

    public <X> Quartet<A,B,X,D> setAt2(final X value) {
        return new Quartet<A,B,X,D>(
                this.val0, this.val1, value, this.val3);
    }

    public <X> Quartet<A,B,C,X> setAt3(final X value) {
        return new Quartet<A,B,C,X>(
                this.val0, this.val1, this.val2, value);
    }



    public Triplet<B,C,D> removeFrom0() {
        return new Triplet<B,C,D>(
                this.val1, this.val2, this.val3);
    }

    public Triplet<A,C,D> removeFrom1() {
        return new Triplet<A,C,D>(
                this.val0, this.val2, this.val3);
    }

    public Triplet<A,B,D> removeFrom2() {
        return new Triplet<A,B,D>(
                this.val0, this.val1, this.val3);
    }

    public Triplet<A,B,C> removeFrom3() {
        return new Triplet<A,B,C>(
                this.val0, this.val1, this.val2);
    }


}