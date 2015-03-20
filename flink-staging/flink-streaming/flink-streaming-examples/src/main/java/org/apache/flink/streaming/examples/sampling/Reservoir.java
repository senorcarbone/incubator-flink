/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.sampling;



 import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
 import org.apache.flink.util.IterableIterator;

 import java.io.Serializable;
 import java.util.ArrayList;
 import java.util.Iterator;
 import java.util.Random;

/**
 * Created by marthavk on 2015-03-05.
 */
public class Reservoir<T> implements Serializable, Iterable {

    private ArrayList<T> reservoir;
    private int maxSize;


    public Reservoir(int size) {
        reservoir = new ArrayList<T>();
        maxSize = size;

        // maxSize = size;
    }

    /** GETTERS AND SETTERS**/
    public ArrayList<T> getReservoir() {
        return reservoir;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public int getSize() { return reservoir.size(); }

    public void setReservoir(ArrayList<T> reservoir) {
        this.reservoir = reservoir;
    }

    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }


    /** INSERT & REPLACE METHODS **/
    void insertElement(T e) {
        if (!isFull()) reservoir.add(e);
        else replaceElement(e);
    }


    /**
     * Chooses an existing element uniformly at random and replaces it with e
     * @param e
     */

    void replaceElement(T e) {
        if (isFull()) {
            // choose position uniformly at random
            int pos = new Random().nextInt(maxSize);
            // replace element at pos
            reservoir.set(pos, e);
        }
        else throw new ArrayIndexOutOfBoundsException();
    }

    /** AUXILIARY METHODS **/
    boolean isFull() {
        return reservoir.isEmpty() ? false :  reservoir.size()==maxSize;
    }


    @Override
    public String toString() {
        return reservoir.toString();
    }


    void print() {
        System.out.println(reservoir.toString());
    }

    /** MERGE METHODS **/
    static Reservoir merge(Reservoir r1, Reservoir r2) {
        Reservoir rout = new Reservoir(r1.maxSize+r2.maxSize);
        rout.reservoir.addAll(r1.reservoir);
        rout.reservoir.addAll(r2.reservoir);
        return rout;
    }

    public void mergeWith(Reservoir<T> r1) {
        this.setMaxSize(r1.getMaxSize() + this.getMaxSize());
        ArrayList<T> newReservoir = new ArrayList<T>();
        newReservoir.addAll(this.getReservoir());
        newReservoir.addAll(r1.getReservoir());
    }

    @Override
    public Iterator iterator() {
        return reservoir.iterator();
    }


}
