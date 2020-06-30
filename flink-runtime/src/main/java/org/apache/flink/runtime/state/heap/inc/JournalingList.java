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

package org.apache.flink.runtime.state.heap.inc;

import org.apache.flink.runtime.state.heap.inc.StateDiff.ListDiff;

import org.apache.commons.collections.list.AbstractListDecorator;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Spliterator;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

// todo: review & reduces overrides
class JournalingList<T> extends AbstractListDecorator {
	private final List<T> list;
	private final int initialLength;
	private boolean isCleared;
	private boolean isExposed;

	JournalingList(List<T> list, boolean wasCleared) {
		super(list);
		this.list = list; // todo: use super.collection?
		this.initialLength = list.size();
		this.isCleared = wasCleared;
	}

	@Override
	public void clear() {
		isCleared = true;
		super.clear();
	}

	public ListDiff<T> toDiff() {
		return isCleared || isExposed ? new ListDiff<>(list, true) : new ListDiff<>(list.subList(initialLength, list.size()), false);
	}

	@Override
	public ListIterator listIterator() {
		isExposed = true;
		return super.listIterator();
	}

	@Override
	public ListIterator listIterator(int index) {
		isExposed = true;
		return super.listIterator(index);
	}

	@Override
	public Object remove(int index) {
		isExposed = true;
		return super.remove(index);
	}

	@Override
	public Object set(int index, Object object) {
		isExposed = true;
		return super.set(index, object);
	}

	@Override
	public boolean removeIf(Predicate filter) {
		isExposed = true;
		return super.removeIf(filter);
	}

	@Override
	public boolean removeAll(Collection coll) {
		isExposed = true;
		return super.removeAll(coll);
	}

	@Override
	public boolean retainAll(Collection coll) {
		isExposed = true;
		return super.retainAll(coll);
	}

	@Override
	public boolean remove(Object object) {
		isExposed = true;
		return super.remove(object);
	}

	@Override
	public void replaceAll(UnaryOperator operator) {
		isExposed = true;
		super.replaceAll(operator);
	}

	@Override
	public Iterator iterator() {
		isExposed = true;
		return super.iterator();
	}

	@Override
	public List subList(int fromIndex, int toIndex) {
		isExposed = true;
		return super.subList(fromIndex, toIndex);
	}

	@Override
	public void sort(Comparator c) {
		isExposed = true;
		super.sort(c);
	}

	@Override
	public Spliterator spliterator() {
		isExposed = true;
		return super.spliterator();
	}

	List<T> unwrap() {
		return list;
	}

	public boolean isStateExposed() {
		return isExposed;
	}

	boolean isCleared() {
		return isExposed || isCleared;
	}
}
