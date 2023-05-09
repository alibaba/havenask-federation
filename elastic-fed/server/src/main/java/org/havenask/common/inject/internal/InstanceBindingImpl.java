/*
*Copyright (c) 2021, Alibaba Group;
*Licensed under the Apache License, Version 2.0 (the "License");
*you may not use this file except in compliance with the License.
*You may obtain a copy of the License at

*   http://www.apache.org/licenses/LICENSE-2.0

*Unless required by applicable law or agreed to in writing, software
*distributed under the License is distributed on an "AS IS" BASIS,
*WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*See the License for the specific language governing permissions and
*limitations under the License.
*/

/*
 * Copyright (C) 2008 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Modifications Copyright Havenask Contributors. See
 * GitHub history for details.
 */

package org.havenask.common.inject.internal;

import org.havenask.common.inject.Binder;
import org.havenask.common.inject.Injector;
import org.havenask.common.inject.Key;
import org.havenask.common.inject.Provider;
import org.havenask.common.inject.spi.BindingTargetVisitor;
import org.havenask.common.inject.spi.Dependency;
import org.havenask.common.inject.spi.HasDependencies;
import org.havenask.common.inject.spi.InjectionPoint;
import org.havenask.common.inject.spi.InstanceBinding;
import org.havenask.common.inject.util.Providers;

import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;

public class InstanceBindingImpl<T> extends BindingImpl<T> implements InstanceBinding<T> {

    final T instance;
    final Provider<T> provider;
    final Set<InjectionPoint> injectionPoints;

    public InstanceBindingImpl(Injector injector, Key<T> key, Object source,
                               InternalFactory<? extends T> internalFactory, Set<InjectionPoint> injectionPoints,
                               T instance) {
        super(injector, key, source, internalFactory, Scoping.UNSCOPED);
        this.injectionPoints = injectionPoints;
        this.instance = instance;
        this.provider = Providers.of(instance);
    }

    public InstanceBindingImpl(Object source, Key<T> key, Scoping scoping,
                               Set<InjectionPoint> injectionPoints, T instance) {
        super(source, key, scoping);
        this.injectionPoints = injectionPoints;
        this.instance = instance;
        this.provider = Providers.of(instance);
    }

    @Override
    public Provider<T> getProvider() {
        return this.provider;
    }

    @Override
    public <V> V acceptTargetVisitor(BindingTargetVisitor<? super T, V> visitor) {
        return visitor.visit(this);
    }

    @Override
    public T getInstance() {
        return instance;
    }

    @Override
    public Set<InjectionPoint> getInjectionPoints() {
        return injectionPoints;
    }

    @Override
    public Set<Dependency<?>> getDependencies() {
        return instance instanceof HasDependencies
                ? unmodifiableSet(new HashSet<>((((HasDependencies) instance).getDependencies())))
                : Dependency.forInjectionPoints(injectionPoints);
    }

    @Override
    public BindingImpl<T> withScoping(Scoping scoping) {
        return new InstanceBindingImpl<>(getSource(), getKey(), scoping, injectionPoints, instance);
    }

    @Override
    public BindingImpl<T> withKey(Key<T> key) {
        return new InstanceBindingImpl<>(getSource(), key, getScoping(), injectionPoints, instance);
    }

    @Override
    public void applyTo(Binder binder) {
        // instance bindings aren't scoped
        binder.withSource(getSource()).bind(getKey()).toInstance(instance);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(InstanceBinding.class)
                .add("key", getKey())
                .add("source", getSource())
                .add("instance", instance)
                .toString();
    }
}
