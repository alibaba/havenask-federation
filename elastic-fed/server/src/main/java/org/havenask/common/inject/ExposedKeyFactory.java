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

package org.havenask.common.inject;

import org.havenask.common.inject.internal.BindingImpl;
import org.havenask.common.inject.internal.Errors;
import org.havenask.common.inject.internal.ErrorsException;
import org.havenask.common.inject.internal.InternalContext;
import org.havenask.common.inject.internal.InternalFactory;
import org.havenask.common.inject.spi.Dependency;
import org.havenask.common.inject.spi.PrivateElements;

/**
 * This factory exists in a parent injector. When invoked, it retrieves its value from a child
 * injector.
 */
class ExposedKeyFactory<T> implements InternalFactory<T>, BindingProcessor.CreationListener {
    private final Key<T> key;
    private final PrivateElements privateElements;
    private BindingImpl<T> delegate;

    ExposedKeyFactory(Key<T> key, PrivateElements privateElements) {
        this.key = key;
        this.privateElements = privateElements;
    }

    @Override
    public void notify(Errors errors) {
        InjectorImpl privateInjector = (InjectorImpl) privateElements.getInjector();
        BindingImpl<T> explicitBinding = privateInjector.state.getExplicitBinding(key);

        // validate that the child injector has its own factory. If the getInternalFactory() returns
        // this, then that child injector doesn't have a factory (and getExplicitBinding has returned
        // its parent's binding instead
        if (explicitBinding.getInternalFactory() == this) {
            errors.withSource(explicitBinding.getSource()).exposedButNotBound(key);
            return;
        }

        this.delegate = explicitBinding;
    }

    @Override
    public T get(Errors errors, InternalContext context, Dependency<?> dependency)
            throws ErrorsException {
        return delegate.getInternalFactory().get(errors, context, dependency);
    }
}
