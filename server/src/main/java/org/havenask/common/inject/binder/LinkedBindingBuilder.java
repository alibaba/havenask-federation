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
 * Copyright (C) 2006 Google Inc.
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

package org.havenask.common.inject.binder;

import org.havenask.common.inject.Key;
import org.havenask.common.inject.Provider;
import org.havenask.common.inject.TypeLiteral;

/**
 * See the EDSL examples at {@link org.havenask.common.inject.Binder}.
 *
 * @author crazybob@google.com (Bob Lee)
 */
public interface LinkedBindingBuilder<T> extends ScopedBindingBuilder {

    /**
     * See the EDSL examples at {@link org.havenask.common.inject.Binder}.
     */
    ScopedBindingBuilder to(Class<? extends T> implementation);

    /**
     * See the EDSL examples at {@link org.havenask.common.inject.Binder}.
     */
    ScopedBindingBuilder to(TypeLiteral<? extends T> implementation);

    /**
     * See the EDSL examples at {@link org.havenask.common.inject.Binder}.
     */
    ScopedBindingBuilder to(Key<? extends T> targetKey);

    /**
     * See the EDSL examples at {@link org.havenask.common.inject.Binder}.
     *
     * @see org.havenask.common.inject.Injector#injectMembers
     */
    void toInstance(T instance);

    /**
     * See the EDSL examples at {@link org.havenask.common.inject.Binder}.
     *
     * @see org.havenask.common.inject.Injector#injectMembers
     */
    ScopedBindingBuilder toProvider(Provider<? extends T> provider);

    /**
     * See the EDSL examples at {@link org.havenask.common.inject.Binder}.
     */
    ScopedBindingBuilder toProvider(
            Class<? extends Provider<? extends T>> providerType);

    /**
     * See the EDSL examples at {@link org.havenask.common.inject.Binder}.
     */
    ScopedBindingBuilder toProvider(
            Key<? extends Provider<? extends T>> providerKey);
}
