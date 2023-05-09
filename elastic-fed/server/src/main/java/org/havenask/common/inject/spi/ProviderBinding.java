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

package org.havenask.common.inject.spi;

import org.havenask.common.inject.Binding;
import org.havenask.common.inject.Key;
import org.havenask.common.inject.Provider;

/**
 * A binding to a {@link Provider} that delegates to the binding for the provided type. This binding
 * is used whenever a {@code Provider<T>} is injected (as opposed to injecting {@code T} directly).
 *
 * @author jessewilson@google.com (Jesse Wilson)
 * @since 2.0
 */
public interface ProviderBinding<T extends Provider<?>> extends Binding<T> {

    /**
     * Returns the key whose binding is used to {@link Provider#get provide instances}.
     */
    Key<?> getProvidedKey();
}
