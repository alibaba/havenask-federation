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

package org.havenask.common.inject;

import org.havenask.common.inject.internal.Errors;
import org.havenask.common.inject.internal.ErrorsException;
import org.havenask.common.inject.internal.InternalContext;
import org.havenask.common.inject.internal.InternalFactory;
import org.havenask.common.inject.internal.ToStringBuilder;
import org.havenask.common.inject.spi.Dependency;

/**
 * @author crazybob@google.com (Bob Lee)
 */
class ConstantFactory<T> implements InternalFactory<T> {

    private final Initializable<T> initializable;

    ConstantFactory(Initializable<T> initializable) {
        this.initializable = initializable;
    }

    @Override
    public T get(Errors errors, InternalContext context, Dependency dependency)
            throws ErrorsException {
        return initializable.get(errors);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(ConstantFactory.class)
                .add("value", initializable)
                .toString();
    }
}
