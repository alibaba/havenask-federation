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
*
* Modifications Copyright Havenask Contributors. See
* GitHub history for details.
*/

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * Mappings. Mappings define the way that documents should be translated to
 * Lucene indices, for instance whether a string field should be indexed as a
 * {@link org.havenask.index.mapper.TextFieldMapper text} or
 * {@link org.havenask.index.mapper.KeywordFieldMapper keyword} field,
 * etc. This parsing is done by the
 * {@link org.havenask.index.mapper.DocumentParser} class which delegates
 * to various {@link org.havenask.index.mapper.Mapper} implementations for
 * per-field handling.
 * <p>Mappings support the addition of new fields, so that fields can be added
 * to indices even though users had not thought about them at index creation
 * time. However, the removal of fields is not supported, as it would allow to
 * re-add a field with a different configuration under the same name, which
 * Lucene cannot handle. Introduction of new fields into the mappings is handled
 * by the {@link org.havenask.index.mapper.MapperService} class.
 */


package org.havenask.index.mapper;

