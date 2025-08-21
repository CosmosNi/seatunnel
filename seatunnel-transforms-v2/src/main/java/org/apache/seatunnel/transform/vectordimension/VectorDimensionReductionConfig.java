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

package org.apache.seatunnel.transform.vectordimension;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.util.List;

public class VectorDimensionReductionConfig {

    public static final Option<List<String>> VECTOR_FIELDS =
            Options.key("vector_fields")
                    .listType()
                    .noDefaultValue()
                    .withDescription("List of vector field names to apply dimension reduction");

    public static final Option<Integer> TARGET_DIMENSION =
            Options.key("target_dimension")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Target dimension for vector reduction. Must be smaller than source dimension.");

    public static final Option<ReductionMethod> REDUCTION_METHOD =
            Options.key("reduction_method")
                    .enumType(ReductionMethod.class)
                    .defaultValue(ReductionMethod.TRUNCATE)
                    .withDescription("Method for dimension reduction");

    public static final Option<Integer> SOURCE_DIMENSION =
            Options.key("source_dimension")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Source vector dimension. Must be larger than target dimension.");

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Batch size for processing vectors");

    public enum ReductionMethod {
        TRUNCATE,

        RANDOM_PROJECTION,

        SPARSE_RANDOM_PROJECTION
    }
}
