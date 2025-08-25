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

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableTransform;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.transform.common.TransformCommonOptions;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.transform.vectordimension.VectorDimensionReductionConfig.BATCH_SIZE;
import static org.apache.seatunnel.transform.vectordimension.VectorDimensionReductionConfig.REDUCTION_METHOD;
import static org.apache.seatunnel.transform.vectordimension.VectorDimensionReductionConfig.SOURCE_DIMENSION;
import static org.apache.seatunnel.transform.vectordimension.VectorDimensionReductionConfig.TARGET_DIMENSION;
import static org.apache.seatunnel.transform.vectordimension.VectorDimensionReductionConfig.VECTOR_FIELDS;

@AutoService(Factory.class)
public class VectorDimensionReductionTransformFactory implements TableTransformFactory {

    @Override
    public String factoryIdentifier() {
        return VectorDimensionReductionTransform.PLUGIN_NAME;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(VECTOR_FIELDS)
                .required(SOURCE_DIMENSION)
                .required(TARGET_DIMENSION)
                .optional(REDUCTION_METHOD)
                .optional(BATCH_SIZE)
                .optional(TransformCommonOptions.MULTI_TABLES)
                .build();
    }

    @Override
    public TableTransform createTransform(TableTransformFactoryContext context) {
        return () -> {
            VectorDimensionReductionTransform transform =
                    new VectorDimensionReductionTransform(
                            context.getCatalogTables().get(0), context.getOptions());
            return transform;
        };
    }
}
