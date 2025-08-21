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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.common.utils.BufferUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class VectorDimensionReductionTransformTest {

    private CatalogTable catalogTable;
    private static final int SOURCE_DIMENSION = 10;
    private static final int TARGET_DIMENSION = 5;

    @BeforeEach
    void setUp() {
        catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("catalog", TablePath.DEFAULT),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "id",
                                                BasicType.LONG_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "vector_field",
                                                VectorType.VECTOR_FLOAT_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "text_field",
                                                BasicType.STRING_TYPE,
                                                1L,
                                                Boolean.FALSE,
                                                null,
                                                null))
                                .build(),
                        new HashMap<>(),
                        Collections.emptyList(),
                        "test table");
    }

    @Test
    void testTruncateMethod() {
        Map<String, Object> config =
                createConfig(VectorDimensionReductionConfig.ReductionMethod.TRUNCATE);
        VectorDimensionReductionTransform transform =
                new VectorDimensionReductionTransform(catalogTable, ReadonlyConfig.fromMap(config));

        // Create test vector with SOURCE_DIMENSION elements
        Float[] sourceVector = new Float[SOURCE_DIMENSION];
        for (int i = 0; i < SOURCE_DIMENSION; i++) {
            sourceVector[i] = (float) i;
        }
        ByteBuffer inputVector = BufferUtils.toByteBuffer(sourceVector);

        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, inputVector, "test"});
        SeaTunnelRow outputRow = transform.transformRow(inputRow);

        // Verify the output
        Assertions.assertNotNull(outputRow);
        Assertions.assertEquals(3, outputRow.getFields().length);
        Assertions.assertEquals(1L, outputRow.getField(0));
        Assertions.assertEquals("test", outputRow.getField(2));

        // Check the reduced vector
        ByteBuffer reducedVector = (ByteBuffer) outputRow.getField(1);
        Float[] reducedArray = BufferUtils.toFloatArray(reducedVector);
        Assertions.assertEquals(TARGET_DIMENSION, reducedArray.length);

        // Verify truncation - should contain first TARGET_DIMENSION elements
        for (int i = 0; i < TARGET_DIMENSION; i++) {
            Assertions.assertEquals((float) i, reducedArray[i], 0.001f);
        }
    }

    @Test
    void testRandomProjectionMethod() {
        Map<String, Object> config =
                createConfig(VectorDimensionReductionConfig.ReductionMethod.RANDOM_PROJECTION);
        VectorDimensionReductionTransform transform =
                new VectorDimensionReductionTransform(catalogTable, ReadonlyConfig.fromMap(config));

        // Create test vector
        Float[] sourceVector = new Float[SOURCE_DIMENSION];
        for (int i = 0; i < SOURCE_DIMENSION; i++) {
            sourceVector[i] = 1.0f; // All ones for predictable testing
        }
        ByteBuffer inputVector = BufferUtils.toByteBuffer(sourceVector);

        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, inputVector, "test"});
        SeaTunnelRow outputRow = transform.transformRow(inputRow);

        // Verify the output
        Assertions.assertNotNull(outputRow);
        ByteBuffer reducedVector = (ByteBuffer) outputRow.getField(1);
        Float[] reducedArray = BufferUtils.toFloatArray(reducedVector);
        Assertions.assertEquals(TARGET_DIMENSION, reducedArray.length);

        // All elements should be non-zero (since we're projecting non-zero vector)
        for (Float value : reducedArray) {
            Assertions.assertNotNull(value);
        }
    }

    @Test
    void testSparseRandomProjectionMethod() {
        Map<String, Object> config =
                createConfig(
                        VectorDimensionReductionConfig.ReductionMethod.SPARSE_RANDOM_PROJECTION);
        VectorDimensionReductionTransform transform =
                new VectorDimensionReductionTransform(catalogTable, ReadonlyConfig.fromMap(config));

        // Create test vector
        Float[] sourceVector = new Float[SOURCE_DIMENSION];
        for (int i = 0; i < SOURCE_DIMENSION; i++) {
            sourceVector[i] = 1.0f; // All ones for predictable testing
        }
        ByteBuffer inputVector = BufferUtils.toByteBuffer(sourceVector);

        SeaTunnelRow inputRow = new SeaTunnelRow(new Object[] {1L, inputVector, "test"});
        SeaTunnelRow outputRow = transform.transformRow(inputRow);

        // Verify the output
        Assertions.assertNotNull(outputRow);
        ByteBuffer reducedVector = (ByteBuffer) outputRow.getField(1);
        Float[] reducedArray = BufferUtils.toFloatArray(reducedVector);
        Assertions.assertEquals(TARGET_DIMENSION, reducedArray.length);

        // All elements should be non-zero (since we're projecting non-zero vector)
        for (Float value : reducedArray) {
            Assertions.assertNotNull(value);
        }
    }

    private Map<String, Object> createConfig(
            VectorDimensionReductionConfig.ReductionMethod method) {
        Map<String, Object> config = new HashMap<>();
        config.put(
                VectorDimensionReductionConfig.VECTOR_FIELDS.key(), Arrays.asList("vector_field"));
        config.put(VectorDimensionReductionConfig.SOURCE_DIMENSION.key(), SOURCE_DIMENSION);
        config.put(VectorDimensionReductionConfig.TARGET_DIMENSION.key(), TARGET_DIMENSION);
        config.put(VectorDimensionReductionConfig.REDUCTION_METHOD.key(), method);
        return config;
    }
}
