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
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.common.utils.BufferUtils;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportMapTransform;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

@Slf4j
public class VectorDimensionReductionTransform extends AbstractCatalogSupportMapTransform {

    public static final String PLUGIN_NAME = "VectorDimensionReduction";

    private final List<String> vectorFields;
    private final int targetDimension;
    private final VectorDimensionReductionConfig.ReductionMethod reductionMethod;

    private float[][] projectionMatrix;
    private final Random random = new Random(42);

    public VectorDimensionReductionTransform(
            CatalogTable inputCatalogTable, @NonNull ReadonlyConfig config) {
        super(inputCatalogTable);
        this.vectorFields = config.get(VectorDimensionReductionConfig.VECTOR_FIELDS);
        int sourceDimension = config.get(VectorDimensionReductionConfig.SOURCE_DIMENSION);
        this.targetDimension = config.get(VectorDimensionReductionConfig.TARGET_DIMENSION);
        this.reductionMethod = config.get(VectorDimensionReductionConfig.REDUCTION_METHOD);

        if (targetDimension >= sourceDimension) {
            throw new IllegalArgumentException(
                    String.format(
                            "Target dimension (%d) must be smaller than source dimension (%d)",
                            targetDimension, sourceDimension));
        }
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    protected TableSchema transformTableSchema() {
        validateVectorFields();
        return inputCatalogTable.getTableSchema();
    }

    private void validateVectorFields() {
        TableSchema inputSchema = inputCatalogTable.getTableSchema();

        for (String vectorField : vectorFields) {
            Column column = findColumnByName(inputSchema, vectorField);
            if (column == null) {
                throw new IllegalArgumentException("Vector field not found: " + vectorField);
            }

            if (!(column.getDataType() instanceof VectorType)) {
                throw new IllegalArgumentException("Field is not a vector type: " + vectorField);
            }
        }
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId().copy();
    }

    private Column findColumnByName(TableSchema schema, String columnName) {
        return schema.getColumns().stream()
                .filter(column -> column.getName().equals(columnName))
                .findFirst()
                .orElse(null);
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        Object[] originalFields = inputRow.getFields();
        Object[] newFields = new Object[originalFields.length];
        System.arraycopy(originalFields, 0, newFields, 0, originalFields.length);

        TableSchema inputSchema = inputCatalogTable.getTableSchema();

        for (String vectorField : vectorFields) {
            int fieldIndex = findFieldIndex(inputSchema, vectorField);
            if (fieldIndex == -1) {
                throw new IllegalArgumentException("Vector field not found: " + vectorField);
            }

            Object inputVector = originalFields[fieldIndex];
            if (inputVector == null) {
                newFields[fieldIndex] = null;
            } else {
                ByteBuffer reducedVector = reduceVectorDimension(inputVector);
                newFields[fieldIndex] = reducedVector;
            }
        }

        return new SeaTunnelRow(newFields);
    }

    private int findFieldIndex(TableSchema schema, String fieldName) {
        List<Column> columns = schema.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equals(fieldName)) {
                return i;
            }
        }
        return -1;
    }

    private ByteBuffer reduceVectorDimension(Object inputVector) {
        Float[] sourceVector = extractFloatArray(inputVector);

        Float[] reducedVector = applyDimensionReduction(sourceVector);
        Float[] reducedVectorBoxed = new Float[reducedVector.length];
        System.arraycopy(reducedVector, 0, reducedVectorBoxed, 0, reducedVector.length);

        return BufferUtils.toByteBuffer(reducedVectorBoxed);
    }

    private Float[] extractFloatArray(Object vectorData) {
        if (vectorData instanceof ByteBuffer) {
            return BufferUtils.toFloatArray((ByteBuffer) vectorData);
        } else if (vectorData instanceof Float[]) {
            return (Float[]) vectorData;
        } else if (vectorData instanceof List) {
            @SuppressWarnings("unchecked")
            List<Number> list = (List<Number>) vectorData;
            Float[] array = new Float[list.size()];
            for (int i = 0; i < list.size(); i++) {
                array[i] = list.get(i).floatValue();
            }
            return array;
        } else {
            throw new IllegalArgumentException(
                    "Unsupported vector data type: " + vectorData.getClass());
        }
    }

    private Float[] applyDimensionReduction(Float[] sourceVector) {
        switch (reductionMethod) {
            case TRUNCATE:
                return truncateVector(sourceVector);
            case RANDOM_PROJECTION:
            case SPARSE_RANDOM_PROJECTION:
                return applyRandomProjection(sourceVector);
            default:
                throw new IllegalArgumentException("Unknown reduction method: " + reductionMethod);
        }
    }

    private Float[] truncateVector(Float[] sourceVector) {
        if (sourceVector.length <= targetDimension) {
            return sourceVector;
        }

        Float[] result = new Float[targetDimension];
        System.arraycopy(sourceVector, 0, result, 0, targetDimension);
        return result;
    }

    private Float[] applyRandomProjection(Float[] sourceVector) {
        if (projectionMatrix == null) {
            initializeProjectionMatrix(sourceVector.length);
        }

        Float[] result = new Float[targetDimension];
        for (int i = 0; i < targetDimension; i++) {
            float sum = 0.0f;
            for (int j = 0; j < sourceVector.length; j++) {
                if (projectionMatrix[i][j] != 0) { // Optimization for sparse matrix
                    sum += sourceVector[j] * projectionMatrix[i][j];
                }
            }
            result[i] = sum;
        }
        return result;
    }

    private void initializeProjectionMatrix(int sourceDimension) {
        switch (reductionMethod) {
            case RANDOM_PROJECTION:
                initializeGaussianProjectionMatrix(sourceDimension);
                break;
            case SPARSE_RANDOM_PROJECTION:
                initializeSparseProjectionMatrix(sourceDimension);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + reductionMethod);
        }
    }

    private void initializeGaussianProjectionMatrix(int sourceDimension) {
        projectionMatrix = new float[targetDimension][sourceDimension];
        float scale = (float) Math.sqrt(1.0 / targetDimension);

        for (int i = 0; i < targetDimension; i++) {
            for (int j = 0; j < sourceDimension; j++) {
                projectionMatrix[i][j] = (float) random.nextGaussian() * scale;
            }
        }
    }

    private void initializeSparseProjectionMatrix(int sourceDimension) {
        projectionMatrix = new float[targetDimension][sourceDimension];
        float scale = (float) Math.sqrt(3.0);
        double p1 = 1.0 / 6.0;
        double p2 = 2.0 / 6.0;

        for (int i = 0; i < targetDimension; i++) {
            for (int j = 0; j < sourceDimension; j++) {
                double rand = random.nextDouble();
                if (rand < p1) {
                    projectionMatrix[i][j] = scale;
                } else if (rand < p2) {
                    projectionMatrix[i][j] = -scale;
                } else {
                    projectionMatrix[i][j] = 0;
                }
            }
        }
    }
}
