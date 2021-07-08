package com.aurora.table.utils;


import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Type;

import java.sql.Timestamp;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @author lj.michale
 * @description
 * @date 2021-07-07
 */
public class KuduTypeUtils {

    public static DataType toFlinkType(Type type, ColumnTypeAttributes typeAttributes) {
        switch (type) {
            case STRING:
                return DataTypes.STRING();
            case FLOAT:
                return DataTypes.FLOAT();
            case INT8:
                return DataTypes.TINYINT();
            case INT16:
                return DataTypes.SMALLINT();
            case INT32:
                return DataTypes.INT();
            case INT64:
                return DataTypes.BIGINT();
            case DOUBLE:
                return DataTypes.DOUBLE();
            case DECIMAL:
                return DataTypes.DECIMAL(typeAttributes.getPrecision(), typeAttributes.getScale());
            case BOOL:
                return DataTypes.BOOLEAN();
            case BINARY:
                return DataTypes.BYTES();
            case UNIXTIME_MICROS:
                return new AtomicDataType(new TimestampType(3), Timestamp.class);

            default:
                throw new IllegalArgumentException("Illegal var type: " + type);
        }
    }

    public static Type toKuduType(DataType dataType) {
        checkNotNull(dataType, "type cannot be null");
        LogicalType logicalType = dataType.getLogicalType();
        return logicalType.accept(new KuduTypeLogicalTypeVisitor(dataType));
    }

    private static class KuduTypeLogicalTypeVisitor extends LogicalTypeDefaultVisitor<Type> {

        private final DataType dataType;

        KuduTypeLogicalTypeVisitor(DataType dataType) {
            this.dataType = dataType;
        }

        @Override
        public Type visit(BooleanType booleanType) {
            return Type.BOOL;
        }

        @Override
        public Type visit(TinyIntType tinyIntType) {
            return Type.INT8;
        }

        @Override
        public Type visit(SmallIntType smallIntType) {
            return Type.INT16;
        }

        @Override
        public Type visit(IntType intType) {
            return Type.INT32;
        }

        @Override
        public Type visit(BigIntType bigIntType) {
            return Type.INT64;
        }

        @Override
        public Type visit(FloatType floatType) {
            return Type.FLOAT;
        }

        @Override
        public Type visit(DoubleType doubleType) {
            return Type.DOUBLE;
        }

        @Override
        public Type visit(DecimalType decimalType) {
            return Type.DECIMAL;
        }

        @Override
        public Type visit(TimestampType timestampType) {
            return Type.UNIXTIME_MICROS;
        }

        @Override
        public Type visit(VarCharType varCharType) {
            return Type.STRING;
        }

        @Override
        public Type visit(VarBinaryType varBinaryType) {
            return Type.BINARY;
        }

        @Override
        protected Type defaultMethod(LogicalType logicalType) {
            throw new UnsupportedOperationException(
                    String.format("Flink doesn't support converting type %s to Kudu type yet.", dataType.toString()));
        }

    }
}