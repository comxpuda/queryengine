package com.comxpuda.datatypes

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.*
import org.apache.arrow.vector.types.pojo.ArrowType

object FieldVectorFactory {

    fun create(arrowType: ArrowType, initialCapacity: Int): FieldVector {
        val rootAllocator = RootAllocator(Long.MAX_VALUE)
        val fieldVector: FieldVector =
            when (arrowType) {
                ArrowTypes.BooleanType -> BitVector("v", rootAllocator)
                ArrowTypes.Int8Type -> TinyIntVector("v", rootAllocator)
                ArrowTypes.Int16Type -> SmallIntVector("v", rootAllocator)
                ArrowTypes.Int32Type -> IntVector("v", rootAllocator)
                ArrowTypes.Int64Type -> BigIntVector("v", rootAllocator)
                ArrowTypes.FloatType -> Float4Vector("v", rootAllocator)
                ArrowTypes.DoubleType -> Float8Vector("v", rootAllocator)
                ArrowTypes.StringType -> VarCharVector("v", rootAllocator)
                else -> throw java.lang.IllegalStateException()
            }
        if (initialCapacity != 0) {
            fieldVector.setInitialCapacity(initialCapacity)
        }
        fieldVector.allocateNew()
        return fieldVector
    }
}

class ArrowFieldVector(val field: FieldVector) : ColumnVector {
    override fun getType(): ArrowType {
        return when (field) {
            is BitVector -> ArrowTypes.BooleanType
            is TinyIntVector -> ArrowTypes.Int8Type
            is SmallIntVector -> ArrowTypes.Int16Type
            is IntVector -> ArrowTypes.Int32Type
            is BigIntVector -> ArrowTypes.Int64Type
            is Float4Vector -> ArrowTypes.FloatType
            is Float8Vector -> ArrowTypes.DoubleType
            is VarCharVector -> ArrowTypes.StringType
            else -> throw IllegalStateException()
        }
    }

    override fun getValue(i: Int): Any? {
        if (field.isNull(i)) {
            return null
        }

        return when (field) {
            is BitVector -> if (field.get(i) == 1) true else false
            is TinyIntVector -> field.get(i)
            is SmallIntVector -> field.get(i)
            is IntVector -> field.get(i)
            is BigIntVector -> field.get(i)
            is Float4Vector -> field.get(i)
            is Float8Vector -> field.get(i)
            is VarCharVector -> {
                val bytes = field.get(i)
                if (bytes == null) {
                    null
                } else {
                    String(bytes)
                }
            }

            is VarBinaryVector -> {
                val bytes = field.get(i)
                if (bytes == null) {
                    null
                } else {
                    String(bytes)
                }
            }

            else -> throw IllegalStateException()
        }
    }

    override fun size(): Int {
        return field.valueCount
    }
}