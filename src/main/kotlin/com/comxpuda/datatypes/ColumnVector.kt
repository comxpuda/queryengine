package com.comxpuda.datatypes

import org.apache.arrow.vector.types.pojo.ArrowType

interface ColumnVector {

    fun getType(): ArrowType
    fun getValue(i: Int): Any?
    fun size(): Int

}