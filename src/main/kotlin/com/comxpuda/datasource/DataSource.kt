package com.comxpuda.datasource

import com.comxpuda.datatypes.RecordBatch
import com.comxpuda.datatypes.Schema

interface DataSource {

    fun schema(): Schema

    fun scan(projection: List<String>): Sequence<RecordBatch>

}