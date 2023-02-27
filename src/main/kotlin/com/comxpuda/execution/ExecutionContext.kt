package com.comxpuda.execution

import com.comxpuda.datasource.CsvDataSource
import com.comxpuda.datasource.ParquetDataSource
import com.comxpuda.logicalplan.DataFrame
import com.comxpuda.logicalplan.DataFrameImpl
import com.comxpuda.logicalplan.Scan

class ExecutionContext {

    fun csv(filename: String): DataFrame {
        return DataFrameImpl(Scan(filename, CsvDataSource(filename, null, true, 1024), listOf()))
    }

    fun parquet(filename: String): DataFrame {
        return DataFrameImpl(Scan(filename, ParquetDataSource(filename), listOf()))
    }

}