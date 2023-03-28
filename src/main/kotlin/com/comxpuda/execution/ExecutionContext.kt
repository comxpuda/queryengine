package com.comxpuda.execution

import com.comxpuda.datasource.CsvDataSource
import com.comxpuda.datasource.DataSource
import com.comxpuda.datasource.ParquetDataSource
import com.comxpuda.datatypes.RecordBatch
import com.comxpuda.logicalplan.DataFrame
import com.comxpuda.logicalplan.DataFrameImpl
import com.comxpuda.logicalplan.LogicalPlan
import com.comxpuda.logicalplan.Scan
import com.comxpuda.optimizer.Optimizer
import com.comxpuda.queryplanner.QueryPlanner
import com.comxpuda.sql.SqlParser
import com.comxpuda.sql.SqlPlanner
import com.comxpuda.sql.SqlSelect
import com.comxpuda.sql.SqlTokenizer

class ExecutionContext(val settings: Map<String, String>) {

    val batchSize: Int = settings.getOrDefault("ballista.csv.batchSize", "1024").toInt()

    /** Tables registered with this context */
    private val tables = mutableMapOf<String, DataFrame>()

    fun csv(filename: String): DataFrame {
        return DataFrameImpl(Scan(filename, CsvDataSource(filename, null, true, batchSize), listOf()))
    }

    fun parquet(filename: String): DataFrame {
        return DataFrameImpl(Scan(filename, ParquetDataSource(filename), listOf()))
    }

    /** Create a DataFrame for the given SQL Select */
    fun sql(sql: String): DataFrame {
        val tokens = SqlTokenizer(sql).tokenize()
        val ast = SqlParser(tokens).parse() as SqlSelect
        val df = SqlPlanner().createDataFrame(ast, tables)
        return DataFrameImpl(df.logicalPlan())
    }

    /** Register a DataFrame with the context */
    fun register(tablename: String, df: DataFrame) {
        tables[tablename] = df
    }

    /** Register a CSV data source with the context */
    fun registerDataSource(tablename: String, datasource: DataSource) {
        register(tablename, DataFrameImpl(Scan(tablename, datasource, listOf())))
    }

    /** Register a CSV data source with the context */
    fun registerCsv(tablename: String, filename: String) {
        register(tablename, csv(filename))
    }

    /** Execute the logical plan represented by a DataFrame */
    fun execute(df: DataFrame): Sequence<RecordBatch> {
        return execute(df.logicalPlan())
    }

    /** Execute the provided logical plan */
    fun execute(plan: LogicalPlan): Sequence<RecordBatch> {
        val optimizedPlan = Optimizer().optimize(plan)
        val physicalPlan = QueryPlanner().createPhysicalPlan(optimizedPlan)
        return physicalPlan.execute()
    }

}