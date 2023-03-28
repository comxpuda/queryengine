package com.comxpuda.execution

import com.comxpuda.datasource.ParquetDataSource
import org.junit.Ignore
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import java.io.File
import kotlin.system.measureTimeMillis
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Ignore
class ExecutionTest {

    @Test
    fun `read parquet file`() {
        val dir = "testdata/nyctaxi"
        val parquet = ParquetDataSource(File(dir, "yellow_tripdata_2022-01.parquet").absolutePath)

        val it = parquet.scan(listOf("VendorID")).iterator()
        assertTrue(it.hasNext())

        val batch = it.next()

        val id = batch.field(0)
        val values = (0..id.size()).map { id.getValue(it) ?: "null" }
        values.forEach { println(it) }
    }

    @Test
    fun `query taxi`() {
        val ctx = ExecutionContext(mapOf())

        val time =
            measureTimeMillis {
                val taxiDf = ctx.parquet("testdata/nyctaxi/yellow_tripdata_2022-01.parquet")
                ctx.register("taxi", taxiDf)

//                println("Logical Plan:\t${format(taxiDf.logicalPlan())}")
//
//                val optimizedPlan = Optimizer().optimize(taxiDf.logicalPlan())
//                println("Optimized Plan:\t${format(optimizedPlan)}")

//                val results = ctx.execute(optimizedPlan)
                val results = ctx.execute(
                    ctx.sql("SELECT VendorID,passenger_count,trip_distance,PULocationID,DOLocationID,payment_type,total_amount FROM test;")
//                    ctx.sql("SELECT SUM(total_amount) FROM taxi;") // todo ignore case of agg func
                )

                results.forEach {
                    println(it.schema)
                    println(it.toCSV())
                }
            }

        println("Query took $time ms")
    }
}

//sun 1024 record
//Schema(fields=[Field(name=SUM, dataType=FloatingPoint(DOUBLE))])
//20100.719999999823
//
//Query took 2179 ms
