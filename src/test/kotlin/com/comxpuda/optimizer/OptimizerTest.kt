package com.comxpuda.optimizer

import com.comxpuda.datasource.CsvDataSource
import com.comxpuda.logicalplan.*
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OptimizerTest {

    @Test
    fun `projection push down`() {
        val df = csv().project(listOf(col("id"), col("first_name"), col("last_name")))
        val rule = ProjectionPushDownRule()
        val optimizedPlan = rule.optimize(df.logicalPlan())

        val expected =
            "Projection: #id, #first_name, #last_name\n" +
                    "\tScan: employee; projection=[first_name, id, last_name]\n"

        assertEquals(expected, optimizedPlan.pretty())
    }

    @Test
    fun `projection push down with selection`() {

        val df =
            csv()
                .filter(col("state") eq lit("CO"))
                .project(listOf(col("id"), col("first_name"), col("last_name")))

        println(df.logicalPlan().pretty());

        val rule = ProjectionPushDownRule()
        val optimizedPlan = rule.optimize(df.logicalPlan())
        println(optimizedPlan.pretty());

        val expected =
            "Projection: #id, #first_name, #last_name\n" +
                    "\tSelection: #state = 'CO'\n" +
                    "\t\tScan: employee; projection=[first_name, id, last_name, state]\n"

        assertEquals(expected, optimizedPlan.pretty())
    }

    @Test
    fun `projection push down with  aggregate query`() {

        val df =
            csv()
                .aggregate(
                    listOf(col("state")),
                    listOf(Min(col("salary")), Max(col("salary")), Count(col("salary"))))

        val rule = ProjectionPushDownRule()
        val optimizedPlan = rule.optimize(df.logicalPlan())

        assertEquals(
            "Aggregate: groupExpr=[#state], aggregateExpr=[MIN(#salary), MAX(#salary), COUNT(#salary)]\n" +
                    "\tScan: employee; projection=[salary, state]\n",
            format(optimizedPlan))
    }

    private fun csv(): DataFrame {
        val employeeCsv = "testdata/employee.csv"
        return DataFrameImpl(Scan("employee", CsvDataSource(employeeCsv, null, true, 1024), listOf()))
    }
}