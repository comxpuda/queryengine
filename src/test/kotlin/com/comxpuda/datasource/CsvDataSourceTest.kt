package com.comxpuda.datasource

import org.junit.Test
import org.junit.jupiter.api.TestInstance
import java.io.File

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CsvDataSourceTest {

    val dir = "testdata"

    @Test
    fun `read csv with no projection`() {
        val csv = CsvDataSource(File(dir, "employee.csv").absolutePath, null, true, 1024)

        val headers = listOf("id", "first_name", "last_name", "state", "job_title", "salary")
        val result = csv.scan(listOf())

        result.asSequence().forEach {
            val field = it.field(0)
            assert(field.size() == 4)

            assert(it.schema.fields.size == headers.size)
            assert(it.schema.fields.map { h -> h.name }.containsAll(headers))
        }
    }

    @Test
    fun `read csv with projection`() {
        val csv = CsvDataSource(File(dir, "employee.csv").absolutePath, null, true, 1024)

        val headers = listOf("first_name", "last_name", "state", "job_title", "salary")
        val result = csv.scan(headers)

        result.asSequence().forEach {
            val field = it.field(0)
            assert(field.size() == 4)

            assert(it.schema.fields.size == headers.size)
            assert(it.schema.fields.map { h -> h.name }.containsAll(headers))
        } 
    }

    @Test
    fun `read csv with first single projection`() {
        val csv = CsvDataSource(File(dir, "employee.csv").absolutePath, null, true, 1024)

        val headers = listOf("id")
        val result = csv.scan(headers)

        result.asSequence().forEach {
            val field = it.field(0)
            assert(field.size() == 4)

            assert(it.schema.fields.size == headers.size)
            assert(it.schema.fields.map { h -> h.name }.containsAll(headers))
        }
    }

    @Test
    fun `read csv with middle single projection`() {
        val csv = CsvDataSource(File(dir, "employee.csv").absolutePath, null, true, 1024)

        val headers = listOf("state")
        val result = csv.scan(headers)

        result.asSequence().forEach {
            val field = it.field(0)
            assert(field.size() == 4)

            assert(it.schema.fields.size == headers.size)
            assert(it.schema.fields.map { h -> h.name }.containsAll(headers))
        }
    }

    @Test
    fun `read csv with small batch`() {
        val csv = CsvDataSource(File(dir, "employee.csv").absolutePath, null, true, 1)
        val result = csv.scan(listOf()).asSequence().toList()

        assert(result.size == 4)

        result.forEach {
            val field = it.field(0)
            assert(field.size() == 1)
        }
    }

    @Test
    fun `read csv with no header`() {
        val csv = CsvDataSource(File(dir, "employee_no_header.csv").absolutePath, null, false, 1024)
        val result = csv.scan(listOf())
        val headers = listOf("field_1", "field_2", "field_3", "field_4", "field_5", "field_6")
        result.asSequence().forEach {
            val field = it.field(0)
            assert(field.size() == 4)
        }
    }

    @Test
    fun `read csv with projections and no header`() {
        val csv = CsvDataSource(File(dir, "employee_no_header.csv").absolutePath, null, false, 1024)
        val headers = listOf("field_1", "field_3", "field_5")
        val result = csv.scan(headers)
        result.asSequence().forEach {
            val field = it.field(0)
            assert(field.size() == 4)
        }
    }

    @Test
    fun `read tsv with no projection`() {
        val csv = CsvDataSource(File(dir, "employee.tsv").absolutePath, null, true, 1024)

        val headers = listOf("id", "first_name", "last_name", "state", "job_title", "salary")
        val result = csv.scan(listOf())

        result.asSequence().forEach {
            val field = it.field(0)
            assert(field.size() == 3)

            assert(it.schema.fields.size == headers.size)
            assert(it.schema.fields.map { h -> h.name }.containsAll(headers))
        }
    }

    @Test
    fun `read tsv with projection`() {
        val csv = CsvDataSource(File(dir, "employee.tsv").absolutePath, null, true, 1024)

        val headers = listOf("first_name", "last_name", "state", "job_title", "salary")
        val result = csv.scan(headers)

        result.asSequence().forEach {
            val field = it.field(0)
            assert(field.size() == 3)

            assert(it.schema.fields.size == headers.size)
            assert(it.schema.fields.map { h -> h.name }.containsAll(headers))
        }
    }

    @Test
    fun `read tsv with first single projection`() {
        val csv = CsvDataSource(File(dir, "employee.tsv").absolutePath, null, true, 1024)

        val headers = listOf("id")
        val result = csv.scan(headers)

        result.asSequence().forEach {
            val field = it.field(0)
            assert(field.size() == 3)

            assert(it.schema.fields.size == headers.size)
            assert(it.schema.fields.map { h -> h.name }.containsAll(headers))
        }
    }

    @Test
    fun `read tsv with middle single projection`() {
        val csv = CsvDataSource(File(dir, "employee.tsv").absolutePath, null, true, 1024)

        val headers = listOf("state")
        val result = csv.scan(headers)

        result.asSequence().forEach {
            val field = it.field(0)
            assert(field.size() == 3)

            assert(it.schema.fields.size == headers.size)
            assert(it.schema.fields.map { h -> h.name }.containsAll(headers))
        }
    }

    @Test
    fun `read tsv with small batch`() {
        val csv = CsvDataSource(File(dir, "employee.tsv").absolutePath, null, true, 1)
        val result = csv.scan(listOf()).asSequence().toList()

        assert(result.size == 3)

        result.forEach {
            val field = it.field(0)
            assert(field.size() == 1)
        }
    }

    @Test
    fun `read tsv with no header`() {
        val csv = CsvDataSource(File(dir, "employee_no_header.tsv").absolutePath, null, false, 1024)
        val result = csv.scan(listOf())
        val headers = listOf("field_1", "field_2", "field_3", "field_4", "field_5", "field_6")
        result.asSequence().forEach {
            val field = it.field(0)
            assert(field.size() == 3)
        }
    }

    @Test
    fun `read tsv with projections and no header`() {
        val csv = CsvDataSource(File(dir, "employee_no_header.tsv").absolutePath, null, false, 1024)
        val headers = listOf("field_2", "field_4", "field_6")
        val result = csv.scan(headers)
        result.asSequence().forEach {
            val field = it.field(0)
            assert(field.size() == 3)
        }
    }

}