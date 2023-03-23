package com.comxpuda.sql

interface SqlExpr

/** Simple SQL identifier such as a table or column name */
data class SqlIdentifier(val id: String) : SqlExpr {
    override fun toString(): String {
        return id
    }
}

/** Binary expression */
data class SqlBinaryExpr(val l: SqlExpr, val op: String, val r: SqlExpr) : SqlExpr {
    override fun toString(): String = "$l $op $r"
}

/** SQL literal string */
data class SqlString(val value: String) : SqlExpr {
    override fun toString() = "'$value'"
}

/** SQL literal long */
data class SqlLong(val value: Long) : SqlExpr {
    override fun toString() = "$value"
}

/** SQL literal double */
data class SqlDouble(val value: Double) : SqlExpr {
    override fun toString() = "$value"
}

/** SQL function call */
data class SqlFunction(val id: String, val args: List<SqlExpr>) : SqlExpr {
    override fun toString() = id
}

/** SQL aliased expression */
data class SqlAlias(val expr: SqlExpr, val alias: SqlIdentifier) : SqlExpr

data class SqlCast(val expr: SqlExpr, val dataType: SqlIdentifier) : SqlExpr

data class SqlSort(val expr: SqlExpr, val asc: Boolean) : SqlExpr

interface SqlRelation : SqlExpr

data class SqlSelect(
    val projection: List<SqlExpr>,
    val selection: SqlExpr?,
    val groupBy: List<SqlExpr>,
    val orderBy: List<SqlExpr>,
    val having: SqlExpr?,
    val tableName: String
) : SqlRelation