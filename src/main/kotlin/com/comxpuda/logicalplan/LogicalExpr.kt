package com.comxpuda.logicalplan

import com.comxpuda.datatypes.Field

interface LogicalExpr {

    fun toFiled(input: LogicalPlan): Field

}