package com.comxpuda.sql

/** Pratt Top Down Operator Precedence Parser. See https://tdop.github.io/ for paper. */
interface PrattParser {

    fun parse(precedence: Int = 0): SqlExpr? {
        var expr = parsePrefix() ?: return null
        while (precedence < nextPrecedence()) {
            expr = parseInfix(expr, nextPrecedence())
        }
        return expr
    }

    /** Parse the next prefix expression */
    fun parsePrefix(): SqlExpr?

    /** Get the precedence of the next token */
    fun nextPrecedence(): Int

    /** Parse the next infix expression */
    fun parseInfix(left: SqlExpr, precedence: Int): SqlExpr

}