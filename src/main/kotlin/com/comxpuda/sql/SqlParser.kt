package com.comxpuda.sql

import java.sql.SQLException
import java.util.logging.Logger

class SqlParser(val tokens: TokenStream) : PrattParser {

    private val logger = Logger.getLogger(SqlParser::class.simpleName)

    override fun parsePrefix(): SqlExpr? {
        logger.fine("parsePrefix() next token = ${tokens.peek()}")
        val token = tokens.next() ?: return null
        val expr =
            when (token.type) {
                Keyword.SELECT -> parseSelect()
                Keyword.CAST -> parseCast()

                Keyword.MAX -> SqlIdentifier(token.text)
                Keyword.MIN -> SqlIdentifier(token.text)
                Keyword.SUM -> SqlIdentifier(token.text)
                // type
                Keyword.INT -> SqlIdentifier(token.text)
                Keyword.DOUBLE -> SqlIdentifier(token.text)

                // Literals
                Literal.IDENTIFIER -> SqlIdentifier(token.text)
                Literal.STRING -> SqlString(token.text)
                Literal.LONG -> SqlLong(token.text.toLong())
                Literal.DOUBLE -> SqlDouble(token.text.toDouble())
                else -> throw IllegalStateException("Unexpected token $token")
            }
        logger.fine("parsePrefix() returning $expr")
        return expr
    }

    override fun nextPrecedence(): Int {
        val token = tokens.peek() ?: return 0
        val precedence =
            when (token.type) {
                // Keywords
                Keyword.AS, Keyword.ASC, Keyword.DESC -> 10
                Keyword.OR -> 20
                Keyword.AND -> 30

                // Symbols
                Symbol.LT, Symbol.LT_EQ, Symbol.EQ,
                Symbol.BANG_EQ, Symbol.GT_EQ, Symbol.GT -> 40

                Symbol.PLUS, Symbol.SUB -> 50
                Symbol.STAR, Symbol.SLASH -> 60

                Symbol.LEFT_PAREN -> 70
                else -> 0
            }
        logger.fine("nextPrecedence($token) returning $precedence")
        return precedence
    }

    override fun parseInfix(left: SqlExpr, precedence: Int): SqlExpr {
        logger.fine("parseInfix() next token = ${tokens.peek()}")
        val token = tokens.peek()!!
        val expr =
            when (token.type) {
                Symbol.PLUS, Symbol.SUB, Symbol.STAR, Symbol.SLASH,
                Symbol.EQ, Symbol.GT, Symbol.LT -> {
                    tokens.next() // consume the token
                    SqlBinaryExpr(
                        left, token.text, parse(precedence) ?: throw SQLException("Error parsing infix")
                    )
                }

                // keywords
                Keyword.AS -> {
                    tokens.next() // consume the token
                    SqlAlias(left, parseIdentifier())
                }

                Keyword.AND, Keyword.OR -> {
                    tokens.next() // consume the token
                    SqlBinaryExpr(
                        left,
                        token.text,
                        parse(precedence) ?: throw SQLException("Error parsing infix")
                    )
                }

                Keyword.ASC, Keyword.DESC -> {
                    tokens.next()
                    SqlSort(left, token.type == Keyword.ASC)
                }


                Symbol.LEFT_PAREN -> {
                    if (left is SqlIdentifier) {
                        tokens.next() // consume the token
                        val args = parseExprList()
                        assert(tokens.next()?.type == Symbol.RIGHT_PAREN)
                        SqlFunction(left.id, args)
                    } else {
                        throw IllegalStateException("Unexpected LPAREN")
                    }
                }

                else -> throw IllegalStateException("Unexpected infix token $token")
            }
        logger.fine("parseInfix() returning $expr")
        return expr
    }

    private fun parseSelect(): SqlExpr {
        val projection = parseExprList()
        if (tokens.consumeKeyword("FROM")) {
            val table = parseExpr() as SqlIdentifier
            // parse optional WHERE clause
            var filterExpr: SqlExpr? = null
            if (tokens.consumeKeyword("WHERE")) {
                filterExpr = parseExpr()
            }

            // parse optional GROUP BY clause
            var groupBy: List<SqlExpr> = listOf()
            if (tokens.consumeKeywords(listOf("GROUP", "BY"))) {
                groupBy = parseExprList()
            }

            // parse optional HAVING clause
            var havingExpr: SqlExpr? = null
            if (tokens.consumeKeyword("HAVING")) {
                havingExpr = parseExpr()
            }

            // parse optional ORDER BY clause
            var orderBy: List<SqlExpr> = listOf()
            if (tokens.consumeKeywords(listOf("ORDER", "BY"))) {
                orderBy = parseOrder()
            }

            return SqlSelect(projection, filterExpr, groupBy, orderBy, havingExpr, table.id)

        } else {
            throw IllegalStateException("Expected FROM keyword, found ${tokens.peek()}")
        }
    }

    private fun parseOrder(): List<SqlExpr> {
        val sortList = mutableListOf<SqlSort>()
        var sort = parseExpr()
        while (sort != null) {
            sort = when (sort) {
                is SqlIdentifier -> SqlSort(sort, true)
                is SqlSort -> sort
                else -> throw java.lang.IllegalStateException("Unexpected expression $sort after order by.")
            }
            sortList.add(sort)

            if (tokens.peek()?.type == Symbol.COMMA) {
                tokens.next()
            } else {
                break
            }
            sort = parseExpr()
        }
        return sortList
    }

    private fun parseExprList(): List<SqlExpr> {
        logger.fine("parseExprList()")
        val list = mutableListOf<SqlExpr>()
        var expr = parseExpr()
        while (expr != null) {
            // logger.fine("parseExprList parsed $expr")
            list.add(expr)
            if (tokens.peek()?.type == Symbol.COMMA) {
                tokens.next()
            } else {
                break
            }
            expr = parseExpr()
        }
        logger.fine("parseExprList() returning $list")
        return list
    }

    private fun parseExpr() = parse(0)

    private fun parseCast(): SqlExpr {
        assert(tokens.consumeTokenType(Symbol.LEFT_PAREN))
        val expr = parseExpr() ?: throw SQLException()
        val alias = expr as SqlAlias
        assert(tokens.consumeTokenType(Symbol.RIGHT_PAREN))
        return SqlCast(alias.expr, alias.alias)
    }

    private fun parseIdentifier(): SqlIdentifier {
        val expr = parseExpr() ?: throw SQLException("Expected identifier, found EOF")
        return when (expr) {
            is SqlIdentifier -> expr
            else -> throw SQLException("Expected identifier, found $expr")
        }
    }
}