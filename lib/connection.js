'use strict'

const pmConnection = require(`promise-mysql/lib/connection`)
const SqlString = require('mysql/lib/protocol/SqlString.js')

class connection extends pmConnection {

    escapeNamedParams(query, params) {
        if (!params) { return query }
        [{regex: /\:\:(\w+)/g, esc: SqlString.escapeId}, {regex: /\:(\w+)/g, esc: SqlString.escape} ].forEach(function(opt) {
            query = query.replace(opt.regex, function(txt, key) {
                if (params.hasOwnProperty(key)) {
                    return opt.esc(params[key])
                }
                return txt
            })
        })
        return query
    }

    where(where){

        if (typeof where == 'undefined') {
            where = false
        } else if (typeof where == 'string') {
            where = (where.trim().length == 0) ? false : where
        } else if (where.constructor === Array) {
            where = (where.length == 0) ? false : where.join(' and ')
        } else if (where.constructor === Object) {
            where = (Object.keys(where).length == 0) ? false : Object.keys(where).map((v, i, a) => {
                return (v.indexOf('?') >= 0 ? v : v+' = ?').replace('?', SqlString.escape(where[v]))
            }).join(' and ')
        } else {
            where = false
        }

        return where
    }

    queryTrans(stmt, params) {
        const athis = this

        return new Promise(async function(resolve, reject){

            await athis.beginTransaction()

            try {

                const ret = await athis.query(stmt, params)
                await athis.commit()

                resolve(ret)

            } catch(err){

                await athis.rollback()
                reject(err)

            }

        })

    }

    insert(table, fields){
        const tableName = SqlString.escapeId(table)
        const fieldsList = Object.keys(fields).map((v, i, a) => SqlString.escapeId(v)).join(', ')
        const valuesList = Object.keys(fields).map((v, i, a) => '?').join(', ')

        const sql = `insert into ${tableName} (${fieldsList}) values (${valuesList})`
        const params = Object.keys(fields).map((v, i, a) => fields[v])

        return this.query(sql, params)
    }

    update(table, fields, where){
        const tableName = SqlString.escapeId(table)
        where = this.where(where) || false
        if (where === false) throw new Error('where always needed')

        const fieldsList = Object.keys(fields).map((v, i, a) => SqlString.escapeId(v) + ' = ?' ).join(', ')

        const sql = `update ${tableName} set ${fieldsList} where ${where}`
        const params = Object.keys(fields).map((v, i, a) => fields[v])

        return this.query(sql, params)
    }

    insertOrUpdate(table, fields){
        const tableName = SqlString.escapeId(table)
        const fieldsList = Object.keys(fields).map((v, i, a) => SqlString.escapeId(v)).join(', ')
        const valuesList = Object.keys(fields).map((v, i, a) => '?').join(', ')

        const uFieldsList = Object.keys(fields).map((v, i, a) => SqlString.escapeId(v) + ' = ?' ).join(', ')

        const sql = `insert into ${tableName} (${fieldsList}) values (${valuesList}) on duplicate key update ${uFieldsList}`
        const params = [...Object.keys(fields).map((v, i, a) => fields[v]), ...Object.keys(fields).map((v, i, a) => fields[v])]

        return this.query(sql, params)
    }

    insertExpr(table, fields){
        const tableName = SqlString.escapeId(table)

        let fieldsList = []
        let valuesList = []
        let params = []
        Object.keys(fields).map((v, i, a) => {

            fieldsList.push(SqlString.escapeId(v))

            if (typeof fields[v] == 'string' && fields[v].substr(0, 2) == '``') {
                valuesList.push(fields[v].substr(2))
            } else {
                valuesList.push('?')
                params.push(fields[v])
            }

        })

        fieldsList = fieldsList.join(', ')
        valuesList = valuesList.join(', ')

        const sql = `insert into ${tableName} (${fieldsList}) values (${valuesList})`

        return this.query(sql, params)
    }

    updateExpr(table, fields, where){
        const tableName = SqlString.escapeId(table)
        where = this.where(where) || false
        if (where === false) throw new Error('where always needed')

        let params = []
        let fieldsList = Object.keys(fields).map((v, i, a) => {

            if (typeof fields[v] == 'string' && fields[v].substr(0, 2) == '``') {
                return SqlString.escapeId(v) + ' = ' + fields[v].substr(2)
            } else {
                params.push(fields[v])
                return SqlString.escapeId(v) + ' = ?'
            }

        }).join(', ')

        const sql = `update ${tableName} set ${fieldsList} where ${where}`
        return this.query(sql, params)
    }

    insertDML(table, fields){
        const tableName = SqlString.escapeId(table)

        let fieldsList = []
        let valuesList = []
        Object.keys(fields).map((v, i, a) => {

            fieldsList.push(SqlString.escapeId(v))

            if (typeof fields[v] == 'string' && fields[v].substr(0, 2) == '``') {
                valuesList.push(fields[v].substr(2))
            } else {
                if (typeof fields[v] == 'undefined' || fields[v] === null)
                    valuesList.push('null')
                else if (typeof fields[v] == 'number')
                    valuesList.push(`${fields[v]}`)
                else if (typeof fields[v] == 'boolean')
                    valuesList.push(fields[v] ? 'true' : 'false')
                else
                    valuesList.push(SqlString.escape(`${fields[v]}`))
            }

        })

        fieldsList = fieldsList.join(', ')
        valuesList = valuesList.join(', ')

        return `insert into ${tableName} (${fieldsList}) values (${valuesList})`
    }

    updateDML(table, fields, where){
        const tableName = SqlString.escapeId(table)
        where = this.where(where) || false
        if (where === false) throw new Error('where always needed')

        let fieldsList = Object.keys(fields).map((v, i, a) => {

            if (typeof fields[v] == 'string' && fields[v].substr(0, 2) == '``') {
                return SqlString.escapeId(v) + ' = ' + fields[v].substr(2)
            } else {
                return SqlString.escapeId(v) + ' = ' + SqlString.escape(fields[v])
            }

        }).join(', ')

        return `update ${tableName} set ${fieldsList} where ${where}`
    }

    delete(table, where){
        const tableName = SqlString.escapeId(table)
        where = this.where(where) || false
        if (where === false) throw new Error('where always needed')

        const sql = `delete from ${tableName} where ${where}`

        return this.query(sql, [])
    }

    fetch(stmt, params){
        return this.query(stmt, params)
    }

    fetchRow(stmt, params){
        const athis = this

        return new Promise(async function(resolve, reject){

            try {

                let rows = await athis.query(stmt, params)
                if (rows.length == 0) return resolve(false)

                if (Array.isArray(rows[0]))
                    resolve(rows[0].shift())
                else
                    resolve(rows[0])

            } catch(err){
                reject(err)
            }

        });
    }

    fetchOne(stmt, params){
        const athis = this

        return new Promise(async function(resolve, reject){

            try {

                let rows = await athis.query(stmt, params)
                if (rows.length == 0) resolve(false)

                if (Array.isArray(rows[0]))
                    resolve(Object.entries(rows[0].shift()).shift()[1])
                else
                    resolve(Object.entries(rows[0]).shift()[1])

            } catch(err){
                reject(err)
            }

        });

    }

}

module.exports = connection
