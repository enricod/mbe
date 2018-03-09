package it.enricod.mbe.spring.metareader

import java.sql.Connection
import javax.sql.DataSource


data class Tabella(val nome: String,
                   val schema: String,
                   val colonne: List<Colonna>)

data class FK(val tabella1: String,
              val colonna1: String,
              val tabella2: String,
              val colonna2: String)

data class Colonna(val nome: String,
                   val dataType: String,
                   val dimColonna: String?,
                   val decimalDigits: String,
                   val nullable: Boolean,
                   val checkPK: Boolean,
                   val fks: List<FK>)


class MetadataReader(val dataSource: DataSource){

    //private var dbBackend: IDbBackend? = null

    fun nullToEmpty(s: String?) : String {
        if (s == null) return "";
        return s;
    }

    fun updateMetadata(schema: String) {
        val conn = dataSource.connection
        try {

            val databaseMetaData = conn.getMetaData()

            val types = arrayOf("TABLE")
            val tabelleRS = databaseMetaData.getTables(schema,  null, "%", types)

            val tabelle = ArrayList<Tabella>()
            while (tabelleRS.next()) {
                var nomeTabella = tabelleRS.getString("TABLE_NAME")
                tabelle.add(strutturaTabella(conn, schema, nomeTabella))
            }


            //salvaInDatabase(conn, tabelleDominio, tabelle)
            dump(tabelle)
        } finally {
            conn.close()
        }
    }

    private fun dump(tabelle: List<Tabella>) {
        tabelle.forEach { t ->
            println("=======================================")
            println("${t.schema}.${t.nome}")

            t.colonne.forEach { c ->
                println("\t${c}")

                c.fks.forEach { fk ->
                    println("\t\t${fk}")
                }
            }
        }
    }

    fun strutturaTabella(conn: Connection, schema: String, tabellaName: String): Tabella {
        val tabella = Tabella(tabellaName, schema, listOf() )
        val colonne = popolaColonne(conn,  tabella )
        return Tabella(tabellaName, schema, colonne )
    }

    fun popolaColonne(conn: Connection, tabella: Tabella): List<Colonna> {
        return getColsMetadata(conn, tabella)
    }
/*
    @Throws(SQLException::class)
    private fun salvaInDatabase(conn: Connection, tabelleDominio: List<ITabellaDominio>, tabelle: List<Tabella>) {

        val deleteFKSql = "DELETE FROM tabellaDominioColumnFK";

        val stmt = conn.createStatement()
        stmt.executeUpdate(deleteFKSql)



        tabelleDominio.forEach { tabellaDominio ->
            val tabella = tabelle.find { t -> t.nome.equals(tabellaDominio.nome) }
            if (tabella != null) {
                val elencoTabellaDominioColumn = dbBackend!!.elencoTabellaDominioColumn(tabellaDominio.id)

                tabella.colonne.forEach { colonna ->
                    var find: ITabellaDominioColumn? = elencoTabellaDominioColumn.find { col -> col.nome.equals(colonna.nome) }
                    if (find == null) {
                        find = EntityFactory.createEntity(EntityName.tabellaDominioColumn)
                        find.nome = colonna.nome
                    } else {
                    }
                    find!!.tipo = colonna.dataType
                    find.tabellaDiminioId = tabellaDominio.id
                    if (StringUtils.isBlank(find.descrizione)) {
                        find.descrizione = find.nome
                    }
                    if (find.dimColonna == 0) {
                        try {
                            find.dimColonna = Integer.valueOf(colonna.dimColonna)
                        } catch (ex: NumberFormatException) {
                            //ex.printStackTrace()
                        }

                    }
                    find.isCheckNullable = colonna.nullable
                    find.isCheckPK = colonna.checkPK
                    if (colonna.checkPK) {
                        find.isCheckVisibile = false
                    }

                    dbBackend!!.insertOrUpdateTabellaDominioColumn(find)

                    val insertTableSQL = "INSERT INTO tabellaDominioColumnFK" +
                            "( tabellaDominioColumnId, tabella, colonna)" +
                            "VALUES" +
                            "(?, ?, ?)"
                    val preparedStatement = conn.prepareStatement(insertTableSQL)
                    colonna.fks.forEach { fk ->
                        preparedStatement.setString(1, find.id);
                        preparedStatement.setString(2, fk.tabella2);
                        preparedStatement.setString(3, fk.colonna2);
                        preparedStatement.execute()
                    }

                }
            }
        }
        /*
        val colsMetadata = getColsMetadata(conn, Tabella(iTabellaDominio.nome, "ilex", Lists.newArrayList()))

        */
    }

    */

    /*
    -7 	BIT
    -6 	TINYINT
    -5 	BIGINT
    -4 	LONGVARBINARY
    -3 	VARBINARY
    -2 	BINARY
    -1 	LONGVARCHAR
    0 	NULL
    1 	CHAR
    2 	NUMERIC
    3 	DECIMAL
    4 	INTEGER
    5 	SMALLINT
    6 	FLOAT
    7 	REAL
    8 	DOUBLE
    12 	VARCHAR
    91 	DATE
    92 	TIME
    93 	TIMESTAMP
    1111  	OTHER
     */

    fun toSqlType(s: String): String = when (s) {
        "-5" -> "java.lang.Long"
        "3" -> "java.lang.Double"
        "-6" -> "java.lang.Boolean"
        "-7" -> "java.lang.Boolean"
        "1" -> "java.lang.String"
        "4" -> "java.lang.Integer"
        "12" -> "java.lang.String"
        "91" -> "java.lang.Date"
        "92" -> "java.lang.Date"
        "93" -> "java.lang.Timestamp"
        else -> s
    }

    fun getColsMetadata(conn: Connection, tabella: Tabella) : List<Colonna>{
        val databaseMetaData = conn.getMetaData()


        val catalog: String? = null
        val schemaPattern = tabella.schema
        val tableNamePattern = tabella.nome
        val columnNamePattern = "%"

        val columns = databaseMetaData.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern)

        val primaryKeys = databaseMetaData.getPrimaryKeys("", tabella.schema, tabella.nome)

        val pks = mutableListOf<String>()
        while (primaryKeys.next()) {
            val columnName = primaryKeys.getString("COLUMN_NAME")
            pks.add(columnName)
        }

//        println("-------------------------------------")
//        println("ilex.${tabella.nome}")
        val fks = mutableListOf<FK>()
        val rs = databaseMetaData.getImportedKeys(null, tabella.schema, tabella.nome)
        //val rs = databaseMetaData.getImportedKeys(null, tabella.schema, tabella.nome)
        while (rs.next()) {
            val pkey = rs.getString("PKCOLUMN_NAME")
            val ptab = rs.getString("PKTABLE_NAME")
            val fkey = rs.getString("FKCOLUMN_NAME")
            val ftab = rs.getString("FKTABLE_NAME")

//            println ("~~~~~~~~~~~~~ ${tabella.nome} ")
//            println("primary key table = " + ptab)
//            println("primary key = " + pkey)
//            println("foreign key table = " + ftab)
//            println("foreign key = " + fkey)
            fks.add(FK(ftab, fkey, ptab, pkey))
        }

        val cols = mutableListOf<Colonna>()

        while (columns.next()) {

            val colname = columns.getString("COLUMN_NAME")
            val colonna = Colonna(
                    colname,
                    toSqlType(columns.getString("DATA_TYPE")),
                    nullToEmpty(columns.getString("COLUMN_SIZE")),
                    nullToEmpty(columns.getString("DECIMAL_DIGITS")),
                    "YES".equals(nullToEmpty(columns.getString("IS_NULLABLE"))),
                    pks.contains(colname),
                    fks.filter { it.colonna1.equals(colname) && it.tabella1.equals(tabella.nome) }
            )
//          val is_autoIncrment = columns.getString("IS_AUTOINCREMENT")
            //Printing results
//            println(colonna)
            cols.add(colonna)

        }

        return cols.toList()
    }
}
