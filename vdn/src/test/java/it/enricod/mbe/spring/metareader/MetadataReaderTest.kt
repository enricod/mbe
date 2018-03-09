package it.enricod.mbe.spring.metareader

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.jdbc.datasource.DataSourceUtils
import org.springframework.jdbc.datasource.SimpleDriverDataSource
import org.springframework.jdbc.datasource.embedded.DataSourceFactory
import java.sql.Driver
import java.util.*
import javax.sql.DataSource

internal class MetadataReaderTest {

    @Test
    fun test1() {
        val properties = Properties()
        properties.load( this.javaClass.getResourceAsStream("/application.properties"))
        var driver = com.mysql.jdbc.Driver();
        var dataSource = SimpleDriverDataSource(driver, properties.getProperty("spring.datasource.url"), properties.getProperty("spring.datasource.username"), properties.getProperty("spring.datasource.password"))
        var metadataReader =  MetadataReader(dataSource)
        metadataReader.updateMetadata("qaria")
    }
}