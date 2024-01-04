import com.opencsv.bean.CsvBindByName
import com.opencsv.bean.CsvIgnore
import com.opencsv.bean.CsvToBean
import com.opencsv.bean.CsvToBeanBuilder
import com.opencsv.bean.HeaderColumnNameMappingStrategy
import com.opencsv.bean.HeaderColumnNameMappingStrategyBuilder
import com.opencsv.bean.StatefulBeanToCsv
import com.opencsv.bean.StatefulBeanToCsvBuilder
import groovy.sql.Sql
import groovy.transform.Canonical
import org.apache.sshd.client.SshClient
import org.apache.sshd.client.session.ClientSession
import org.apache.sshd.common.keyprovider.FileKeyPairProvider
import org.apache.sshd.sftp.client.SftpClient
import org.apache.sshd.sftp.client.fs.SftpFileSystem
import org.apache.sshd.sftp.client.fs.SftpFileSystemProvider
import org.apache.sshd.sftp.client.impl.DefaultSftpClientFactory



import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.security.KeyPair
import java.sql.ResultSet
import java.time.LocalDateTime
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

@GrabConfig(systemClassLoader = true)
@Grab(group='org.apache.sshd', module='sshd-sftp', version='2.10.0')
@Grab(group='org.slf4j', module='slf4j-nop', version='1.7.32')
@Grab(group='com.oracle.ojdbc', module='ojdbc8', version='19.3.0.0')
@Grab(group='com.opencsv', module='opencsv', version='5.9')

def aidy = args[0]
def runTimestamp = LocalDateTime.now()
def indexFile = Paths.get(System.properties.'user.dir','index.txt')
def zipFile = Paths.get(System.properties.'user.dir','dip_award_letters_' + runTimestamp.format('yyyyMMddHHmmss') + '.zip' )
def letters = []
Sql.withInstance (dbProps()) { Sql sql ->

    letters = findLetters(sql,aidy)

    createIndexFile(indexFile,letters)

    zipLetters(zipFile,indexFile,letters)

    // SFTP zip file to Slate
    sftp(sftpProps()) { SftpFileSystem sftpFs ->
        Files.copy(zipFile,sftpFs.getPath('incoming','financial_aid', zipFile.getFileName().toString()), StandardCopyOption.REPLACE_EXISTING)
    }

    // Batch update all award letter records to set Slate upload indicator and insert tracking
    sql.withTransaction {
        batchUpdateSlateUploadInd(sql,letters,runTimestamp)
        batchInsertSlateUploadTrackingRecord(sql,letters,runTimestamp)
    }

    // Remove Index and Zip file
    Files.deleteIfExists(indexFile)
    Files.deleteIfExists(zipFile)

}

/**
 * Find award letters for aid year and parse result set in List of Letters
 * @param sql
 * @param aidy
 * @return
 */
def findLetters(Sql sql, String aidy) {
    List letters = []
    def stmt =
            """
            select xapp.syrxapp_erx_appidcode application_id
                  ,f.ryrawrf_pidm pidm
                  ,i.spriden_id banner_id
                  ,f.ryrawrf_aidy_code aidy_code
                  ,f.ryrawrf_ltrc ltrc
                  ,f.ryrawrf_file letter
                  ,decode((select count(*) 
                             from ryrawrfst 
                            where ryrawrfst_pidm = f.ryrawrf_pidm
                              and ryrawrfst_aidy_code = f.ryrawrf_aidy_code
                              and ryrawrfst_ltrc = f.ryrawrf_ltrc),0,'Financial Aid Package','Updated Financial Aid Package') material_type
                  ,'Award_Package_' || f.ryrawrf_aidy_code || '_' || i.spriden_id || '.pdf' file_name
              from syrxapp xapp, saradap ap, spriden i, ryrawrf f, stvterm tv
             where xapp.syrxapp_erx_application_id like 'UU%'
               and ap.saradap_pidm = xapp.syrxapp_pidm
               and ap.saradap_appl_no = xapp.syrxapp_saradap_appl_no    
               and ap.saradap_appl_no = (select max(ap2.saradap_appl_no) from saradap ap2
                                          where ap2.saradap_pidm = ap.saradap_pidm)
               and i.spriden_pidm = xapp.syrxapp_pidm   
               and i.spriden_change_ind is null     
               and f.ryrawrf_pidm = xapp.syrxapp_pidm
               and f.ryrawrf_aidy_code = ?
               and f.ryrawrf_ltrc = 'AWRP'
               and f.ryrawrf_slate_send is null  
               and tv.stvterm_code = xapp.syrxapp_term_code
               and tv.stvterm_fa_proc_yr = f.ryrawrf_aidy_code   
             fetch first 100 rows only   
            """
    sql.query(stmt,[aidy]) { ResultSet rs ->
        while (rs.next()) {
            letters.add(
                    new Letter(
                            rs.getString('application_id'),
                            rs.getInt('pidm'),
                            rs.getString('banner_id'),
                            rs.getString('aidy_code'),
                            rs.getString('ltrc'),
                            rs.getBytes('letter'),
                            rs.getString('material_type'),
                            rs.getString('file_name')
                    )
            )
        }
    }
    return letters
}

/**
 * Batch update slate send date for ryrawrf record
 * @param sql
 * @param letters
 */
void batchUpdateSlateUploadInd(Sql sql, List letters, LocalDateTime run) {
    def stmt =
            """
            update ryrawrf
               set ryrawrf_slate_send = ?
             where ryrawrf_pidm = ?
               and ryrawrf_aidy_code = ?
               and ryrawrf_ltrc = ?
            """

    sql.withBatch(25,stmt) {ps ->
        letters.each { l ->
            ps.addBatch(run,l.pidm,l.aidyCode,l.ltrc)
        }
    }
}

/**
 * Insert a tracking record for award letters sent to slate. Pidm aidy, and ltrc.
 * This will be used to determine the material code sent in the index file
 * @param sql
 * @param letter
 */
void batchInsertSlateUploadTrackingRecord(Sql sql, List letters, LocalDateTime run) {
    def stmt =
            """
            insert into ryrawrfst(ryrawrfst_pidm,
                                  ryrawrfst_aidy_code,
                                  ryrawrfst_ltrc,
                                  ryrawrfst_date)
                           values(?,
                                  ?,
                                  ?,
                                  ?)      
            """
    sql.withBatch(25,stmt) {ps ->
        letters.each { l ->
            ps.addBatch(l.pidm,l.aidyCode,l.ltrc,run)
        }
    }
}

/**
 * Writes index file using OpenCsv library.
 * @param path
 * @param letters
 */
void createIndexFile(Path path, List letters) {

    path.withWriter { bw ->
        StatefulBeanToCsv<Letter> beanToCsv = new StatefulBeanToCsvBuilder<Letter>(bw)
                .withSeparator('\t' as char)
                .withMappingStrategy(indexFileHeaderStrategy())
                .build()
        beanToCsv.write(letters)
    }
}

/**
 * Returns CSV header strategy by parsing header string.
 * @return
 */
def indexFileHeaderStrategy() {
    HeaderColumnNameMappingStrategy<Letter> strategy = new HeaderColumnNameMappingStrategyBuilder<Letter>()
            .build()
    strategy.setType(Letter.class)

    new StringReader(Letter.header.join(',')).withReader { reader ->
        CsvToBean<Letter> csvToBean = new CsvToBeanBuilder<Letter>(reader)
                .withMappingStrategy(strategy)
                .build()
        csvToBean.parse()
    }

    return strategy
}

/**
 * Zips up awards letters and index file
 * @param zip
 * @param index
 * @param letters
 */
void zipLetters(Path zip, Path index, List letters) {
    FileOutputStream fos = new FileOutputStream(zip.toFile())
    ZipOutputStream zos = new ZipOutputStream(fos)

    // Put the index file in the Zip
    ZipEntry zipEntry = new ZipEntry(index.fileName.toString())
    zos.putNextEntry(zipEntry)
    zos.write(index.readBytes())

    // For each letter create zip entry and write
    letters.each { l ->
        ZipEntry zipEntryLetter = new ZipEntry(l.fileName)
        zos.putNextEntry(zipEntryLetter)
        zos.write(l.letter)
    }
    zos.close()
    fos.close()
}

/**
 * Create SFTP connection and provides SFTP filesystem object within closure made available to calling method
 * @param properties
 * @param c
 * @return
 */
def sftp(Properties properties, Closure c) {
    SshClient client = SshClient.setUpDefaultClient()
    client.start()
    client.connect(properties.'sftp.user',properties.'sftp.host',properties.'sftp.port' as int).verify().getClientSession().withCloseable { ClientSession clientSession->
        FileKeyPairProvider fileKeyPairProvider = new FileKeyPairProvider(Paths.get(properties.'sftp.private-key'))
        Iterable<KeyPair> keyPairs = fileKeyPairProvider.loadKeys(clientSession)
        clientSession.addPublicKeyIdentity(keyPairs.iterator().next())
        clientSession.auth().verify()
        DefaultSftpClientFactory.INSTANCE.createSftpClient(clientSession).withCloseable { SftpClient sftpClient ->
            SftpFileSystemProvider provider = new SftpFileSystemProvider(client)
            provider.newFileSystem(sftpClient.session).withCloseable(c)
        }
    }
    client.stop()
}

/**
 * Sftp credentials
 * @return
 */
def sftpProps() {
    def properties = new Properties()
    Paths.get(System.properties.'user.home','.credentials','technolutionsSftp.properties').withInputStream {
        properties.load(it)
    }
    return properties
}

/**
 * Database properties from credentials file
 * @return
 */
def dbProps() {
    Properties properties = new Properties()
    Paths.get(System.properties.'user.home','.credentials','bannerProduction.properties').withInputStream {
        properties.load(it)
    }
    return properties
}
/**
 * Pojo Class to represent award letter record with pdf file
 */
@Canonical
class Letter {

    static final String[] header = new String[] {
            'app ref',
            'material type',
            'Filename'
    }

    @CsvBindByName(column = 'app ref')
    String applicationId
    @CsvIgnore
    int pidm
    @CsvIgnore
    String bannerId
    @CsvIgnore
    String aidyCode
    @CsvIgnore
    String ltrc
    @CsvIgnore
    byte[] letter
    @CsvBindByName(column = 'material type')
    String materialType
    @CsvBindByName(column = 'Filename')
    String fileName
}