package TransAndJob;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.RepositoryPluginType;
import org.pentaho.di.repository.RepositoriesMeta;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryMeta;

/**
 * Created by Administrator on 2017/4/14.
 */
public class RepositoryConnected {

    private static final String repositoryName ="kettle_repository"; //资源库名称

    private static final String  repositoryUserName="admin"; //资源库账号
    private static final String  repositoryPassword="admin"; //资源库密码


    /**
     *  连接资源库
     */
    protected static Repository connectRepository(){
        RepositoriesMeta repositoriesMeta = new RepositoriesMeta();
        try {
            repositoriesMeta.readData();                                                     //资源库元数据都在Repositories.xml中,该方法用来读取
            RepositoryMeta repositoryMeta = repositoriesMeta.findRepository(repositoryName);
            PluginRegistry registry = PluginRegistry.getInstance();                          //获得资源库对象
            Repository repository =registry.loadClass(RepositoryPluginType.class,repositoryMeta,Repository.class);
            repository.init(repositoryMeta);
            repository.connect(repositoryUserName,repositoryPassword);
            return repository;
        } catch (KettleException e) {
            e.printStackTrace();
        }
        return null;
    }
}
//    方法二:       KettleDatabaseRepository repository = new KettleDatabaseRepository();
//            DatabaseMeta dataMeta =
//                    new DatabaseMeta("kettle_repository","MySQL","JDBC","192.168.1.124","kettle_repository","3306","root","123456");//可用XML来代替
//            String repositoryXML="<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
//                    "<connection>" +
//                    "<name>kettle_repository</name>" +
//                    "<server>192.168.1.124</server>" +
//                    "<type>Mysql</type>" +
//                    "<access>Native</access>" +
//                    "<database>kettle_repository</database>" +
//                    "<port>3306</port>" +
//                    "<username>root</username>" +
//                    "<password>123456</password>" +
//                    "</connection>";
//            DatabaseMeta dataMeta =
//                    new DatabaseMeta(repositoryXML);
//            KettleDatabaseRepositoryMeta kettleDatabaseMeta =
//                    new KettleDatabaseRepositoryMeta("kettle_repository", "kettle_repository", "king description",dataMeta);
//            repository.init(kettleDatabaseMeta);
//            repository.connect("admin","admin");
