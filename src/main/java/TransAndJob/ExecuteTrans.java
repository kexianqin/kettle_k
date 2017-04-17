package TransAndJob;




import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.RepositoryPluginType;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.repository.*;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.repository.kdr.KettleDatabaseRepositoryMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;


/**
 * Created by Administrator on 2017/4/11.
 */
public class ExecuteTrans extends RepositoryConnected {

    public static void runNativeTransformation(String filename) {
        runNativeTransformation(null,filename);
    }
    public static void runNativeTransformation(String[] params,String filename) {
        try {
            //初始化
            KettleEnvironment.init();
            EnvUtil.environmentInit();
            TransMeta transMeta = new TransMeta(filename);
            //转换
            Trans trans = new Trans(transMeta);
            //执行
            trans.execute(params); // You can pass arguments instead of null.
            //等待结束
            trans.waitUntilFinished();
            if ( trans.getErrors() > 0 ){
                throw new RuntimeException( "There were errors during transformation execution." );
            }
        }
        catch (KettleException e ) {
            // TODO Put your exception-handling code here.
            System.out.println(filename);
            System.out.println("-------------------------------------------------");
            System.out.println(e);
        }
    }
    public static void runRepositoryTransformation(String filename){
        try {
            KettleEnvironment.init();
            EnvUtil.environmentInit();

            Repository repository=connectRepository(); //连接资源库

            RepositoryDirectoryInterface tree = repository.loadRepositoryDirectoryTree();
            RepositoryDirectoryInterface directory = tree.findDirectory("/test");   //找资源库中的文件夹

            TransMeta transformationMeta = repository.loadTransformation(filename, directory, null, true, null ) ;
            Trans trans = new Trans(transformationMeta);
            trans.execute(null);
            trans.waitUntilFinished();
            if ( trans.getErrors() > 0 ){
                throw new RuntimeException( "There were errors during transformation execution." );
            }
        } catch (KettleException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {

//        String filename = "C:/Users/Administrator/Desktop/kettle学习（书上例子）/635179_code_ch04/load_dim_actor.ktr";
//        runNativeTransformation(filename);
        String filename="load_dim_actor";
        runRepositoryTransformation(filename);
    }
}
