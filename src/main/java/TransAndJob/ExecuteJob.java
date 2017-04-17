package TransAndJob;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.RepositoryPluginType;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.RepositoriesMeta;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.RepositoryMeta;

import static TransAndJob.RepositoryConnected.connectRepository;

/**
 * Created by Administrator on 2017/4/12.
 */
public class ExecuteJob {

    public static void runNativeJob(String filename){
        try {
            KettleEnvironment.init();

            JobMeta jobMeta = new JobMeta(filename, null);//构建了一个为null的资源库引用
            Job job = new Job(null, jobMeta);
            //向Job 脚本传递参数，脚本中获取参数值：${参数名}
            //job.setVariable(paraname, paravalue);
            job.start();
            job.waitUntilFinished();
            if (job.getErrors() > 0) {
                throw new RuntimeException("There are errors during job exception!(执行job发生异常)");
            }
        }
        catch (KettleException e ) {
            // TODO Put your exception-handling code here.
            System.out.println(filename);
            System.out.println("-------------------------------------------------");
            System.out.println(e);
        }
    }

    public static void runRepositoryJob(String filename) {
        try {
            KettleEnvironment.init();
            EnvUtil.environmentInit();

            Repository repository = connectRepository(); //连接资源库

            RepositoryDirectoryInterface tree = repository.loadRepositoryDirectoryTree();
            RepositoryDirectoryInterface directory = tree.findDirectory("/");
            JobMeta jobMeta = repository.loadJob(filename,directory, null, null);
            Job job = new Job(repository, jobMeta); //前面不能为null
            job.start();
            job.waitUntilFinished();
        } catch (KettleException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String filename = "C:/Users/Administrator/Desktop/kettle学习（书上例子）/635179_code_ch04/load_rentals233.kjb";
        runNativeJob(filename);
//        String filename="load_rentals233";
//        runRepositoryJob(filename);
    }
}
