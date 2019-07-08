package name.ealen.batch;

import name.ealen.listener.JobListener;
import name.ealen.model.Access;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.job.SimpleJob;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.repository.support.SimpleJobRepository;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by EalenXie on 2018/9/10 14:50.
 * :@EnableBatchProcessing提供用于构建批处理作业的基本配置
 */
@Configuration
@EnableBatchProcessing
public class DataBatchConfiguration {
    private static final Logger log = LoggerFactory.getLogger(DataBatchConfiguration.class);
    @Autowired
    JobExplorer jobExplorer;
    @Autowired
    JobLauncher jobLauncher;
    @Autowired
    JobRepository jobRepository;
    @Resource
    private JobBuilderFactory jobBuilderFactory;    //用于构建JOB

    @Resource
    private StepBuilderFactory stepBuilderFactory;  //用于构建Step

//    @Resource
//    private EntityManagerFactory emf;           //注入实例化Factory 访问数据

//    @Resource
//    private JobListener jobListener;            //简单的JOB listener

    /**
     * 一个简单基础的Job通常由一个或者多个Step组成
     */
    @Bean
    public Job dataHandleJob() {
        return jobBuilderFactory.get("dataHandleJob").incrementer(new JobParametersIncrementer() {
            @Override
            public JobParameters getNext(JobParameters parameters) {
                return new JobParametersBuilder().addDate("execTime",new Date()).toJobParameters();
            }
        })
                .incrementer(new RunIdIncrementer())
                .start(handleDataStep())  //start是JOB执行的第一个step
//                .flow(handleDataStep())
//                .end()
//                .next(xxxStep())
//                .next(xxxStep())
//                ...
//                .istener(jobListener)      //设置了一个简单JobListener
                .build();
    }

    /**
     * 一个简单基础的Step主要分为三个部分
     * ItemReader : 用于读取数据
     * ItemProcessor : 用于处理数据
     * ItemWriter : 用于写数据
     */
    @Bean
    public Step handleDataStep() {
        return stepBuilderFactory.get("getData").
                <Access, Access>chunk(20).        // <输入,输出> 。chunk通俗的讲类似于SQL的commit; 这里表示处理(processor)100条后写入(writer)一次。
                faultTolerant().retryLimit(3).retry(Exception.class).skipLimit(100).skip(Exception.class). //捕捉到异常就重试,重试100次还是异常,JOB就停止并标志失败
                reader(getDataReader()).         //指定ItemReader
                processor(getDataProcessor()).   //指定ItemProcessor
                writer(getDataWriter()).         //指定ItemWriter
                build();
    }
    @Bean
    public ItemReader<? extends Access> getDataReader() {
        ArrayList<Access> list = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            Access access = new Access();
            access.setId(i);
            access.setBrandName("brand_"+i);
            list.add(access);
        }
        ListItemReader<Access> reader = new ListItemReader<>(list);
//        //读取数据,这里可以用JPA,JDBC,JMS 等方式 读入数据
//        JpaPagingItemReader<Access> reader = new JpaPagingItemReader<>();
//        //这里选择JPA方式读数据 一个简单的 native SQL
//        String sqlQuery = "SELECT * FROM access";
//        try {
//            JpaNativeQueryProvider<Access> queryProvider = new JpaNativeQueryProvider<>();
//            queryProvider.setSqlQuery(sqlQuery);
//            queryProvider.setEntityClass(Access.class);
//            queryProvider.afterPropertiesSet();
//            reader.setEntityManagerFactory(emf);
//            reader.setPageSize(3);
//            reader.setQueryProvider(queryProvider);
//            reader.afterPropertiesSet();
//            //所有ItemReader和ItemWriter实现都会在ExecutionContext提交之前将其当前状态存储在其中,如果不希望这样做,可以设置setSaveState(false)
//            reader.setSaveState(true);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
        return reader;
    }

    @Bean
    public ItemProcessor<Access, Access> getDataProcessor() {
        return new ItemProcessor<Access, Access>() {
            @Override
            public Access process(Access access) throws Exception {
                log.info("processor data : " + access.toString());  //模拟  假装处理数据,这里处理就是打印一下
                access.setBrandName(access.getBrandName()+"____");
                return access;
            }
        };
//        lambda也可以写为:
//        return access -> {
//            log.info("processor data : " + access.toString());
//            return access;
//        };
    }

    @Bean
    public ItemWriter<Access> getDataWriter() {
        return new ItemWriter<Access>() {
            @Override
            public void write(List<? extends Access> items) throws Exception {
                for (Access access : items) {
                    log.info("write data : " + access); //模拟 假装写数据 ,这里写真正写入数据的逻辑
                }
            }
        };
    }
}
