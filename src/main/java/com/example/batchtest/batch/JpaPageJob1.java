package com.example.batchtest.batch;

import com.example.batchtest.domain.Dept;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import javax.persistence.EntityManagerFactory;

@Slf4j
@RequiredArgsConstructor
@Configuration
public class JpaPageJob1 {
    final JobBuilderFactory jobBuilderFactory;
    final StepBuilderFactory stepBuilderFactory;
    final EntityManagerFactory entityManagerFactory;

    private int chunkSize = 10;

    @Bean
    public Job JpaPageJob1_batchBuild(){
        return jobBuilderFactory.get("jpaPageJob1")
                .start(JpaPageJob1_step1()).build();
    }

    @Bean
    public Step JpaPageJob1_step1(){
        return stepBuilderFactory.get("jpaPageJob_step1")
                .<Dept, Dept>chunk(chunkSize)
                .reader(jpaPageJob1_dbItemReader())
                .writer(csvJob2_FileWriter(new FileSystemResource("output/csvOutput.csv")))
                .build();
    }

    @Bean
    public JpaPagingItemReader<Dept> jpaPageJob1_dbItemReader(){
        return new JpaPagingItemReaderBuilder<Dept>()
                .name("jpaPageJob1_dbItemReader")
                .entityManagerFactory(entityManagerFactory)
                .pageSize(chunkSize)
                .queryString("SELECT d FROM Dept d")
                .build();
    }

    @Bean
    public FlatFileItemWriter<Dept> csvJob2_FileWriter(Resource resource){
        BeanWrapperFieldExtractor<Dept> beanWrapperFieldExtractor = new BeanWrapperFieldExtractor<>();
        beanWrapperFieldExtractor.setNames(new String[]{"deptNo", "dName", "loc"});
        beanWrapperFieldExtractor.afterPropertiesSet();

        DelimitedLineAggregator<Dept> delimitedLineAggregator = new DelimitedLineAggregator<>();
        delimitedLineAggregator.setDelimiter(",");
        delimitedLineAggregator.setFieldExtractor(beanWrapperFieldExtractor);

        return new FlatFileItemWriterBuilder<Dept>().name("csvJob2_FileWriter")
                .resource(resource)
                .lineAggregator(delimitedLineAggregator)
                .build();
    }
}
