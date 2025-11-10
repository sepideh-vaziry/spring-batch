package com.example.springbatch;

import javax.sql.DataSource;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableBatchProcessing
@RequiredArgsConstructor
public class BatchConfig {

  private final DataSource dataSource;

  @Bean
  public FlatFileItemReader<Product> reader() {
    FlatFileItemReader<Product> reader = new FlatFileItemReader<>();
    reader.setResource(new org.springframework.core.io.ClassPathResource("products.csv"));
    reader.setLinesToSkip(1);
    reader.setLineMapper(new DefaultLineMapper<>() {{
      setLineTokenizer(new DelimitedLineTokenizer() {{
        setNames("id", "name", "price");
      }});
      setFieldSetMapper(fieldSet -> Product.builder()
          .id(fieldSet.readLong("id"))
          .name(fieldSet.readString("name"))
          .price(fieldSet.readDouble("price"))
          .build());
    }});

    return reader;
  }

  @Bean
  public JdbcBatchItemWriter<Product> writer() {
    JdbcBatchItemWriter<Product> writer = new JdbcBatchItemWriter<>();
    writer.setDataSource(dataSource);
    writer.setSql("INSERT INTO product(id, name, price) VALUES (:id, :name, :price)");
    writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());

    return writer;
  }

  @Bean
  public Step importProductStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
    return new StepBuilder("importProductStep", jobRepository)
        .<Product, Product>chunk(2, transactionManager)
        .reader(reader())
        .writer(writer())
        .build();
  }

  @Bean
  public Job importProductJob(JobRepository jobRepository, Step importProductStep) {
    return new JobBuilder("importProductJob", jobRepository)
        .start(importProductStep)
        .build();
  }

}
