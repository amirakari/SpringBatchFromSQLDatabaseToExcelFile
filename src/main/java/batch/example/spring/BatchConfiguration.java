package batch.example.spring;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import batch.example.spring.model.User;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {
	
	@Autowired
     public JobBuilderFactory jobBuilderFactory;
	
	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	public DataSource dataSource;

	 
	@Bean
	public JdbcCursorItemReader<User> readerFromDB(){
		JdbcCursorItemReader<User> reader = new JdbcCursorItemReader<User>();
		reader.setDataSource(dataSource);
		reader.setSql("select id,nom,prenom,age from User");
		reader.setRowMapper(new RowMapper<User>(){
			@Override
			public User mapRow(ResultSet rs,int rowNum) throws SQLException{
				User u = new User();
				u.setId(rs.getInt("id"));
				u.setNom(rs.getString("nom"));
				u.setPrenom(rs.getString("prenom"));
				u.setAge(rs.getInt("age"));
				return u;
			}
		});
		return reader;
	}
	
	@Bean
	public FlatFileItemWriter<User> writeIntoCSV(){
		FlatFileItemWriter<User> writer= new FlatFileItemWriter<>();
		writer.setResource(new FileSystemResource("C://Users//USER//Desktop//user.csv"));
		writer.setLineAggregator(new DelimitedLineAggregator<User>() {{
            setDelimiter(",");
            setFieldExtractor(new BeanWrapperFieldExtractor<User>() {{
                setNames(new String[] { "id", "nom","prenom","age"});
            }});
        }});
		writer.setHeaderCallback(writer1 -> writer1.write("Id,Nom,Prenom,Age"));
		return writer;
	}
	
	@Bean
	public Step step(){
	return	stepBuilderFactory.get("step").<User,User>chunk(10).reader(readerFromDB()).writer(writeIntoCSV()).build();
	}
	
	@Bean
	public Job job(){
		return jobBuilderFactory.get("job").incrementer(new RunIdIncrementer()).flow(step()).end().build();
	}
}
