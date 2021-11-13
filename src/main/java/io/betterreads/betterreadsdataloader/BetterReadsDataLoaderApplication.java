package io.betterreads.betterreadsdataloader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import io.betterreads.betterreadsdataloader.author.Author;
import io.betterreads.betterreadsdataloader.author.repository.AuthorRepository;
import io.betterreads.betterreadsdataloader.book.Book;
import io.betterreads.betterreadsdataloader.book.repository.BookRepository;
import io.betterreads.betterreadsdataloader.connection.DataStaxAstraProperties;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterReadsDataLoaderApplication {

	@Autowired
	AuthorRepository authorRepository;
	@Autowired
	BookRepository bookRepository;
	@Value("${datadump.location.works}")
	private String worksDumpLocation;
	
	@Value("${datadump.location.author}")
	private String authorDumpLocation;
	
	public static void main(String[] args) {
		SpringApplication.run(BetterReadsDataLoaderApplication.class, args);
	}
	
	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
		Path bundle = astraProperties.getSecureConnectBundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(bundle) ;
	}
	
	@PostConstruct
	public void start() {
//		initAuthors();
		initWorks();
	}

	private void initAuthors() {
		Path path = Paths.get(authorDumpLocation);
		try(Stream<String> lines = Files.lines(path)){
			
			lines.forEach(line ->
			{
				String jsonString = line.substring(line.indexOf("{"));
				try {
				JSONObject jsonObject = new JSONObject(jsonString);
				
				Author author = new Author();
				author.setId(jsonObject.optString("key").replace("/authors/", ""));
				author.setName(jsonObject.optString("name"));
				author.setPersonalName(jsonObject.optString("personal_name"));
				System.out.println("saving author - "+author.getName());
				//save using repository
				authorRepository.save(author);
				} catch (JSONException e) {
					e.printStackTrace();
				}
			});
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	private void initWorks() {
		Path path = Paths.get(worksDumpLocation);
		DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
		try(Stream<String> lines = Files.lines(path)){
			
			lines.limit(50).forEach(line ->
			{
				String jsonString = line.substring(line.indexOf("{"));
				try {
					JSONObject jsonObject = new JSONObject(jsonString);
					//Constructing book object
					Book book = new Book();
					book.setId(jsonObject.getString("key").replace("/works/", ""));
					book.setName(jsonObject.optString("title"));
					JSONObject descriptionJsonObject = jsonObject.optJSONObject("description");
					if(descriptionJsonObject!=null) {
						book.setDescription(descriptionJsonObject.optString("value"));
					}
					JSONObject publishedObject = jsonObject.optJSONObject("created");
					if(publishedObject!=null) {
						String dateStr =publishedObject.getString("value");
						book.setPublishedDate(LocalDate.parse(dateStr,dateFormatter));
					}
					JSONArray coversJsonArray = jsonObject.optJSONArray("covers");
					if(coversJsonArray!=null) {
						List<String> coverIds = new ArrayList<>();
						for(int i=0;i<coversJsonArray.length();i++) {
							coverIds.add(coversJsonArray.getString(i));
						}
						book.setCoverIds(coverIds);
					}
					JSONArray authorJsonArray = jsonObject.optJSONArray("authors");
					if(authorJsonArray!=null) {
						List<String> authorIds = new ArrayList<>();
						List<String> authorNames = new ArrayList<>();
						for(int i=0;i<authorJsonArray.length();i++) {
							String authorId = authorJsonArray.getJSONObject(i).getJSONObject("author").getString("key")
									.replace("/authors/", "");
							authorIds.add(authorId);
						}
						book.setAuthorIds(authorIds);
						authorIds.stream().map(id -> authorRepository.findById(id))
						.map(optionalAuthor-> 
						{
							if(!optionalAuthor.isPresent()) return "Unknown Author";
							return optionalAuthor.get().getName();
						}
								).collect(Collectors.toList());
						book.setAuthorNames(authorNames);
					}
					System.out.println("saving book - "+book.getName());
					//save using repository
					bookRepository.save(book);
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
