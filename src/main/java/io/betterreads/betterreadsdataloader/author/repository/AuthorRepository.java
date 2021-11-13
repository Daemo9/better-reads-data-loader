package io.betterreads.betterreadsdataloader.author.repository;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;
import io.betterreads.betterreadsdataloader.author.Author;

@Repository
public interface AuthorRepository extends CassandraRepository<Author, String> {

}
