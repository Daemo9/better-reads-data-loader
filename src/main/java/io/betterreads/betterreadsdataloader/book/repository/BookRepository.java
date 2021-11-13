package io.betterreads.betterreadsdataloader.book.repository;

import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;

import io.betterreads.betterreadsdataloader.book.Book;

@Repository
public interface BookRepository  extends CassandraRepository<Book, String> {

}
