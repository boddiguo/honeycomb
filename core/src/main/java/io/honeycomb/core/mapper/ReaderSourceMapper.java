package io.honeycomb.core.mapper;

import java.util.List;

import io.honeycomb.core.entity.ReaderSource;

/**
 * Created by guoyubo on 2018/1/15.
 */
public interface ReaderSourceMapper {

  public int insert(ReaderSource readerSource);
  public int update(ReaderSource readerSource);
  public void delete(int id);
  public List<ReaderSource> getAll();
  public ReaderSource getById(int id);


}
