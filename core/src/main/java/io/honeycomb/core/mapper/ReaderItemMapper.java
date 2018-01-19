package io.honeycomb.core.mapper;

import java.util.List;

import io.honeycomb.core.entity.ReaderItem;
import io.honeycomb.core.entity.ReaderSource;

/**
 * Created by guoyubo on 2018/1/15.
 */
public interface ReaderItemMapper {

  public int insert(ReaderItem readerItem);
  public int update(ReaderItem readerItem);
  public void delete(int id);
  public List<ReaderItem> getAll();
  public ReaderItem getById(int id);


}
