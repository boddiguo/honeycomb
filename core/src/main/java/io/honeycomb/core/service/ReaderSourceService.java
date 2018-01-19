package io.honeycomb.core.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

import io.honeycomb.core.entity.ReaderSource;
import io.honeycomb.core.mapper.ReaderSourceMapper;

/**
 * Created by guoyubo on 2018/1/15.
 */
@Service
public class ReaderSourceService {

  @Autowired
  private ReaderSourceMapper readerSourceMapper;


  public List<ReaderSource> getAllReaderSources() {
    return readerSourceMapper.getAll();

  }

  public ReaderSource getReaderSourceById(int id) {
    return readerSourceMapper.getById(id);

  }

}
