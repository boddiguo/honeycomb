package io.honeycomb.core.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.common.collect.Lists;

import java.util.List;

import io.honeycomb.core.entity.ReaderItem;
import io.honeycomb.core.mapper.ReaderItemMapper;

/**
 * Created by guoyubo on 2018/1/15.
 */
@Service
public class ReaderItemService {

  @Autowired
  ReaderItemMapper readerItemMapper;


  public List<ReaderItem> getAllReaderItems() {
    return readerItemMapper.getAll();

  }

}
