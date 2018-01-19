package io.honeycomb.core.service;

import org.springframework.stereotype.Service;

import com.google.common.collect.Lists;

import java.util.List;

import io.honeycomb.core.entity.ReaderItem;

/**
 * Created by guoyubo on 2018/1/15.
 */
@Service
public class ReaderItemService {



  public List<ReaderItem> getAllReaderItems() {
    return Lists.newArrayList(new ReaderItem());

  }

}
