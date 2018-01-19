package io.honeycomb.core.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

import lombok.Data;

/**
 * Created by guoyubo on 2018/1/16.
 */
@ConfigurationProperties("reader.job")
@Data
@Component
public class ReaderJobProperties {

  private Map<String, String> types = new HashMap<>();


}
