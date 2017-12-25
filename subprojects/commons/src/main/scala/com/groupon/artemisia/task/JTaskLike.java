package com.groupon.artemisia.task;

import com.typesafe.config.Config;
import java.util.Map;
import java.util.Optional;

/**
 * Created by chlr on 12/22/17.
 */
abstract public class JTaskLike extends BaseTaskLike {

    @Override
    final public APIType taskType() { return JavaAPIType$.MODULE$; }

    abstract public Optional<Config> outputConfig();

    public Map<String, Object> fieldDefinition;

    abstract public Task create(String name, Config config, Config reference);

}
