package com.groupon.artemisia.task;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.util.Optional;

/**
 * Created by chlr on 12/26/17.
 */
class JTestTaskLike extends JTaskLike {


    @Override
    public String taskName() {
        return "JTestTask";
    }

    @Override
    public Optional<Config> outputConfig() {
        return Optional.empty();
    }

    @Override
    public Task create(String name, Config config, Config reference) {
        return new JSubTask(name, config.getInt("num1"), config.getInt("num2"));
    }

    @Override
    public Config defaultConfig() {
        return ConfigFactory.empty();
    }

    @Override
    public Config paramConfigDoc() {
        return ConfigFactory.empty();
    }

    @Override
    public String info() {
        return "This is a Test subtraction task";
    }

    @Override
    public String desc() {
        return "This is a Test subtraction task";
    }

    @Override
    public String outputConfigDesc() {
        return null;
    }
}

class JSubTask extends Task {

    private int num1, num2;

    public JSubTask(String taskName, int num1, int num2) {
        super(taskName);
        this.num1 = num1;
        this.num2 = num2;
    }

    @Override
    public void setup() {}

    @Override
    public Config work() {
        return wrapAsStats(ConfigValueFactory.fromAnyRef(num1 + num2));
    }

    @Override
    public void teardown() {}
}
