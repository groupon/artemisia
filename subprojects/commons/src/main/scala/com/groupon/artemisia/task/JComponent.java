package com.groupon.artemisia.task;

import com.typesafe.config.Config;
import java.util.List;

/**
 * Created by chlr on 12/22/17.
 */
abstract public class JComponent extends BaseComponent {

     public JComponent(String name) {
          super(name);
     }

     final public APIType componentType = JavaAPIType$.MODULE$;

     /**
      * component Name
      */
     public String name;



}
