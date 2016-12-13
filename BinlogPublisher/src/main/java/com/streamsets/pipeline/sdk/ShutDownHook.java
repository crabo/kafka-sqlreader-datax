package com.streamsets.pipeline.sdk;

public class ShutDownHook implements  Runnable  {
	final Bootstrap instance;

    public ShutDownHook(Bootstrap runner ){
        super();
        this.instance = runner;
        Runtime.getRuntime().addShutdownHook(new Thread(this)); 
    }

    @Override  
    public void run() {
    	if(instance.source!=null)
    		instance.source.destroy();
		 if(instance.target!=null)
			 instance.target.destroy();
		 System.out.print("=====ShutDownHook cleanup=====");
    }
}