package com.streamsets.pipeline.sdk;

import com.streamsets.pipeline.api.StageException;

public class Bootstrap {
	
	public static void main(String[] args)throws StageException,InterruptedException
	{
		StageRunner runner = new StageRunner();
		new ShutDownHook(runner);
		new Thread(runner,"stage-runner").start();
	}
}
