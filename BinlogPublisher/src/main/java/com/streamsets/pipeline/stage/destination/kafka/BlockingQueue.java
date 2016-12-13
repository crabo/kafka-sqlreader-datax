package com.streamsets.pipeline.stage.destination.kafka;
import java.util.LinkedList;
import java.util.Queue;

public class BlockingQueue<T> {
    private Queue<T> queue = new LinkedList<T>();
    private int capacity;

    public BlockingQueue(int capacity) {
        this.capacity = capacity;
    }

    public synchronized void put(T element) {
        while(queue.size() == capacity) {
            try {
				wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        }

        queue.add(element);
        
        notifyAll();
    }
    
    public synchronized void waitIfNeccessary() {
        while(queue.isEmpty()) {
            try {
				wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        }
    }
    
    public synchronized T poll() {
    	if(queue.isEmpty())
    		return null;

        T item = queue.remove();
        
        notifyAll();
        
        return item;
    }
}