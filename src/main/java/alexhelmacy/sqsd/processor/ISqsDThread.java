package alexhelmacy.sqsd.processor;

public interface ISqsDThread extends  Runnable{
  void stop();//stop the sqsd thread
  boolean running();//is the thread running
  boolean closed();//is the thread closed
}
