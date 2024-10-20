package alexhelmacy.sqsd;

public interface ISqsD{
  public void start();//start SQSD
  public void stop(String reason);//Stop SQSD
}
