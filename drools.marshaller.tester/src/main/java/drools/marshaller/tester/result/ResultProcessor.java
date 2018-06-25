package drools.marshaller.tester.result;

import java.util.ArrayList;
import java.util.List;

import drools.marshaller.tester.Account;


public class ResultProcessor {

  private List<Account> results = new ArrayList<>();

  public List<Account> getResults() {
    return results;
  }

  public <T> void processResults(T captureEvent) {
    if (captureEvent instanceof Account) {
      results.add((Account) captureEvent);
    }
  }


}
