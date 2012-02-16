package com.rapleaf.cascading_ext.workflow2.action;

import org.apache.commons.lang.NotImplementedException;

import com.rapleaf.cascading_ext.relevance.BytesRelevance;
import com.rapleaf.cascading_ext.relevance.PINAndOwnerRelevance;
import com.rapleaf.cascading_ext.relevance.PINRelevance;
import com.rapleaf.cascading_ext.relevance.Relevance;
import com.rapleaf.cascading_ext.relevance.StringRelevance;
import com.rapleaf.cascading_ext.workflow2.Action;
import com.rapleaf.types.new_person_data.PIN;
import com.rapleaf.types.new_person_data.PINAndOwner;

public abstract class RelevanceAction extends Action {
  
  public RelevanceAction() {
    super();
  }
  
  public RelevanceAction(String tmpDir) {
    super(tmpDir);
  }
  
  protected Relevance getRelevance(Class type) {
    if (type == byte[].class) {
      return new BytesRelevance(16); // the argument is irrelevant in this use
                                     // case
    } else if (type == PIN.class) {
      return new PINRelevance();
    } else if (type == PINAndOwner.class) {
      return new PINAndOwnerRelevance();
    } else if (type == String.class) {
      return new StringRelevance(16); // the argument is irrelevant in this use
                                      // case
    } else {
      throw new NotImplementedException("Relevance for class " + type
          + " has not been implemented yet!");
    }
  }
}
