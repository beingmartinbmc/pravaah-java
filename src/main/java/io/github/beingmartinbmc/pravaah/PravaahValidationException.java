package io.github.beingmartinbmc.pravaah;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PravaahValidationException extends RuntimeException {
    private final List<PravaahIssue> issues;

    public PravaahValidationException(List<PravaahIssue> issues) {
        super("Pravaah validation failed with " + issues.size() + " issue" + (issues.size() == 1 ? "" : "s"));
        this.issues = Collections.unmodifiableList(new ArrayList<>(issues));
    }

    public List<PravaahIssue> getIssues() {
        return issues;
    }
}
