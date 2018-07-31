package ch.javaee.springBootBatch.job.flow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.FlowExecutor;
import org.springframework.batch.core.job.flow.State;
import org.springframework.batch.core.job.flow.support.state.AbstractState;
import org.springframework.batch.core.step.NoSuchStepException;
import org.springframework.batch.core.step.StepHolder;
import org.springframework.batch.core.step.StepLocator;

/**
 * {@link State} implementation that delegates to a {@link FlowExecutor} to
 * execute the specified {@link Step}.
 *
 * @author Dave Syer
 * @author Michael Minella
 * @since 2.0
 */
public class GroupStepState extends AbstractState implements StepLocator, StepHolder {

    private final Step step;

    /**
     * @param step the step that will be executed
     */
    public GroupStepState(Step step) {
        super(step.getName());
        this.step = step;
    }

    /**
     * @param name for the step that will be executed
     * @param step the step that will be executed
     */
    public GroupStepState(String name, Step step) {
        super(name);
        this.step = step;
    }

    @Override
    public FlowExecutionStatus handle(FlowExecutor executor) throws Exception {
        return new FlowExecutionStatus(executor.executeStep(step));
    }

    /**
     * @deprecated in favor of using {@link StepLocator#getStep(String)}.
     */
    @Override
    public Step getStep() {
        return step;
    }

    /* (non-Javadoc)
     * @see org.springframework.batch.core.job.flow.State#isEndState()
     */
    @Override
    public boolean isEndState() {
        return false;
    }

    /* (non-Javadoc)
     * @see org.springframework.batch.core.step.StepLocator#getStepNames()
     */
    @Override
    public Collection<String> getStepNames() {
        List<String> names = new ArrayList<String>();

        names.add(step.getName());

        if(step instanceof StepLocator) {
            names.addAll(((StepLocator)step).getStepNames());
        }

        return names;
    }

    /* (non-Javadoc)
     * @see org.springframework.batch.core.step.StepLocator#getStep(java.lang.String)
     */
    @Override
    public Step getStep(String stepName) throws NoSuchStepException {
        Step result = null;

        if(step.getName().equals(stepName)) {
            result = step;
        } else if(step instanceof StepLocator) {
            result = ((StepLocator) step).getStep(stepName);
        }

        return result;
    }
}
