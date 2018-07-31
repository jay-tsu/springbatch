package ch.javaee.springBootBatch.job.flow;

/*
 * Copyright 2006-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.FlowExecution;
import org.springframework.batch.core.job.flow.FlowExecutionException;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.FlowExecutor;
import org.springframework.batch.core.job.flow.State;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.job.flow.support.StateTransition;
import org.springframework.beans.factory.InitializingBean;

/**
 * A {@link Flow} that branches conditionally depending on the exit status of
 * the last {@link State}. The input parameters are the state transitions (in no
 * particular order). The start state name can be specified explicitly (and must
 * exist in the set of transitions), or computed from the existing transitions,
 * if unambiguous.
 *
 * @author Dave Syer
 * @author Michael Minella
 * @since 2.0
 */
public class GroupFlow extends SimpleFlow {

    private static final Log logger = LogFactory.getLog(org.springframework.batch.core.job.flow.support.SimpleFlow.class);


    /**
     * Create a flow with the given name.
     *
     * @param name the name of the flow
     */
    public GroupFlow(String name) { super(name);
    }


    /**
     * @see Flow#resume(String, FlowExecutor)
     */
    @Override
    public FlowExecution resume(String stateName, FlowExecutor executor) throws FlowExecutionException {

        FlowExecutionStatus status = FlowExecutionStatus.UNKNOWN;
        State state = getState(stateName);

        if (logger.isDebugEnabled()) {
            logger.debug("Resuming state="+stateName+" with status="+status);
        }
        StepExecution stepExecution = null;

        // Terminate if there are no more states
        while (isFlowContinued(state, status, stepExecution)) {
            stateName = state.getName();

            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("Handling state="+stateName);
                }
                state.handle(executor);
                status = updateFlowExecutionStatus(stateName, status, executor);

                stepExecution = executor.getStepExecution();
            }
            catch (FlowExecutionException e) {
                executor.close(new FlowExecution(stateName, status));
                throw e;
            }
            catch (Exception e) {
                executor.close(new FlowExecution(stateName, status));
                throw new FlowExecutionException(String.format("Ended flow=%s at state=%s with exception", getName(),
                        stateName), e);
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Completed state="+stateName+" with status="+status);
            }

            state = nextState(stateName, status, stepExecution);
        }

        FlowExecution result = new FlowExecution(stateName, status);
        executor.close(result);
        return result;

    }

    private FlowExecutionStatus updateFlowExecutionStatus(final String stateName, FlowExecutionStatus status, FlowExecutor executor) {
        String stepName = stateName.split("\\.")[1];
        return executor.getJobExecution().getStepExecutions().stream()
                .filter(stepExecution -> stepExecution.getStepName().equals(stepName))
                .findFirst()
                .map(stepExecution -> {
                   if (status.isFail() || status.isStop()) {
                        return status;
                    } else if (status.isEnd() && (stepExecution.getExitStatus().equals(ExitStatus.FAILED) || stepExecution.getExitStatus().equals(ExitStatus.STOPPED))) {
                        return new FlowExecutionStatus(stepExecution.getExitStatus().getExitCode());
                    } else {
                        return new FlowExecutionStatus(stepExecution.getExitStatus().getExitCode());
                    }
                }).orElse(status);
    }
}

