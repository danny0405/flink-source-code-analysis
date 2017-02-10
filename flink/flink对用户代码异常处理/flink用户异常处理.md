# flink 对用户代码异常的处理

## 前言

flink 的架构在 flink 基本组件一节已经介绍过，其中的 TaskManager 负责监护 task 的执行，对于每个 task，flink 都会启动一个线程去执行，那么当用户的代码抛出异常时，flink 的处理逻辑是什么呢？

## 异常后的组件通信

flink 的 task 的 Runnable 类是 Task.java，我们观察到它的 `run()` 方法真个被一个大的 try catch 包住，我们重点关注 catch 用户异常之后的部分：

```java
//Task line612
		catch (Throwable t) {

			// ----------------------------------------------------------------
			// the execution failed. either the invokable code properly failed, or
			// an exception was thrown as a side effect of cancelling
			// ----------------------------------------------------------------

			try {
				// transition into our final state. we should be either in DEPLOYING, RUNNING, CANCELING, or FAILED
				// loop for multiple retries during concurrent state changes via calls to cancel() or
				// to failExternally()
				while (true) {
					ExecutionState current = this.executionState;
```

简单总结其逻辑：

- 如果当前的执行状态是 `ExecutionState.RUNNING` 或者 `ExecutionState.DEPLOYING`，表明是从正常运行到异常状态的过度，这时候判断是主动 Cancel 执行，如果是，执行 StreamTask 的 cancel 方法， 并通知观察者它的状态已变成：`ExecutionState.CANCELED`；如果不是主动 Cancel，表明是用户异常触发，这时候同样执行 StreamTask 的 cancel 方法，然后通知观察者它的状态变成：`ExecutionState.FAILED`，这里的 cancel 方法留给 flink 内部的算子来实现，对于普通 task ，会停止消费上游数据，对于 source task，会停止发送源数据

- 对于用户异常来说，通知观察者的状态应该为 `ExecutionState.FAILED`，*我们下面详细分析*

- finally 的部分会释放掉这个 task 占有的所有资源，包括线程池、输入 InputGate 及 写出 ResultPartition 占用的全部 BufferPool、缓存的 jar 包等，最后通知 TaskManager 这个 Job 的 这个 task 已经执行结束：

  `notifyFinalState()`


- 如果异常逻辑发生了任何其它异常，说明是 TaskManager 相关环境发生问题，这个时候会杀死 TaskManager

### 通知TaskManager

上面提到，finally 的最后阶段会通知 TaskManager，我们来梳理逻辑：

```java
//TaskManager line444
// removes the task from the TaskManager and frees all its resources
        case TaskInFinalState(executionID) =>
          unregisterTaskAndNotifyFinalState(executionID)
          
//TaskManager line1228
private def unregisterTaskAndNotifyFinalState(executionID: ExecutionAttemptID): Unit = {

    val task = runningTasks.remove(executionID)
    if (task != null) {

      // the task must be in a terminal state
      if (!task.getExecutionState.isTerminal) {
        try {
          task.failExternally(new Exception("Task is being removed from TaskManager"))
        } catch {
          case e: Exception => log.error("Could not properly fail task", e)
        }
      }
      
//TaskManager line1251
     self ! decorateMessage(
        UpdateTaskExecutionState(
          new TaskExecutionState(
            task.getJobID,
            task.getExecutionId,
            task.getExecutionState,
            task.getFailureCause,
            accumulators)
        )
      )
       
//ExecutionGraph line1189
				case FAILED:
					attempt.markFailed(state.getError(userClassLoader));
					return true;

//Execution line658
	void markFinished(Map<AccumulatorRegistry.Metric, Accumulator<?, ?>> flinkAccumulators, Map<String, Accumulator<?, ?>> userAccumulators) {

		// this call usually comes during RUNNING, but may also come while still in deploying (very fast tasks!)
		while (true) {
			ExecutionState current = this.state;

			if (current == RUNNING || current == DEPLOYING) {

				if (transitionState(current, FINISHED)) {
					try {

//Execution line991
			try {
				vertex.notifyStateTransition(attemptId, targetState, error);
			}
			catch (Throwable t) {
				LOG.error("Error while notifying execution graph of execution state transition.", t);
			}
                      
//ExecutionGraph line1291
	void notifyExecutionChange(JobVertexID vertexId, int subtask, ExecutionAttemptID executionID, ExecutionState
							newExecutionState, Throwable error)
	{
     //...
        // see what this means for us. currently, the first FAILED state means -> FAILED
		if (newExecutionState == ExecutionState.FAILED) {
			fail(error);
		}

//ExecutionGraph line845     
	public void fail(Throwable t) {
		while (true) {
			JobStatus current = state;
			// stay in these states
			if (current == JobStatus.FAILING ||
				current == JobStatus.SUSPENDED ||
				current.isGloballyTerminalState()) {
				return;
			} else if (current == JobStatus.RESTARTING && transitionState(current, JobStatus.FAILED, t)) {
				synchronized (progressLock) {
					postRunCleanup();
					progressLock.notifyAll();

					LOG.info("Job {} failed during restart.", getJobID());
					return;
				}
			} else if (transitionState(current, JobStatus.FAILING, t)) {
				this.failureCause = t;

				if (!verticesInCreationOrder.isEmpty()) {
					// cancel all. what is failed will not cancel but stay failed
					for (ExecutionJobVertex ejv : verticesInCreationOrder) {
						ejv.cancel();
					}
				} else {
					// set the state of the job to failed
					transitionState(JobStatus.FAILING, JobStatus.FAILED, t);
				}

				return;
			}

			// no need to treat other states
		}
	}
```

总结其逻辑：

- 在一些合法性 check 之后，TaskManager 会给自己发送一条路由消息：`UpdateTaskExecutionState`，TaskManager 继而将这条消息转发给 JobManager
- JobManager 会标志 Job 状态为 FAILING 并通知 JobCli，并且立即停止所有 task 的执行，这时候 CheckpointCoordinator 在执行 checkpoint 的时候感知到 task 失败状态会立即返回，停止 checkpoint



## 异常后的资源释放

主要包括以下资源：

- 网络资源：InputGate 和 ResultPartiton 的内存占用
- 其他内存：通过 MemoryManager 申请的资源
- 缓存资源：lib 包和其他缓存
- 线程池：Task 内部持有