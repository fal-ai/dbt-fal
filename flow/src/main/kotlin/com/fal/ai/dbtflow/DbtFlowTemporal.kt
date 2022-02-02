package com.fal.ai.dbtflow

import com.fal.ai.dbtflow.activities.DockerActivities
import com.fal.ai.dbtflow.activities.DockerActivitiesWithCloudBuild
import com.google.cloud.devtools.cloudbuild.v1.CloudBuildClient
import io.temporal.activity.ActivityOptions
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowOptions
import io.temporal.common.RetryOptions
import io.temporal.worker.WorkerFactory
import io.temporal.workflow.Workflow
import io.temporal.workflow.WorkflowInterface
import io.temporal.workflow.WorkflowMethod
import java.io.File
import java.time.Duration
import java.util.*

const val TASK_QUEUE_NAME = "DBT-FLOW"

@WorkflowInterface
interface DockerBuildAndDeployWorkflow {
  @WorkflowMethod fun run(): Unit
}

class DockerBuildAndDeployWorkflowImpl : DockerBuildAndDeployWorkflow {
  override fun run(): Unit {

    val activityStub =
        Workflow.newActivityStub(
            DockerActivities::class.java,
            ActivityOptions.newBuilder().setStartToCloseTimeout(Duration.ofHours(1)).build())
    // We are not preserving any state so this can be random for now
    val scriptId = UUID.randomUUID().toString()
    val imageId = activityStub.buildDockerImage(scriptId)
    val containerImage = activityStub.uploadDockerImage(imageId)
    activityStub.deployDockerImage(containerImage, scriptId)

    val pythonActivityOptions =
        ActivityOptions.newBuilder()
            .setStartToCloseTimeout(Duration.ofSeconds(1000))
            .setTaskQueue("DBT-FLOW-PYTHON")
            .setRetryOptions(
                RetryOptions.newBuilder()
                    .setInitialInterval(Duration.ofSeconds(30))
                    .setMaximumAttempts(5)
                    .build())
            .build()
    val pythonActivity = Workflow.newUntypedActivityStub(pythonActivityOptions)
    pythonActivity.execute("FalScriptActivities::run_script", String::class.java, "JavaWorkflow")
  }
}

fun initiateTemporalWorker(client: WorkflowClient, projectId: String) {
  WorkerFactory.newInstance(client).let {
    it.newWorker(TASK_QUEUE_NAME).let { worker ->
      worker.registerWorkflowImplementationTypes(DockerBuildAndDeployWorkflowImpl::class.java)
      val cloudBuildClient = CloudBuildClient.create()
      val dockerPath = File(System.getProperty("user.dir"))
      worker.registerActivitiesImplementations(
          DockerActivitiesWithCloudBuild(projectId, cloudBuildClient, dockerPath))
    }
    it.start()
  }
}

fun runDbtFlow(client: WorkflowClient, projectId: String) {
  initiateTemporalWorker(client, projectId)

  Thread.sleep(5000)

  val options =
      WorkflowOptions.newBuilder()
          .setTaskQueue(TASK_QUEUE_NAME)
          .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(3).build())
          .build()
  client.newWorkflowStub(DockerBuildAndDeployWorkflow::class.java, options).run()
}
