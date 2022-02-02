package com.fal.ai.dbtflow

import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowClientOptions
import io.temporal.serviceclient.WorkflowServiceStubsOptions
import io.temporal.serviceclient.WorkflowServiceStubs


class TemporalConfig {
  fun workflowClient(
    target: String?,
    namespace: String
  ): WorkflowClient {
    val service = WorkflowServiceStubs.newInstance(
      WorkflowServiceStubsOptions.newBuilder().setTarget(target).build()
    )
    val workflowClientOpts = WorkflowClientOptions.newBuilder()
    if (!namespace.isEmpty()) {
      // NOTE: We need to manually create namespaces before using them
      workflowClientOpts.setNamespace(namespace)
    }
    return WorkflowClient.newInstance(service, workflowClientOpts.build())
  }
}
