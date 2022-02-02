package com.fal.ai.dbtflow.activities

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.command.BuildImageResultCallback
import com.github.dockerjava.core.command.PushImageResultCallback
import java.io.File

/** Handles the lifecycle of a fal script with docker java client */
class DockerActivitiesWithDockerJavaClient(
    private val projectId: String,
    private val dockerClient: DockerClient,
    private val dockerFile: File
) : DockerActivities {

  override fun buildDockerImage(scriptId: String): String {
    return dockerClient
        .buildImageCmd()
        .withDockerfile(dockerFile)
        .withPull(true)
        .withNoCache(true)
        .withTags(setOf("dbt-from-temporal"))
        .withPlatform("linux/amd64")
        .exec(BuildImageResultCallback())
        .awaitImageId()
  }

  override fun uploadDockerImage(imageId: String): String {
    val imageRepository = "gcr.io/$projectId/$imageId"

    dockerClient.tagImageCmd(imageId, imageRepository, "latest").exec()
    dockerClient
        .pushImageCmd(imageRepository)
        .withTag("latest")
        .exec(PushImageResultCallback())
        .awaitCompletion()
    return imageRepository
  }

  override fun deployDockerImage(containerImage: String, scriptId: String): String {
    val name = "dbt-flow-$scriptId"
    val command =
        """
          gcloud compute instances 
            create-with-container $name 
            --container-image $containerImage 
            --container-restart-policy never
        """.trimIndent()
    Runtime.getRuntime().exec(command)
    return name
  }

  override fun stopComputeInstance(vmName: String) {
    val command = "gcloud compute instances stop $vmName"
    Runtime.getRuntime().exec(command)
  }
}
