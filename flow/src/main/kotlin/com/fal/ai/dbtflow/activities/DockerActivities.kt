package com.fal.ai.dbtflow.activities

import com.google.cloud.devtools.cloudbuild.v1.CloudBuildClient
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.BucketInfo
import com.google.cloud.storage.StorageClass
import com.google.cloud.storage.StorageOptions
import com.google.cloudbuild.v1.Build
import com.google.cloudbuild.v1.BuildStep
import com.google.cloudbuild.v1.CreateBuildRequest
import com.google.cloudbuild.v1.Source
import com.google.cloudbuild.v1.StorageSource
import io.temporal.activity.ActivityInterface
import java.io.BufferedOutputStream
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*
import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream

/** Lifecycle of the infrastructure to run a fal script */
@ActivityInterface
interface DockerActivities {

  fun buildDockerImage(scriptId: String): String

  fun uploadDockerImage(imageId: String): String

  fun deployDockerImage(containerImage: String, scriptId: String): String

  fun stopComputeInstance(vmName: String)
}

/** Handles the lifecycle of a fal script with google cloud build */
class DockerActivitiesWithCloudBuild(
    private val projectId: String,
    private val cloudBuildClient: CloudBuildClient,
    private val sourcePath: File
) : DockerActivities {

  override fun buildDockerImage(scriptId: String): String {
    // TODO: We keep no state for now so this ID can be random everytime
    val id = UUID.randomUUID()
    val imageId = "dbt-flow-$id"
    val imageRepository = "gcr.io/$projectId/$imageId"
    val bucketName = "${projectId}_${imageId}_bucket_test"
    val objectId = "zipfile_$imageId.tar.gz"
    uploadToStorage(bucketName, objectId, sourcePath, projectId)

    val buildDockerStep =
        BuildStep.newBuilder()
            .setName("gcr.io/cloud-builders/docker")
            .addAllArgs(listOf("build", "-t", imageRepository, "."))

    val build =
        Build.newBuilder()
            .setSource(getStorageSource(bucketName, objectId))
            .addSteps(buildDockerStep)
            .build()

    val request = CreateBuildRequest.newBuilder().setBuild(build).setProjectId(projectId).build()
    val result = cloudBuildClient.createBuildAsync(request)
    result.get()

    return imageRepository
  }

  override fun uploadDockerImage(imageId: String): String {
    val buildDockerStep =
        BuildStep.newBuilder()
            .setName("gcr.io/cloud-builders/docker")
            .addAllArgs(listOf("push", imageId))

    val build = Build.newBuilder().addSteps(buildDockerStep).build()
    val request = CreateBuildRequest.newBuilder().setBuild(build).build()
    val result = cloudBuildClient.createBuildAsync(request)
    result.get()

    return imageId
  }

  override fun deployDockerImage(containerImage: String, scriptId: String): String {
    val name = "dbt-flow-$scriptId"
    val buildDockerStep =
        BuildStep.newBuilder()
            .setName("gcr.io/cloud-builders/gcloud")
            .addAllArgs(
                listOf(
                    "compute",
                    "instances",
                    "create-with-container",
                    name,
                    "--container-image",
                    containerImage,
                    "--container-restart-policy",
                    "never"))

    val build = Build.newBuilder().addSteps(buildDockerStep).build()
    val request = CreateBuildRequest.newBuilder().setBuild(build).build()
    val result = cloudBuildClient.createBuildAsync(request)
    result.get()

    return name
  }

  override fun stopComputeInstance(vmName: String) {
    val buildDockerStep =
        BuildStep.newBuilder()
            .setName("gcr.io/cloud-builders/gcloud")
            .addAllArgs(listOf("compute", "instances", "stop", vmName))

    val build = Build.newBuilder().addSteps(buildDockerStep).build()
    val request = CreateBuildRequest.newBuilder().setBuild(build).build()
    val result = cloudBuildClient.createBuildAsync(request)
    result.get()
  }
}

private fun uploadToStorage(
    bucketName: String,
    objectName: String,
    sourcePath: File,
    projectId: String
) {
  val zipFile = zipDirectory(sourcePath)
  val storage = StorageOptions.newBuilder().setProjectId(projectId).build().service
  val blobId = BlobId.of(bucketName, objectName)
  val blobInfo = BlobInfo.newBuilder(blobId).build()
  // create bucket
  // TODO: probably you dont need to create this every time
  storage.create(
      BucketInfo.newBuilder(bucketName)
          .setStorageClass(StorageClass.STANDARD)
          .setLocation("US")
          .build())
  // create blob
  storage.create(blobInfo, Files.readAllBytes(Paths.get(zipFile.toURI())))
}

private fun getStorageSource(bucketName: String, objectName: String): Source =
    Source.newBuilder()
        .setStorageSource(
            StorageSource.newBuilder().setBucket(bucketName).setObject(objectName).build())
        .build()

private fun zipDirectory(source: File): File {
  val path = Files.createTempDirectory("java-")
  val outputTarGzipName = "${source.nameWithoutExtension}.tar.gz"
  val outputTarGzip = path.resolve(outputTarGzipName)
  Files.newOutputStream(outputTarGzip).use { fOut ->
    BufferedOutputStream(fOut).use { buffOut ->
      GzipCompressorOutputStream(buffOut).use { gzOut ->
        TarArchiveOutputStream(gzOut).use { tOut ->
          source.walk().forEach { file ->
            if (!file.isDirectory) {
              val tarEntry = TarArchiveEntry(file, file.relativeTo(source).toString())
              tOut.putArchiveEntry(tarEntry)
              Files.copy(file.toPath(), tOut)
              tOut.closeArchiveEntry()
            }
          }
          tOut.finish()
        }
      }
    }
  }
  return outputTarGzip.toFile()
}
