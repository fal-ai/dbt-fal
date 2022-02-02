import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.6.10"
}

group = "me.gorkemyurtseven"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.google.cloud:google-cloud-build:3.3.7")
    implementation("io.temporal:temporal-sdk:1.7.1")
    implementation("com.google.cloud:google-cloud-storage:2.3.0")
    implementation("org.apache.commons:commons-compress:1.21")
    implementation("com.github.docker-java:docker-java:3.2.12")
    implementation("com.github.docker-java:docker-java-transport-netty")
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnit()
}

tasks.withType<KotlinCompile>() {
    kotlinOptions.jvmTarget = "1.8"
}
