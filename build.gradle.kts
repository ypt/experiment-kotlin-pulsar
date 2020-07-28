import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    application
    id("org.jetbrains.kotlin.jvm") version "1.3.50"
    id("com.commercehub.gradle.plugin.avro") version "0.21.0"
}

repositories {
    jcenter()
}

dependencies {
    implementation(platform("org.jetbrains.kotlin:kotlin-bom"))
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    testImplementation("org.jetbrains.kotlin:kotlin-test")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit")
    implementation("org.apache.pulsar:pulsar-client:2.6.0")
    implementation("org.apache.pulsar:pulsar-client-admin:2.6.0")
    implementation("org.slf4j:slf4j-simple:1.7.30")
    implementation("org.apache.avro:avro:1.10.0")

    // Jackson + Kotlin dependencies
    implementation("org.jetbrains.kotlin:kotlin-reflect:1.3.50")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.9.9.3")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.9.+")
}

application {
    mainClassName = "experiment.kotlin.pulsar.AppKt"
}

tasks.withType<KotlinCompile> {
    kotlinOptions {
        freeCompilerArgs = listOf("-Xjsr305=strict")
        jvmTarget = "1.8"
    }
}
