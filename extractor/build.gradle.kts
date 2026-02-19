plugins {
    java
    application
}

group = "com.devpulse"
version = "0.1.0"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

application {
    mainClass.set("com.devpulse.extractor.orchestrator.ExtractorApp")
}

repositories {
    mavenCentral()
}

dependencies {
    // HTTP Client
    implementation("com.squareup.okhttp3:okhttp:4.12.0")

    // JSON Processing
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.0")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.17.0")

    // BigQuery SDK
    implementation("com.google.cloud:google-cloud-bigquery:2.38.2")

    // Configuration
    implementation("io.github.cdimascio:dotenv-java:3.0.0")

    // Logging
    implementation("org.slf4j:slf4j-api:2.0.12")
    runtimeOnly("ch.qos.logback:logback-classic:1.5.3")

    // Testing
    testImplementation(platform("org.junit:junit-bom:5.10.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.mockito:mockito-core:5.11.0")
    testImplementation("org.mockito:mockito-junit-jupiter:5.11.0")
    testImplementation("com.squareup.okhttp3:mockwebserver:4.12.0")
    testRuntimeOnly("ch.qos.logback:logback-classic:1.5.3")
}

tasks.test {
    useJUnitPlatform()
}

// Shadow JAR — bundles all runtime dependencies into a single executable JAR.
// Usage: gradle shadowJar && java -jar build/libs/extractor.jar --incremental
tasks.register<Jar>("shadowJar") {
    archiveBaseName.set("extractor")
    archiveClassifier.set("")
    archiveVersion.set("")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE

    manifest {
        attributes("Main-Class" to "com.devpulse.extractor.orchestrator.ExtractorApp")
    }

    from(sourceSets.main.get().output)

    dependsOn(configurations.runtimeClasspath)
    from({
        configurations.runtimeClasspath.get()
            .filter { it.name.endsWith(".jar") }
            .map { zipTree(it) }
    })

    // Merge META-INF/services files for ServiceLoader-based frameworks
    filesMatching("META-INF/services/*") {
        // Use append-based strategy via duplicatesStrategy above
    }

    exclude("META-INF/*.SF", "META-INF/*.DSA", "META-INF/*.RSA")
}
