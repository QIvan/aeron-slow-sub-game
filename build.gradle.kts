plugins {
    id("java")
}

group = "org.mysun"
version = ""
java {
    sourceCompatibility = org.gradle.api.JavaVersion.VERSION_11
    targetCompatibility = org.gradle.api.JavaVersion.VERSION_11
}

repositories {
    mavenLocal()
    mavenCentral()
}

dependencies {
    implementation(group = "com.fasterxml.jackson.core", name = "jackson-databind", version = "2.11.0")
    implementation("io.aeron:aeron-all:1.28.0")
    implementation("io.micrometer:micrometer-registry-prometheus:latest.release")
    implementation("io.micrometer:micrometer-registry-prometheus:latest.release")


    testImplementation("org.junit.jupiter:junit-jupiter-api:5.3.1")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.3.1")
}


tasks.test {
    useJUnitPlatform()
}
