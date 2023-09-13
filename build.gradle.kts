plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    implementation("org.apache.spark:spark-sql_2.12:3.0.0")
    implementation("org.apache.spark:spark-mllib_2.12:3.0.0")

}

tasks.test {
    useJUnitPlatform()
}