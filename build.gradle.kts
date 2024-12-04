plugins {
    val indraVer = "3.1.3"
    id("net.kyori.indra") version indraVer
    id("net.kyori.indra.publishing") version indraVer
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(libs.org.slf4j.slf4j.api)
    implementation(libs.it.unimi.dsi.fastutil)
}

tasks.jar {
    manifest {
        attributes("FMLModType" to "GAMELIBRARY")
    }
}

indra {
    javaVersions {
        target(21)
    }
    publishSnapshotsTo("paperSnapshots", "https://papermc.io/repo/repository/maven-snapshots/")
    publishReleasesTo("paperReleases", "https://papermc.io/repo/repository/maven-releases/")
    gpl3OnlyLicense()
    github("Tuinity", "ConcurrentUtil")
    configurePublications {
        pom {
            developers {
                developer {
                    id = "spottedleaf"
                }
            }
        }
    }
}
