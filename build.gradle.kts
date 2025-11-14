plugins {
    val indraVer = "3.2.0"
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
    publishSnapshotsTo("paperSnapshots", "https://repo.papermc.io/repository/maven-snapshots/")
    publishReleasesTo("paperReleases", "https://repo.papermc.io/repository/maven-releases/")
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
    signWithKeyFromProperties("signingKey", "signingPassword")
}
