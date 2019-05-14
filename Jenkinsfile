pipeline {
    agent { docker 'maven:3.6.0-jdk-11-slim' }
    environment {
        NEXUS_REPO = 'https://nexus.infra.ssbmod.net'
        GITHUB_REPO = "https://github.com/statisticsnorway/${env.JOB_BASE_NAME}"
    }
    stages {
        stage("Build") {
            steps {
                git url: "${env.GITHUB_REPO}"
                sh 'mvn clean install -B -V -DskipTests'
            }
        }
        stage("Test") {
            steps {
                sh 'mvn test -B -V'
            }

        }
        stage('Deploy') {
            steps {
                withCredentials([usernamePassword(credentialsId: 'jenkins-nexus-credentials', usernameVariable: 'USER', passwordVariable: 'PASS')]) {
                    sh '''
                        set +x
                        echo "<settings><servers><server><id>ssb-nexus</id><username>$USER</username><password>$PASS</password></server></servers></settings>" >> ?/.m2/settings.xml
                    '''
                }
                sh """
                    mvn clean package deploy:deploy \
                    -DskipNexusStagingDeployMojo=true \
                    -DaltDeploymentRepository=ssb-nexus::default::${env.NEXUS_REPO}/repository/maven-snapshots/ \
                    -DaltSnapshotRepository=ssb-nexus::default::${env.NEXUS_REPO}/repository/maven-snapshots/ \
                    -DaltReleaseDeploymentRepository=ssb-nexus::default::${env.NEXUS_REPO}/repository/maven-releases/
                """
            }
        }
    }
}

