@Library('jenkins-shared-libs@feature/lib_refactor')_

pipeline {


    agent { label 'docker'}

    environment {
        ARTIFACTORY_SERVER_REF = 'artifactory'

        artifactVersion = "${new Date().format('yy.MM.dd')}"
        pomPath = 'pom.xml'

        snapshotRepository = 'libs-snapshot-local'
        releaseRepository = 'libs-release-local'
        snapshotDependenciesRepository = 'libs-snapshot'
        releaseDependeciesRepository = 'libs-release'


    }

	parameters {
        string(name: 'HAS_CHANGES', defaultValue: 'N')
        string(name: 'IS_MASTER', defaultValue: 'N')
    }

    stages {

        stage('Pipeline setup') {
            parallel {
                stage('Triggers setup') {
                    agent{
                        docker {
                            image 'maven:3-alpine'
                            args '-v $HOME/.m2:/root/.m2 --network user-default'
                            reuseNode true
                            label 'docker'
                        }
                    }
                    steps {
                        script {
                            triggerStarter  ((env.JOB_NAME.tokenize('/'))[0])
                            /*withCredentials([string(credentialsId: 'github-orwell-cicd-webhook-token', variable: 'githubWebhookGenericToken')]) {
                                properties([
                                        pipelineTriggers([
                                                [
                                                        $class                   : 'GenericTrigger',
                                                        causeString              : 'Push made',
                                                        token                    : githubWebhookGenericToken,
                                                        genericHeaderVariables   : [
                                                                [key: 'X-GitHub-Event', regexpFilter: '']
                                                        ],
                                                        genericVariables         : [
                                                                [key: 'project', value: '$.repository.name'],
                                                                [key: 'branch', value: '$.ref']
                                                        ],
                                                        regexpFilterExpression   : (env.JOB_NAME.tokenize('/'))[0] + ',push',
                                                        regexpFilterText         : '$project,$x_github_event',
                                                        printContributedVariables: true,
                                                        printPostContent         : true
                                                ]
                                        ])
                                ])
                            }*/
                        }
                    }
                }

                stage('Artifactory setup') {
                    agent{
                        docker {
                            image 'maven:3-alpine'
                            args '-v $HOME/.m2:/root/.m2 --network  user-default'
                            reuseNode true
                            label 'docker'
                        }
                    }
                    steps {
                        script {
                            def MAVEN_HOME = sh(script: 'echo $MAVEN_HOME', returnStdout: true).trim()
                            // Obtain an Artifactory server instance, defined in Jenkins --> Manage:
                            server = Artifactory.server ARTIFACTORY_SERVER_REF

                            def  descriptor = Artifactory.mavenDescriptor()
                          //  def end = '-SNAPSHOT'
                            descriptor.pomFile = pomPath
                            def scmVars = checkout scm
                            if (!( scmVars.GIT_BRANCH == 'master'))  {
                                artifactVersion = artifactVersion + '-SNAPSHOT'
                            }
                            if ( scmVars.GIT_BRANCH == 'master')  {
                                env.IS_MASTER='Y'
                            }
                            if (scmVars.GIT_COMMIT != scmVars.GIT_PREVIOUS_COMMIT) {
                                env.HAS_CHANGES='Y'
                            }                            
                            descriptor.version = artifactVersion
                            descriptor.transform()

                            rtMaven = Artifactory.newMavenBuild()
                            env.MAVEN_HOME = MAVEN_HOME
                            rtMaven.deployer releaseRepo: releaseRepository, snapshotRepo: snapshotRepository, server: server
                            rtMaven.resolver releaseRepo: releaseDependeciesRepository, snapshotRepo: snapshotDependenciesRepository, server: server
                            rtMaven.opts = '-DprofileIdEnabled=true'
                            rtMaven.deployer.deployArtifacts = false // Disable artifacts deployment during Maven run

                            buildInfo = Artifactory.newBuildInfo()
                        }
                    }
                }
            }
        }

        stage('Unit test') {
            when {
        		expression { env.HAS_CHANGES == 'Y' }
        		beforeAgent true
      		}
            agent{
                docker {
                    image 'maven:3-alpine'
                    args '-v $HOME/.m2:/root/.m2 --network user-default'
                    reuseNode true
                    label 'docker'
                }
            }
            steps {

                    script {
                        rtMaven.run pom: pomPath, goals: '-U clean test -Pdeploy'
                    }
                
            }
/*
            post {
                  always {
                      junit 'target/surefire-reports/*.xml'
                  }
            }
*/
        }

        stage('Build') {
            when {
        		expression { env.HAS_CHANGES == 'Y' }
        		beforeAgent true
      		}
            agent{
                docker {
                    image 'maven:3-alpine'
                    args '-v $HOME/.m2:/root/.m2 --network user-default'
                    reuseNode true
                    label 'docker'
                }
            }
            steps {
                script {
                    rtMaven.run pom: pomPath, goals: '-U clean package -DskipTests -Pdeploy', buildInfo: buildInfo
                }
            }

            post {
                always {
                    archiveArtifacts artifacts: '**/target/*.jar'
                }
            }
        }

        stage('Publish') {
            when {
        		expression { env.HAS_CHANGES == 'Y' }
        		beforeAgent true
      		}
            agent{
                docker {
                    image 'maven:3-alpine'
                    args '-v $HOME/.m2:/root/.m2 --network user-default'
                    reuseNode true
                    label 'docker'
                }
            }

            steps {
                script {
                    server.publishBuildInfo buildInfo
                    rtMaven.deployer.deployArtifacts buildInfo
                }

            }
        }


       stage('Deploy') {
            when {
        		expression { env.HAS_CHANGES == 'Y' && env.IS_MASTER == 'N' }
        		beforeAgent true
      		}
            steps {
                script {
                    pom = readMavenPom file: pomPath
                    pom = readMavenPom file: pomPath
                    kUser =  'svc_core'
                    hostsDeploy = 'sid-hdf-g4-1.node.sid.consul'
                    nimbusHost = 'sid-hdf-g1-1.node.sid.consul'
                    zkHost = 'sid-hdf-g1-0.node.sid.consul:2181,sid-hdf-g1-1.node.sid.consul:2181,sid-hdf-g1-2.node.sid.consul:2181'
                    mainClass = 'com.orwellg.yggdrasil.interpartyrelationship.topology.ReadInterpartyRelationshipTopology'
                    groupId = 'com.orwellg.yggdrasil'
                    stormDeploy  pom.artifactId  , pom.version , groupId, kUser  ,  hostsDeploy , nimbusHost ,zkHost, mainClass
                }
            }
        }
    }
}