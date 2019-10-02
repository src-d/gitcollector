pipeline {
  agent {
    kubernetes {
      label 'regression-retrieval'
      inheritFrom 'performance'
      defaultContainer 'regression-retrieval'
      nodeSelector 'srcd.host/type=jenkins-worker'
      containerTemplate {
        name 'regression-retrieval'
        image 'srcd/regression-retrieval:latest'
        ttyEnabled true
        command 'cat'
      }
    }
  }
  environment {
    GOPATH = "/go"
    GO_IMPORT_PATH = "github.com/src-d/regression-retrieval"
    GO_IMPORT_FULL_PATH = "${env.GOPATH}/src/${env.GO_IMPORT_PATH}"
    GO111MODULE = "on"
    PROM_ADDRESS = "http://prom-pushgateway-prometheus-pushgateway.monitoring.svc.cluster.local:9091"
    PROM_JOB = "retrieval_performance"
  }
  triggers { pollSCM('0 0,12 * * *') }
  stages {
    stage('Run performance tests') {
      when { branch 'master' }
      steps {
        sh '/bin/regression-retrieval --kind=gitcollector --csv --prom local:HEAD'
      }
    }
    stage('PR-run') {
      when { changeRequest target: 'master' }
      steps {
        sh '/bin/regression-retrieval --kind=gitcollector remote:master local:HEAD'
      }
    }
    stage('Plot') {
      when { branch 'master' }
      steps {
        script {
          plotFiles = findFiles(glob: "plot_*.csv")
          plotFiles.each {
            echo "plot ${it.getName()}"
            sh "cat ${it.getName()}"
            plot(
              group: 'performance',
              csvFileName: it.getName(),
              title: it.getName(),
              numBuilds: '100',
              style: 'line',
              csvSeries: [[
                displayTableFlag: false,
                exclusionValues: '',
                file: it.getName(),
                inclusionFlag: 'OFF',
              ]]
            )
          }
        }
      }
    }
  }
}
