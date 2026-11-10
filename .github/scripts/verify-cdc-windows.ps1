################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

param([Parameter(Mandatory = $true)][string]$JavaVersion)

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false
$testPath = 'paimon-flink/paimon-flink-cdc/src/test/java/org/apache/paimon/flink/action/cdc/serialization/CdcDebeziumDeserializationSchemaTest.java'
$reportPath = 'paimon-flink/paimon-flink-cdc/target/surefire-reports/TEST-org.apache.paimon.flink.action.cdc.serialization.CdcDebeziumDeserializationSchemaTest.xml'
$revisions = [ordered]@{
    before = 'c24be9df2c90f4c6a03d08c9c4b5a56b6d803045'
    after = $env:SUBJECT_SHA
}
$arguments = @(
    '-B', '-ntp', "-Dmaven.repo.local=$env:MAVEN_REPO_LOCAL",
    '-pl', 'paimon-flink/paimon-flink-cdc', '-Pflink1',
    '-DwildcardSuites=none', '-Dtest=CdcDebeziumDeserializationSchemaTest', 'test'
)

try {
    foreach ($phase in $revisions.Keys) {
        & git restore "--source=$($revisions[$phase])" -- $testPath
        if ($LASTEXITCODE -ne 0) { throw "Cannot restore $phase test" }
        Remove-Item $reportPath -ErrorAction SilentlyContinue
        $log = "windows-validation-results/jdk$JavaVersion-$phase.log"
        & mvn.cmd @arguments 2>&1 | Tee-Object -FilePath $log
        $exitCode = $LASTEXITCODE
        if (!(Test-Path $reportPath)) { throw "No fresh test report for $phase" }
        [xml]$report = Get-Content -Raw $reportPath
        $props = $report.testsuite.properties.property
        $classpath = ($props | Where-Object { $_.name -eq 'surefire.test.class.path' }).value
        $os = ($props | Where-Object { $_.name -eq 'os.name' }).value
        $java = ($props | Where-Object { $_.name -eq 'java.version' }).value
        if ($os -notlike 'Windows*') { throw "Expected Windows, got $os" }
        if ($classpath.Length -le 32767) { throw "Classpath too short to exercise boundary: $($classpath.Length)" }
        if ([int]$report.testsuite.tests -ne 2 -or [int]$report.testsuite.skipped -ne 0) {
            throw 'Expected both charset cases to execute'
        }
        if ($phase -eq 'before') {
            $errors = @($report.testsuite.testcase | ForEach-Object { $_.error })
            $lengthErrors = @($errors | Where-Object { $_.message -like '*CreateProcess error=206*' })
            if ($exitCode -eq 0 -or $lengthErrors.Count -ne 2) {
                throw 'Old launcher did not fail with error=206 in both cases'
            }
        } else {
            if ($exitCode -ne 0 -or [int]$report.testsuite.errors -ne 0 -or [int]$report.testsuite.failures -ne 0) {
                throw 'Manifest launcher did not pass both cases'
            }
        }
        Copy-Item $reportPath "windows-validation-results/jdk$JavaVersion-$phase.xml"
        $summary = "$os | JDK $java | $phase | classpath=$($classpath.Length) chars | tests=$($report.testsuite.tests) | failures=$($report.testsuite.failures) | errors=$($report.testsuite.errors) | exit=$exitCode"
        Write-Output $summary
        Add-Content windows-validation-results/summary.txt $summary
        Add-Content $env:GITHUB_STEP_SUMMARY $summary
    }
} finally {
    & git restore "--source=$env:SUBJECT_SHA" -- $testPath
}
