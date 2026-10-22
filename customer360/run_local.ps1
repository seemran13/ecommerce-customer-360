param(
    [double]$Scale = 0.02,
    [string]$DataRoot = ''
)
$ErrorActionPreference = 'Stop'

$RunStamp = Get-Date -Format 'yyyyMMdd_HHmmss'
if ([string]::IsNullOrWhiteSpace($DataRoot)) {
    if ($Scale -eq 1.0) { $DataRoot = "data_full_run_$RunStamp" }
    else { $DataRoot = "data_small_run_$RunStamp" }
}
$Artifacts = Join-Path $DataRoot 'ml_artifacts'
$RagOut = Join-Path $DataRoot 'rag_index'

function Invoke-PythonStep {
    param([string[]]$PythonArgs)
    Write-Host ("`n> python " + ($PythonArgs -join ' ')) -ForegroundColor Cyan
    & python @PythonArgs
    if ($LASTEXITCODE -ne 0) { throw "Python step failed with exit code $LASTEXITCODE" }
}

New-Item -ItemType Directory -Force -Path $DataRoot, $Artifacts, $RagOut | Out-Null
Invoke-PythonStep @('data_gen/generate_data.py', '--out', $DataRoot, '--scale', $Scale.ToString([Globalization.CultureInfo]::InvariantCulture))
Invoke-PythonStep @('data_gen/inject_duplicates.py', $DataRoot, '0.005')
Invoke-PythonStep @('local_pipeline/cleanse_local.py', "$DataRoot/weblogs", "$DataRoot/clean", "$DataRoot/mysql/products.csv")
Invoke-PythonStep @('local_pipeline/abandonment_sim.py', "$DataRoot/clean/events_clean.pkl", $DataRoot)
Invoke-PythonStep @('ml/train_propensity.py', $DataRoot, $Artifacts)
Invoke-PythonStep @('ml/segment_kmeans.py', $DataRoot)
Invoke-PythonStep @('ml/score_customers.py', $DataRoot, $Artifacts)
Invoke-PythonStep @('rag/build_index.py', '--data', $DataRoot, '--out', $RagOut)
$env:RAG_INDEX = (Join-Path $RagOut 'index.pkl')
Invoke-PythonStep @('rag/eval_rag.py')
Remove-Item Env:RAG_INDEX -ErrorAction SilentlyContinue
Invoke-PythonStep @('bi/export_powerbi.py', $DataRoot)
Invoke-PythonStep @('drills/reconcile.py', '--data', $DataRoot)
$env:C360_DATA = $DataRoot
Invoke-PythonStep @('-m', 'pytest', '-q', 'tests')
Remove-Item Env:C360_DATA -ErrorAction SilentlyContinue
Write-Host "`nCUSTOMER360_LOCAL_RUN=PASS data=$DataRoot scale=$Scale artifacts=$Artifacts rag=$RagOut" -ForegroundColor Green
