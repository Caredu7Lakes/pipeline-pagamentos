# ===========================================================================
# create-connector.ps1 · Injeta segredos do .env no template e cria o conector
# Nada sensível é versionado: o template usa tokens __...__ e o arquivo final
# (com segredos) é gerado em memória/temp e apagado ao fim.
#
# Uso:  .\connectors\create-connector.ps1
# Pré:  confluent login, ambiente e cluster já selecionados (confluent ... use)
# ===========================================================================
$ErrorActionPreference = "Stop"

# 1) Carrega o .env em um dicionário
$envVars = @{}
Get-Content .env | Where-Object { $_ -match '^\s*[^#].*=' } | ForEach-Object {
    $k, $v = $_ -split '=', 2
    $envVars[$k.Trim()] = $v.Trim()
}

# 2) Extrai o host DIRETO do Neon (sem -pooler) a partir da DATABASE_URL
$dbUrl = $envVars['DATABASE_URL']
$pgHost  = ($dbUrl -replace '^postgresql://[^@]+@','' -replace '/.*$','') -replace '-pooler',''

# 3) Lê o template e substitui os tokens
$tpl = Get-Content .\connectors\postgres-cdc.template.json -Raw
$final = $tpl `
    -replace '__WRITER_API_KEY__',        $envVars['WRITER_API_KEY'] `
    -replace '__WRITER_API_SECRET__',     $envVars['WRITER_API_SECRET'] `
    -replace '__POSTGRES_HOST__',         $pgHost `
    -replace '__POSTGRES_CDC_PASSWORD__', $envVars['POSTGRES_CDC_PASSWORD']

# 4) Grava um arquivo temporário (fora do git), cria o conector e apaga
$tmp = Join-Path $env:TEMP "postgres-cdc-final.json"
try {
    # grava SEM BOM (a CLI rejeita o BOM do utf8 do PowerShell 5)
    [System.IO.File]::WriteAllText($tmp, $final, (New-Object System.Text.UTF8Encoding($false)))
    confluent connect cluster create --config-file $tmp
}
finally {
    Remove-Item $tmp -ErrorAction SilentlyContinue   # garante que o segredo some
}