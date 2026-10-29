# Windows PowerShell / .NET cryptography; create the output with a user-only ACL.
param([Parameter(Mandatory=$true)][string]$OutputPath)
$ErrorActionPreference = 'Stop'
$security = New-Object System.Security.AccessControl.FileSecurity
$security.SetAccessRuleProtection($true, $false)
$sid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
$rule = New-Object System.Security.AccessControl.FileSystemAccessRule($sid, 'FullControl', 'Allow')
$security.AddAccessRule($rule)
$file = New-Object System.IO.FileStream($OutputPath, [System.IO.FileMode]::CreateNew, [System.Security.AccessControl.FileSystemRights]::Write, [System.IO.FileShare]::None, 4096, [System.IO.FileOptions]::None, $security)
$rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
$sha = [System.Security.Cryptography.SHA256]::Create()
$complete = $false
function Base64Url([byte[]]$Bytes) {
    return [Convert]::ToBase64String($Bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_')
}
try {
    $bytes = New-Object byte[] 32
    $rng.GetBytes($bytes)
    $verifier = Base64Url $bytes
    $rng.GetBytes($bytes)
    $state = Base64Url $bytes
    $challenge = Base64Url ($sha.ComputeHash([Text.Encoding]::ASCII.GetBytes($verifier)))
    $json = @{ verifier=$verifier; state=$state; challenge=$challenge } | ConvertTo-Json -Compress
    $output = [Text.Encoding]::UTF8.GetBytes($json)
    $file.Write($output, 0, $output.Length)
    $complete = $true
} finally {
    $file.Dispose()
    $rng.Dispose()
    $sha.Dispose()
    if (-not $complete) { Remove-Item -LiteralPath $OutputPath }
}
