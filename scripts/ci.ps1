# 本机 CI 门禁：与 .github/workflows/ci.yml 保持一致。
# 用法：pwsh scripts/ci.ps1
$ErrorActionPreference = "Stop"

Write-Host "==> cargo fmt --check"
cargo fmt --check
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "==> cargo clippy --all-targets --locked -- -D warnings"
cargo clippy --all-targets --locked -- -D warnings
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "==> cargo test --workspace --locked"
cargo test --workspace --locked
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "CI 全部通过"
