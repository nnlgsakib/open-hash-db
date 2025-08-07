param (
    [string]$Path = ".",
    [string[]]$Exclude = @("node_modules", "openhashdb-ui", ".git","data"),
    [int]$Indent = 0
)

function Show-Tree {
    param (
        [string]$CurrentPath,
        [int]$Indent,
        [string[]]$Exclude
    )

    $items = Get-ChildItem -LiteralPath $CurrentPath -Force | Sort-Object -Property PSIsContainer, Name

    for ($i = 0; $i -lt $items.Count; $i++) {
        $item = $items[$i]
        $isLastItem = ($i -eq $items.Count - 1)

        $prefix = ""
        for ($j = 0; $j -lt $Indent; $j++) {
            $prefix += "|   "
        }

        if ($isLastItem) {
            $prefix += "`-- "
        } else {
            $prefix += "|-- "
        }

        # Skip excluded folders
        $shouldSkip = $false
        foreach ($ex in $Exclude) {
            if ($item.FullName -match [regex]::Escape($ex)) {
                $shouldSkip = $true
                break
            }
        }
        if ($shouldSkip) {
            continue
        }

        Write-Output "$prefix$($item.Name)"

        if ($item.PSIsContainer) {
            Show-Tree -CurrentPath $item.FullName -Indent ($Indent + 1) -Exclude $Exclude
        }
    }
}

Show-Tree -CurrentPath $Path -Indent $Indent -Exclude $Exclude
