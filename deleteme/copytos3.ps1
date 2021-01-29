$path=$args[0]
$recursive =$args[1]
if($recursive = "yes")
{
$files = Get-ChildItem -Path $path -Name -Recurse 
}
else
{
$files = Get-ChildItem -Path $path -Name
}

echo "number of items is: " $files.Count
$i = 0
foreach ($f in $files){

echo $f
$value1 = $f -match ".tif"
echo $value1

if($value1)
{
python tif2pdf.py
}
<#
$output = Write-S3Object -BucketName textract-uploadbucket-bas6dcsjr2d2 -Key dbj\$f -File $param1\$f
$i++
Write-Host "uploaded " $f  " " $i "of " $files.Count 


Start-Sleep -Milliseconds 400
#>
} 
