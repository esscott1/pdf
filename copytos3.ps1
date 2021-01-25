$param1=$args[0]
$files = Get-ChildItem $param1 -Name -Filter *AFFT*.pdf
echo "number of items is: " $files.Count
$i = 0
foreach ($f in $files){

$output = Write-S3Object -BucketName textract-uploadbucket-bas6dcsjr2d2 -Key dbj\$f -File $param1\$f
$i++
Write-Host "uploaded " $f  " " $i "of " $files.Count 


Start-Sleep -Milliseconds 400

} 
