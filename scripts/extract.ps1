<#
.SYNOPSIS
  Extract Data from OSI PI Server
.DESCRIPTION
  This Script will extract data from an OSI PI Server.  
  The script is built and tested on a Virtual Cloud Environment provided by OSI with an Example Kit.
  Pump Condition Based Maintenance

.EXAMPLE
  .\extract.ps1
  Version History
  v1.0   - Initial Release
#>
    
$destinationFolder = "C:\Users\student01.PISCHOOL\desktop\"
$fileName = "sample"  
$suffix = ".csv"
$filePath = "$($destinationFolder)$($fileName)$($suffix)"
 

# Start Time and End Time to get Archive Data                          
$Starttime = (get-date).AddDays(-1)     
$Endtime=(get-date)      
$Timestamp=get-date -uFormat "%m%d%Y%H%M"  


# Add Header to File
#Add-Content -Path $filePath  -Value 'timestamp,tag,value,questionable,substituted,annotated' 


# Get Tags that match a pattern
$Points = Get-PIPoint -Connection $piConn -AllAttributes -WhereClause "tag:='OSIDemo*'"
 

# Connect to PI Server
$piServerName = "PISRV01"      
$piConn = Connect-PIDataArchive -PIDataArchiveMachineName $piServerName -AuthenticationMethod Windows   

  
# Enumerate all tags and get data.    
ForEach($pt in $Points){    
               
    $results = Get-PIValue -PIPoint $pt -StartTime $Starttime -EndTime $Endtime | Select-Object TimeStamp, Value, IsQuestionable, IsSubstituted, IsAnnotated  
    
    if ($null -ne $results) {
        if ($pt.Attributes.pointtype -eq "Digital") {    
            $digStateSet=  Get-PIDigitalStateSet -Name $pt.Attributes.digitalset -Connection $piConnection    
            
            ForEach ($result in $results) {     
          
                $rtimeStamp = $result.TimeStamp.ToString('yyy-MM-ddThh:mm:ss.fffZ')
                $tag = $($pt.Point.Name)
                $rvalue = $digStateSet[$result.Value.State]
                $rquestionable = $result.IsQuestionable
                $rsubstituted = $result.IsSubstituted
                $rannotated = $result.IsAnnotated

                Write-Host "$rTimeStamp, $tag, $rvalue, $rquestionable, $rsubstituted, $rannotated"
                Add-Content -Path  $filePath -Value "$rTimeStamp,$tag,$rvalue,$rquestionable,$rsubstituted,$rannotated"   
            }    
      
        }
        else {    
            ForEach ($result in $results) { 

                $rtimeStamp = $result.TimeStamp.ToString('yyy-MM-ddThh:mm:ss.fffZ')
                $tag = $($pt.Point.Name)    
                $rvalue = $result.Value
                $rquestionable = $result.IsQuestionable
                $rsubstituted = $result.IsSubstituted
                $rannotated = $result.IsAnnotated

                Write-Host "$rTimeStamp, $tag, $rvalue, $rquestionable, $rsubstituted, $rannotated"
                Add-Content -Path  $filePath -Value "$rTimeStamp,$tag,$rvalue,$rquestionable,$rsubstituted,$rannotated" 
            }    
        } 
    } 
}    