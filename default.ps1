properties { 
  $base_dir  = resolve-path .
  $lib_dir = "$base_dir\SharedLibs"
  $build_dir = "$base_dir\build" 
  $buildartifacts_dir = "$build_dir\" 
  $sln_file = "$base_dir\Rhino.PersistentHashTable.sln" 
  $version = "1.7.0.0"
  $humanReadableversion = "1.7"
  $tools_dir = "$base_dir\Tools"
  $release_dir = "$base_dir\Release"
  $uploadCategory = "Rhino-PHT"
  $uploadScript = "C:\Builds\Upload\PublishBuild.build"
} 

include .\psake_ext.ps1
	
task default -depends Release

task Clean { 
  remove-item -force -recurse $buildartifacts_dir -ErrorAction SilentlyContinue 
  remove-item -force -recurse $release_dir -ErrorAction SilentlyContinue 
} 

task Init -depends Clean { 
	Generate-Assembly-Info `
		-file "$base_dir\Rhino.PersistentHashTable\Properties\AssemblyInfo.cs" `
		-title "Rhino PersistentHashTable $version" `
		-description "Persistent Hash Table" `
		-company "Hibernating Rhinos" `
		-product "Rhino PHT $version" `
		-version $version `
		-copyright "Hibernating Rhinos & Ayende Rahien 2004 - 2009"
		
	Generate-Assembly-Info `
		-file "$base_dir\Rhino.PersistentHashTable.Tests\Properties\AssemblyInfo.cs" `
		-title "Rhino PersistentHashTable $version" `
		-description "Persistent Hash Table" `
		-company "Hibernating Rhinos" `
		-product "Rhino PHT $version" `
		-version $version `
		-copyright "Hibernating Rhinos & Ayende Rahien 2004 - 2009"
        
    Generate-Assembly-Info `
		-file "$base_dir\Rhino.PersistentHashTable.Util\Properties\AssemblyInfo.cs" `
		-title "Rhino PersistentHashTable Util $version" `
		-description "Persistent Hash Table Utils" `
		-company "Hibernating Rhinos" `
		-product "Rhino PHT Util $version" `
		-version $version `
		-copyright "Hibernating Rhinos & Ayende Rahien 2004 - 2009"
		
	new-item $release_dir -itemType directory 
	new-item $buildartifacts_dir -itemType directory 
} 

task Compile -depends Init { 
  exec msbuild "/p:OutDir=""$buildartifacts_dir "" $sln_file"
} 

task Test -depends Compile {
  $old = pwd
  cd $build_dir
  exec "$tools_dir\xUnit\xunit.console.exe" "$build_dir\Rhino.PersistentHashTable.Tests.dll"
  cd $old		
}


task Release -depends Test {
	& $tools_dir\zip.exe -9 -A -j `
		$release_dir\Rhino.PersistentHashTable-$humanReadableversion-Build-$env:ccnetnumericlabel.zip `
		$build_dir\Rhino.PersistentHashTable.dll `
		$build_dir\Rhino.PersistentHashTable.xml `
        $build_dir\Esent.Interop.dll `
		$build_dir\Esent.Interop.xml `
		license.txt `
		acknowledgements.txt
	if ($lastExitCode -ne 0) {
        throw "Error: Failed to execute ZIP command"
    }
}

task Upload -depend Release {
	if (Test-Path $uploadScript ) {
		$log = git log -n 1 --oneline		
		msbuild $uploadScript /p:Category=$uploadCategory "/p:Comment=$log" "/p:File=$release_dir\Rhino.PersistentHashTable-$humanReadableversion-Build-$env:ccnetnumericlabel.zip"
		
		if ($lastExitCode -ne 0) {
			throw "Error: Failed to publish build"
		}
	}
	else {
		Write-Host "could not find upload script $uploadScript, skipping upload"
	}
}