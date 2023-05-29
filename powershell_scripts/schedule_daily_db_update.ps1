# code template

Write-Output "Please make sure python3.exe can be run from anywhere on you computer"

# How to setup a task:
# $action = New-ScheduledTaskAction -Execute 'PROGRAM'
# $trigger = New-ScheduledTaskTrigger -SETTING -At TIME.
# Register-ScheduledTask -Action $action -Trigger $trigger -TaskPath "TASK-FOLDER" -TaskName "TASK-NAME" -Description "OPTIONAL-DESCRIPTION-TEXT"


$project_dir = $PSCommandPath | Split-Path -Parent | Split-Path -Parent
$python_exe_abs_path = $project_dir | Join-Path -ChildPath "venv/Scripts/python.exe"
$update_db_script_abs_path = $project_dir | Join-Path -ChildPath "src/scripts/update_db.py"


# create variable to contain the task action
$action = New-ScheduledTaskAction -Execute "`"$python_exe_abs_path`"" -Argument "`"$update_db_script_abs_path`""

# create the trigger for the scheduled task
$trigger = New-ScheduledTaskTrigger -Daily -At 6pm


# In the following command line, replace "TASK-FOLDER", "TASK-NAME", and OPTIONAL-DESCRIPTION-TEXT with your task information.
Register-ScheduledTask -Action $action -Trigger $trigger -TaskPath "AlgorithmicProjectTasks" -TaskName "UpdateDB" -Description "run daily update db"

# Once all changes are complete - run this script

