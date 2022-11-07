@ECHO OFF
set WORKPATH=%~dp0
set INPUT_FILE=input_index_composition.txt


ECHO Setting up virtual environment
if not exist "%WORKPATH%\venv" py -m venv %WORKPATH%\venv

ECHO Activating virtual environment
CALL %WORKPATH%\venv\Scripts\activate

ECHO Installing nordea-analytics python
pip install nordea-analytics >nul

ECHO Reading input from %INPUT_FILE%
for /f "tokens=1,2 delims==" %%a in (%INPUT_FILE%) DO (
	if %%a==client_id set CLIENT_ID=%%b
	if %%a==client_secret set CLIENT_SECRET=%%b
	if %%a==indices set INDICES=%%b
	if %%a==calc_date set CALC_DATE=%%b
)

ECHO Contacting API, creating output file
cli_tool.py get_index_composition %CLIENT_ID% %CLIENT_SECRET% %INDICES% %CALC_DATE% >nul

ECHO Deactivating virtual environment
CALL %WORKPATH%\venv\Scripts\deactivate



@ECHO ON