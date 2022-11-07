@ECHO OFF
set WORKPATH=%~dp0
set INPUT_FILE=input_time_series.txt


ECHO Setting up virtual environment
if not exist "%WORKPATH%\venv" py -m venv %WORKPATH%\venv
ECHO Virtual environment set up

ECHO Activating virtual environment
CALL %WORKPATH%\venv\Scripts\activate
ECHO Virtual environment activated

ECHO Installing nordea-analytics python
pip install nordea-analytics >nul

ECHO Reading input from %INPUT_FILE%
for /f "tokens=1,2 delims==" %%a in (%INPUT_FILE%) DO (
	if %%a==client_id set CLIENT_ID=%%b
	if %%a==client_secret set CLIENT_SECRET=%%b
	if %%a==symbols set SYMBOLS=%%b
	if %%a==key_figures set KEY_FIGURES=%%b
	if %%a==from_date set FROM_DATE=%%b
	if %%a==to_date set TO_DATE=%%b
)

ECHO Contacting API, creating output file
cli_tool.py get_time_series %CLIENT_ID% %CLIENT_SECRET% %SYMBOLS% %KEY_FIGURES% %FROM_DATE% %TO_DATE% >nul

ECHO Deactivating virtual environment
CALL %WORKPATH%\venv\Scripts\deactivate


@ECHO ON