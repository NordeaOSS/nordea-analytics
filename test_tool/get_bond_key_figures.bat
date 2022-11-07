@ECHO OFF
set WORKPATH=%~dp0
set INPUT_FILE=input_bond_key_figures.txt


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
	if %%a==symbols set SYMBOLS=%%b
	if %%a==key_figures set KEY_FIGURES=%%b
	if %%a==calc_date set CALC_DATE=%%b
)

ECHO Contacting API, creating output file
cli_tool.py get_bond_key_figures %CLIENT_ID% %CLIENT_SECRET% %SYMBOLS% %KEY_FIGURES% %CALC_DATE% >nul

ECHO Deactivating virtual environment
CALL %WORKPATH%\venv\Scripts\deactivate


@ECHO ON