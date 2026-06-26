@echo off
cd /d "%~dp0"
echo ========================================
echo  TG-FileStreamBot
echo  https://github.com/ariful47526/TG-FileStreamBot
echo ========================================
echo.
echo Server: http://localhost:8080
echo Send a file to your bot on Telegram to get a stream link.
echo.
call python main.py
pause
