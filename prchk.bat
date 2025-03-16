@echo off
call "C:\prchk\venv\Scripts\activate"
python "C:\prchk\prchk.py" %*
deactivate