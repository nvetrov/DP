# Convert data from Hyperion.


1.Install python: https://www.python.org/downloads/  
2. install vscode: https://code.visualstudio.com/download
3. install git:  https://git-scm.com/download/win
4. git clone https://github.com/nvetrov/DP.gitto D:\HyperPy\
####Creating a virtual environment
https://mothergeo-py.readthedocs.io/en/latest/development/how-to/venv-win.html

<code>py -m pip install --user virtualenv </code>

<code>py -m venv env </code>


<code>.\env\Scripts\activate </code>

<code>py -m pip install --upgrade pip </code>


Проверить себя: where python
Answer: (env) D:\HyperPy\DP>where python
              D:\HyperPy\DP\env\Scripts\python.exe
              D:\Python39\python.exe

##terminal: pip install -r .\requirements.txt

####выдать права на папки: 
Дата: D:\Export\ 
Запуск: D:\HyperCmd\
Лог: D:\HyperLog\


### Загрузить изменения с сервера
<code> git pull  </code> 
### Установить dask
python -m pip install "dask[complete]"
